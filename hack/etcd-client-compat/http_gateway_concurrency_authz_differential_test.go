package compat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type httpConcurrencyAuthzOutcome struct {
	AllowedLockStatus      int
	AllowedUnlockStatus    int
	DeniedLockStatus       int
	DeniedLockCode         int
	DeniedLockMessage      string
	AllowedCampaignStatus  int
	AllowedLeaderStatus    int
	AllowedProclaimStatus  int
	AllowedResignStatus    int
	DeniedCampaignStatus   int
	DeniedCampaignCode     int
	DeniedLeaderStatus     int
	DeniedLeaderCode       int
	RevokedLockStatus      int
	RevokedLockCode        int
	RegrantedLockStatus    int
	RegrantedUnlockStatus  int
	InvalidatedLockStatus  int
	InvalidatedLockCode    int
	InvalidatedLockMessage string
}

func TestHTTPGatewayConcurrencyAuthorizationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcome := runHTTPConcurrencyAuthorizationScenario(t, reference)
	kubebrainOutcome := runHTTPConcurrencyAuthorizationScenario(t, kubebrain)
	require.Equal(t, referenceOutcome, kubebrainOutcome)
	for _, statusCode := range []int{
		referenceOutcome.AllowedLockStatus, referenceOutcome.AllowedUnlockStatus,
		referenceOutcome.AllowedCampaignStatus, referenceOutcome.AllowedLeaderStatus,
		referenceOutcome.AllowedProclaimStatus, referenceOutcome.AllowedResignStatus,
		referenceOutcome.RegrantedLockStatus, referenceOutcome.RegrantedUnlockStatus,
	} {
		require.Equal(t, http.StatusOK, statusCode)
	}
	for _, statusCode := range []int{
		referenceOutcome.DeniedLockStatus, referenceOutcome.DeniedCampaignStatus,
		referenceOutcome.DeniedLeaderStatus, referenceOutcome.RevokedLockStatus,
		referenceOutcome.InvalidatedLockStatus,
	} {
		require.Equal(t, http.StatusInternalServerError, statusCode)
	}
	require.Equal(t, 2, referenceOutcome.DeniedLockCode)
	require.Equal(t, 2, referenceOutcome.DeniedCampaignCode)
	require.Equal(t, 2, referenceOutcome.DeniedLeaderCode)
	require.Equal(t, 2, referenceOutcome.RevokedLockCode)
	require.Equal(t, 2, referenceOutcome.InvalidatedLockCode)
	require.Equal(t, "etcdserver: permission denied", referenceOutcome.DeniedLockMessage)
	require.Equal(t, "etcdserver: invalid auth token", referenceOutcome.InvalidatedLockMessage)
}

func runHTTPConcurrencyAuthorizationScenario(t *testing.T, endpoint string) httpConcurrencyAuthzOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	const (
		rootPassword  = "a365-root-password"
		alicePassword = "a365-alice-password"
		aliceNewPass  = "a365-alice-password-new"
		aliceRole     = "a365-concurrency-role"
		allowedPrefix = "/a365/concurrency/allowed/"
		deniedPrefix  = "/a365/concurrency/denied/"
	)
	users, err := cli.UserList(ctx)
	require.NoError(t, err)
	for _, user := range users.Users {
		if user == "alice" || user == "root" {
			_, err = cli.UserDelete(ctx, user)
			require.NoError(t, err)
		}
	}
	roles, err := cli.RoleList(ctx)
	require.NoError(t, err)
	for _, role := range roles.Roles {
		if role == aliceRole || role == "root" {
			_, err = cli.RoleDelete(ctx, role)
			require.NoError(t, err)
		}
	}

	_, err = cli.UserAdd(ctx, "root", rootPassword)
	require.NoError(t, err)
	_, err = cli.RoleAdd(ctx, "root")
	require.NoError(t, err)
	_, err = cli.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = cli.UserAdd(ctx, "alice", alicePassword)
	require.NoError(t, err)
	_, err = cli.RoleAdd(ctx, aliceRole)
	require.NoError(t, err)
	_, err = cli.RoleGrantPermission(ctx, aliceRole, allowedPrefix,
		clientv3.GetPrefixRangeEnd(allowedPrefix), clientv3.PermissionType(clientv3.PermReadWrite))
	require.NoError(t, err)
	_, err = cli.UserGrantRole(ctx, "alice", aliceRole)
	require.NoError(t, err)
	lockLease, err := cli.Grant(ctx, 120)
	require.NoError(t, err)
	electionLease, err := cli.Grant(ctx, 120)
	require.NoError(t, err)
	lockLeaseID, electionLeaseID := lockLease.ID, electionLease.ID
	_, err = cli.AuthEnable(ctx)
	require.NoError(t, err)

	rootClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
		Username: "root", Password: rootPassword,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rootClient.Close()) })
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = rootClient.AuthDisable(cleanupCtx)
		_, _ = cli.Revoke(cleanupCtx, lockLeaseID)
		_, _ = cli.Revoke(cleanupCtx, electionLeaseID)
		_, _ = cli.UserDelete(cleanupCtx, "alice")
		_, _ = cli.UserDelete(cleanupCtx, "root")
		_, _ = cli.RoleDelete(cleanupCtx, aliceRole)
		_, _ = cli.RoleDelete(cleanupCtx, "root")
	})

	authenticated, err := cli.Authenticate(ctx, "alice", alicePassword)
	require.NoError(t, err)
	require.NotEmpty(t, authenticated.Token)
	post := newHTTPConcurrencyAuthzPoster(t, endpoint)
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	errorCode := func(response map[string]any) int {
		t.Helper()
		code, ok := response["code"].(float64)
		require.True(t, ok, "missing numeric code in %#v", response)
		return int(code)
	}
	errorMessage := func(response map[string]any) string {
		t.Helper()
		message, ok := response["message"].(string)
		require.True(t, ok, "missing string message in %#v", response)
		return message
	}

	outcome := httpConcurrencyAuthzOutcome{}
	allowedLockName := encode(allowedPrefix + "lock")
	deniedLockName := encode(deniedPrefix + "lock")
	var response map[string]any
	outcome.AllowedLockStatus, response = post("/v3/lock/lock", map[string]any{
		"name": allowedLockName, "lease": fmt.Sprint(int64(lockLeaseID)),
	}, authenticated.Token)
	lockKey, ok := response["key"].(string)
	require.True(t, ok, "missing lock key in %#v", response)
	outcome.AllowedUnlockStatus, _ = post("/v3/lock/unlock", map[string]any{"key": lockKey}, authenticated.Token)
	outcome.DeniedLockStatus, response = post("/v3/lock/lock", map[string]any{
		"name": deniedLockName, "lease": fmt.Sprint(int64(lockLeaseID)),
	}, authenticated.Token)
	outcome.DeniedLockCode = errorCode(response)
	outcome.DeniedLockMessage = errorMessage(response)

	allowedElectionName := encode(allowedPrefix + "election")
	deniedElectionName := encode(deniedPrefix + "election")
	outcome.AllowedCampaignStatus, response = post("/v3/election/campaign", map[string]any{
		"name": allowedElectionName, "lease": fmt.Sprint(int64(electionLeaseID)), "value": encode("first"),
	}, authenticated.Token)
	leader := requireMapField(t, response, "leader")
	outcome.AllowedLeaderStatus, _ = post("/v3/election/leader",
		map[string]any{"name": allowedElectionName}, authenticated.Token)
	outcome.AllowedProclaimStatus, _ = post("/v3/election/proclaim",
		map[string]any{"leader": leader, "value": encode("second")}, authenticated.Token)
	outcome.AllowedResignStatus, _ = post("/v3/election/resign",
		map[string]any{"leader": leader}, authenticated.Token)
	outcome.DeniedCampaignStatus, response = post("/v3/election/campaign", map[string]any{
		"name": deniedElectionName, "lease": fmt.Sprint(int64(electionLeaseID)), "value": encode("denied"),
	}, authenticated.Token)
	outcome.DeniedCampaignCode = errorCode(response)
	outcome.DeniedLeaderStatus, response = post("/v3/election/leader",
		map[string]any{"name": deniedElectionName}, authenticated.Token)
	outcome.DeniedLeaderCode = errorCode(response)

	_, err = rootClient.RoleRevokePermission(ctx, aliceRole, allowedPrefix,
		clientv3.GetPrefixRangeEnd(allowedPrefix))
	require.NoError(t, err)
	outcome.RevokedLockStatus, response = post("/v3/lock/lock", map[string]any{
		"name": allowedLockName, "lease": fmt.Sprint(int64(lockLeaseID)),
	}, authenticated.Token)
	outcome.RevokedLockCode = errorCode(response)
	_, err = rootClient.RoleGrantPermission(ctx, aliceRole, allowedPrefix,
		clientv3.GetPrefixRangeEnd(allowedPrefix), clientv3.PermissionType(clientv3.PermReadWrite))
	require.NoError(t, err)
	outcome.RegrantedLockStatus, response = post("/v3/lock/lock", map[string]any{
		"name": allowedLockName, "lease": fmt.Sprint(int64(lockLeaseID)),
	}, authenticated.Token)
	lockKey, ok = response["key"].(string)
	require.True(t, ok, "missing regranted lock key in %#v", response)
	outcome.RegrantedUnlockStatus, _ = post("/v3/lock/unlock", map[string]any{"key": lockKey}, authenticated.Token)

	_, err = rootClient.UserChangePassword(ctx, "alice", aliceNewPass)
	require.NoError(t, err)
	outcome.InvalidatedLockStatus, response = post("/v3/lock/lock", map[string]any{
		"name": allowedLockName, "lease": fmt.Sprint(int64(lockLeaseID)),
	}, authenticated.Token)
	outcome.InvalidatedLockCode = errorCode(response)
	outcome.InvalidatedLockMessage = errorMessage(response)
	return outcome
}

func newHTTPConcurrencyAuthzPoster(
	t *testing.T, endpoint string,
) func(string, any, string) (int, map[string]any) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	return func(path string, body any, token string) (int, map[string]any) {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(raw))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		response, err := client.Do(request)
		require.NoError(t, err, path)
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(responseBody, &decoded), "%s: %q", path, responseBody)
		return response.StatusCode, decoded
	}
}
