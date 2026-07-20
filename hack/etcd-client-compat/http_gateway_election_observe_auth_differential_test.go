package compat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type httpElectionObserveAuthOutcome struct {
	InitialValue                string
	ExistingAfterRevokeValue    string
	OldTokenNewObserveStatus    int
	OldTokenNewObserveEmpty     bool
	DeniedNewObserveStatus      int
	DeniedNewObserveEmpty       bool
	RegrantedInitialValue       string
	ExistingAfterPasswordValue  string
	RegrantedAfterPasswordValue string
	InvalidNewObserveStatus     int
	InvalidNewObserveEmpty      bool
	ObserveContentType          string
	ObserveChunked              bool
}

func TestHTTPGatewayElectionObserveAuthorizationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcome := runHTTPElectionObserveAuthorizationScenario(t, reference)
	kubebrainOutcome := runHTTPElectionObserveAuthorizationScenario(t, kubebrain)
	require.Equal(t, referenceOutcome, kubebrainOutcome)
	require.Equal(t, "initial", referenceOutcome.InitialValue)
	require.Equal(t, "after-revoke", referenceOutcome.ExistingAfterRevokeValue)
	require.Equal(t, "after-revoke", referenceOutcome.RegrantedInitialValue)
	require.Equal(t, "after-password", referenceOutcome.ExistingAfterPasswordValue)
	require.Equal(t, "after-password", referenceOutcome.RegrantedAfterPasswordValue)
	for _, statusCode := range []int{
		referenceOutcome.OldTokenNewObserveStatus,
		referenceOutcome.DeniedNewObserveStatus,
		referenceOutcome.InvalidNewObserveStatus,
	} {
		require.Equal(t, http.StatusOK, statusCode)
	}
	require.True(t, referenceOutcome.OldTokenNewObserveEmpty)
	require.True(t, referenceOutcome.DeniedNewObserveEmpty)
	require.True(t, referenceOutcome.InvalidNewObserveEmpty)
}

func runHTTPElectionObserveAuthorizationScenario(t *testing.T, endpoint string) httpElectionObserveAuthOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	const (
		rootPassword  = "a366-root-password"
		alicePassword = "a366-alice-password"
		aliceNewPass  = "a366-alice-password-new"
		aliceRole     = "a366-observe-role"
		prefix        = "/a366/election/"
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
	_, err = cli.RoleGrantPermission(ctx, aliceRole, prefix, clientv3.GetPrefixRangeEnd(prefix),
		clientv3.PermissionType(clientv3.PermReadWrite))
	require.NoError(t, err)
	_, err = cli.UserGrantRole(ctx, "alice", aliceRole)
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 120)
	require.NoError(t, err)
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
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
		_, _ = cli.UserDelete(cleanupCtx, "alice")
		_, _ = cli.UserDelete(cleanupCtx, "root")
		_, _ = cli.RoleDelete(cleanupCtx, aliceRole)
		_, _ = cli.RoleDelete(cleanupCtx, "root")
	})

	aliceAuth, err := cli.Authenticate(ctx, "alice", alicePassword)
	require.NoError(t, err)
	post := newHTTPConcurrencyAuthzPoster(t, endpoint)
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	name := encode(prefix + "observe")
	campaignStatus, campaign := post("/v3/election/campaign", map[string]any{
		"name": name, "lease": strconv.FormatInt(int64(lease.ID), 10), "value": encode("initial"),
	}, aliceAuth.Token)
	require.Equal(t, http.StatusOK, campaignStatus, "%#v", campaign)
	leader := requireMapField(t, campaign, "leader")

	openObserve := func(token string) (*http.Response, *bufio.Reader) {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"name": name})
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost,
			strings.TrimRight(endpoint, "/")+"/v3/election/observe", bytes.NewReader(raw))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		return response, bufio.NewReader(response.Body)
	}
	readValue := func(reader *bufio.Reader) string {
		t.Helper()
		line, err := reader.ReadBytes('\n')
		require.NoError(t, err)
		frame := make(map[string]any)
		require.NoError(t, json.Unmarshal(line, &frame), "%q", line)
		result := requireMapField(t, frame, "result")
		kv := requireMapField(t, result, "kv")
		encodedValue, ok := kv["value"].(string)
		require.True(t, ok, "missing value in %#v", kv)
		value, err := base64.StdEncoding.DecodeString(encodedValue)
		require.NoError(t, err)
		return string(value)
	}
	probeObserve := func(token string) (int, bool) {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"name": name})
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost,
			strings.TrimRight(endpoint, "/")+"/v3/election/observe", bytes.NewReader(raw))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return response.StatusCode, len(body) == 0
	}

	firstResponse, firstReader := openObserve(aliceAuth.Token)
	outcome := httpElectionObserveAuthOutcome{
		InitialValue:       readValue(firstReader),
		ObserveContentType: firstResponse.Header.Get("Content-Type"),
		ObserveChunked:     containsString(firstResponse.TransferEncoding, "chunked"),
	}
	_, err = rootClient.RoleRevokePermission(ctx, aliceRole, prefix, clientv3.GetPrefixRangeEnd(prefix))
	require.NoError(t, err)
	rootAuth, err := cli.Authenticate(ctx, "root", rootPassword)
	require.NoError(t, err)
	proclaimStatus, proclaim := post("/v3/election/proclaim", map[string]any{
		"leader": leader, "value": encode("after-revoke"),
	}, rootAuth.Token)
	require.Equal(t, http.StatusOK, proclaimStatus, "%#v", proclaim)
	outcome.ExistingAfterRevokeValue = readValue(firstReader)

	outcome.OldTokenNewObserveStatus, outcome.OldTokenNewObserveEmpty = probeObserve(aliceAuth.Token)
	deniedAuth, err := cli.Authenticate(ctx, "alice", alicePassword)
	require.NoError(t, err)
	outcome.DeniedNewObserveStatus, outcome.DeniedNewObserveEmpty = probeObserve(deniedAuth.Token)

	_, err = rootClient.RoleGrantPermission(ctx, aliceRole, prefix, clientv3.GetPrefixRangeEnd(prefix),
		clientv3.PermissionType(clientv3.PermReadWrite))
	require.NoError(t, err)
	regrantedAuth, err := cli.Authenticate(ctx, "alice", alicePassword)
	require.NoError(t, err)
	secondResponse, secondReader := openObserve(regrantedAuth.Token)
	outcome.RegrantedInitialValue = readValue(secondReader)
	_, err = rootClient.UserChangePassword(ctx, "alice", aliceNewPass)
	require.NoError(t, err)
	rootAuth, err = cli.Authenticate(ctx, "root", rootPassword)
	require.NoError(t, err)
	proclaimStatus, proclaim = post("/v3/election/proclaim", map[string]any{
		"leader": leader, "value": encode("after-password"),
	}, rootAuth.Token)
	require.Equal(t, http.StatusOK, proclaimStatus, "%#v", proclaim)
	outcome.ExistingAfterPasswordValue = readValue(firstReader)
	outcome.RegrantedAfterPasswordValue = readValue(secondReader)
	outcome.InvalidNewObserveStatus, outcome.InvalidNewObserveEmpty = probeObserve(regrantedAuth.Token)

	require.NoError(t, firstResponse.Body.Close())
	require.NoError(t, secondResponse.Body.Close())
	resignStatus, resign := post("/v3/election/resign", map[string]any{"leader": leader}, rootAuth.Token)
	require.Equal(t, http.StatusOK, resignStatus, "%#v", resign)
	return outcome
}
