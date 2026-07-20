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
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestHTTPGatewayZeroLeaseExpiryAcrossLeaderFailover verifies that the
// dedicated Lock and Election services do not leave an in-process keepalive
// behind after orphaning their implicit sessions. The failover command must
// discover and delete the current KubeBrain leader.
func TestHTTPGatewayZeroLeaseExpiryAcrossLeaderFailover(t *testing.T) {
	gatewayEndpoint := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	etcdEndpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	failoverCommand := os.Getenv("KUBEBRAIN_ZERO_LEASE_EXPIRY_FAILOVER_COMMAND")
	if gatewayEndpoint == "" || etcdEndpoint == "" || failoverCommand == "" {
		t.Skip("set KUBEBRAIN_GATEWAY_ENDPOINT, KUBEBRAIN_ETCD_ENDPOINT, and KUBEBRAIN_ZERO_LEASE_EXPIRY_FAILOVER_COMMAND")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdEndpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	post := newConcurrencyFailoverHTTPPoster(t, gatewayEndpoint)
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	prefix := fmt.Sprintf("/a363/zero-lease-failover/%d", time.Now().UnixNano())

	lockResponse := post("/v3/lock/lock", map[string]any{"name": encode(prefix + "/lock")})
	lockKey := decodeConcurrencyFailoverField(t, lockResponse, "key")
	lockLease := concurrencyFailoverKeyLease(t, ctx, cli, lockKey)

	campaignResponse := post("/v3/election/campaign", map[string]any{
		"name": encode(prefix + "/election"), "value": encode("leader"),
	})
	leader := requireMapField(t, campaignResponse, "leader")
	campaignKey := decodeConcurrencyFailoverField(t, leader, "key")
	campaignLease := clientv3.LeaseID(parseConcurrencyFailoverInt64Field(t, leader, "lease"))
	require.Equal(t, campaignLease, concurrencyFailoverKeyLease(t, ctx, cli, campaignKey))
	require.NotEqual(t, lockLease, campaignLease)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, leaseID := range []clientv3.LeaseID{lockLease, campaignLease} {
			if ttl, ttlErr := cli.TimeToLive(cleanupCtx, leaseID); ttlErr == nil && ttl.TTL != -1 {
				_, _ = cli.Revoke(cleanupCtx, leaseID)
			}
		}
	})

	lockInitial := concurrencyFailoverLeaseTTL(t, ctx, cli, lockLease)
	campaignInitial := concurrencyFailoverLeaseTTL(t, ctx, cli, campaignLease)
	require.Equal(t, int64(60), lockInitial.GrantedTTL)
	require.Equal(t, int64(60), campaignInitial.GrantedTTL)
	require.Positive(t, lockInitial.TTL)
	require.Positive(t, campaignInitial.TTL)

	var lockBeforeFailover, campaignBeforeFailover int64
	require.Eventually(t, func() bool {
		lockTTL, lockErr := cli.TimeToLive(ctx, lockLease)
		campaignTTL, campaignErr := cli.TimeToLive(ctx, campaignLease)
		ready := lockErr == nil && campaignErr == nil &&
			lockTTL.TTL > 0 && lockTTL.TTL <= 30 &&
			campaignTTL.TTL > 0 && campaignTTL.TTL <= 30
		if ready {
			lockBeforeFailover = lockTTL.TTL
			campaignBeforeFailover = campaignTTL.TTL
		}
		return ready
	}, 40*time.Second, 250*time.Millisecond, "implicit session leases must count down before failover")

	failoverStarted := time.Now()
	output, err := exec.CommandContext(ctx, "bash", "-c", failoverCommand).CombinedOutput()
	require.NoErrorf(t, err, "delete current leader: %s", strings.TrimSpace(string(output)))
	t.Logf("leader replacement command completed in %s: %s", time.Since(failoverStarted), strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(ctx, namespace)
	require.NoErrorf(t, err, "wait for replacement replica: %s", strings.TrimSpace(string(output)))

	// Match etcd lessor.Promote: without a five-minute checkpoint, promotion
	// refreshes each lease from its granted TTL. This is a one-time failover
	// extension, not evidence that the orphan session resumed its keepalive.
	lockAfterFailover := concurrencyFailoverLeaseTTL(t, ctx, cli, lockLease)
	campaignAfterFailover := concurrencyFailoverLeaseTTL(t, ctx, cli, campaignLease)
	require.Greater(t, lockAfterFailover.TTL, lockBeforeFailover+20)
	require.Greater(t, campaignAfterFailover.TTL, campaignBeforeFailover+20)
	require.LessOrEqual(t, lockAfterFailover.TTL, int64(65))
	require.LessOrEqual(t, campaignAfterFailover.TTL, int64(65))
	require.Equal(t, int64(60), lockAfterFailover.GrantedTTL)
	require.Equal(t, int64(60), campaignAfterFailover.GrantedTTL)

	require.Eventually(t, func() bool {
		lockTTL, lockErr := cli.TimeToLive(ctx, lockLease)
		campaignTTL, campaignErr := cli.TimeToLive(ctx, campaignLease)
		if lockErr != nil || campaignErr != nil || lockTTL.TTL != -1 || campaignTTL.TTL != -1 {
			return false
		}
		lock, lockErr := cli.Get(ctx, lockKey)
		campaign, campaignErr := cli.Get(ctx, campaignKey)
		return lockErr == nil && campaignErr == nil && len(lock.Kvs) == 0 && len(campaign.Kvs) == 0
	}, 75*time.Second, 250*time.Millisecond,
		"successor must expire both orphan sessions and delete their concurrency keys")
}

func newConcurrencyFailoverHTTPPoster(t *testing.T, endpoint string) func(string, any) map[string]any {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	return func(path string, body any) map[string]any {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(raw))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, path)
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		require.NoError(t, err, path)
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(responseBody, &decoded), "%s: %q", path, responseBody)
		require.Equal(t, http.StatusOK, response.StatusCode, "%s: %#v", path, decoded)
		return decoded
	}
}

func decodeConcurrencyFailoverField(t *testing.T, value map[string]any, field string) string {
	t.Helper()
	encoded, ok := value[field].(string)
	require.True(t, ok, "missing string field %q in %#v", field, value)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	return string(decoded)
}

func parseConcurrencyFailoverInt64Field(t *testing.T, value map[string]any, field string) int64 {
	t.Helper()
	raw, ok := value[field].(string)
	require.True(t, ok, "missing string field %q in %#v", field, value)
	parsed, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err)
	return parsed
}

func concurrencyFailoverKeyLease(
	t *testing.T, ctx context.Context, cli *clientv3.Client, key string,
) clientv3.LeaseID {
	t.Helper()
	response, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	require.NotZero(t, response.Kvs[0].Lease)
	return clientv3.LeaseID(response.Kvs[0].Lease)
}

func concurrencyFailoverLeaseTTL(
	t *testing.T, ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID,
) *clientv3.LeaseTimeToLiveResponse {
	t.Helper()
	response, err := cli.TimeToLive(ctx, leaseID)
	require.NoError(t, err)
	return response
}
