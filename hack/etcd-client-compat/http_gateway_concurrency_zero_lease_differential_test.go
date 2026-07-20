package compat

import (
	"bytes"
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
)

type httpConcurrencyZeroLeaseOutcome struct {
	LockLeaseAllocated            bool
	LockKeyHasLease               bool
	LockGrantedTTLDefault         bool
	LockLeaseAliveAfterUnlock     bool
	LockKeyDeleted                bool
	CampaignLeaseAllocated        bool
	CampaignKeyLeaseMatches       bool
	CampaignGrantedTTLDefault     bool
	CampaignLeaseAliveAfterResign bool
	CampaignKeyDeleted            bool
	IndependentLeases             bool
}

func TestHTTPGatewayConcurrencyZeroLeaseDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcome := runHTTPConcurrencyZeroLeaseScenario(t, reference)
	require.Equal(t, httpConcurrencyZeroLeaseOutcome{
		LockLeaseAllocated: true, LockKeyHasLease: true, LockGrantedTTLDefault: true,
		LockLeaseAliveAfterUnlock: true, LockKeyDeleted: true,
		CampaignLeaseAllocated: true, CampaignKeyLeaseMatches: true, CampaignGrantedTTLDefault: true,
		CampaignLeaseAliveAfterResign: true, CampaignKeyDeleted: true, IndependentLeases: true,
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, runHTTPConcurrencyZeroLeaseScenario(t, kubebrain))
}

func runHTTPConcurrencyZeroLeaseScenario(t *testing.T, endpoint string) httpConcurrencyZeroLeaseOutcome {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	post := func(path string, body any) map[string]any {
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
		require.Equal(t, http.StatusOK, response.StatusCode, "%s: %s", path, responseBody)
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(responseBody, &decoded), "%s: %s", path, responseBody)
		return decoded
	}
	stringField := func(value map[string]any, field string) string {
		t.Helper()
		fieldValue, ok := value[field].(string)
		require.True(t, ok, "missing string field %q in %#v", field, value)
		return fieldValue
	}
	leaseTTL := func(leaseID string) (int64, int64) {
		t.Helper()
		response := post("/v3/lease/timetolive", map[string]any{"ID": leaseID})
		ttl, err := strconv.ParseInt(stringField(response, "TTL"), 10, 64)
		require.NoError(t, err)
		grantedTTL, err := strconv.ParseInt(stringField(response, "grantedTTL"), 10, 64)
		require.NoError(t, err)
		return ttl, grantedTTL
	}
	keyLease := func(encodedKey string) string {
		t.Helper()
		response := post("/v3/kv/range", map[string]any{"key": encodedKey})
		kvs := requireSliceField(t, response, "kvs")
		require.Len(t, kvs, 1)
		return stringField(kvs[0].(map[string]any), "lease")
	}
	keyDeleted := func(encodedKey string) bool {
		t.Helper()
		response := post("/v3/kv/range", map[string]any{"key": encodedKey})
		kvs, found := response["kvs"]
		if !found {
			return true
		}
		return len(kvs.([]any)) == 0
	}

	lock := post("/v3/lock/lock", map[string]any{"name": encode("/a361/zero-lease/lock")})
	lockKey := stringField(lock, "key")
	lockLease := keyLease(lockKey)
	lockTTL, lockGrantedTTL := leaseTTL(lockLease)
	post("/v3/lock/unlock", map[string]any{"key": lockKey})
	lockTTLAfterUnlock, _ := leaseTTL(lockLease)
	lockDeleted := keyDeleted(lockKey)
	post("/v3/lease/revoke", map[string]any{"ID": lockLease})

	campaign := post("/v3/election/campaign", map[string]any{
		"name": encode("/a361/zero-lease/election"), "value": encode("leader"),
	})
	leader := requireMapField(t, campaign, "leader")
	campaignKey := stringField(leader, "key")
	campaignLease := stringField(leader, "lease")
	campaignKeyLease := keyLease(campaignKey)
	campaignTTL, campaignGrantedTTL := leaseTTL(campaignLease)
	post("/v3/election/resign", map[string]any{"leader": leader})
	campaignTTLAfterResign, _ := leaseTTL(campaignLease)
	campaignDeleted := keyDeleted(campaignKey)
	post("/v3/lease/revoke", map[string]any{"ID": campaignLease})

	return httpConcurrencyZeroLeaseOutcome{
		LockLeaseAllocated:            lockLease != "" && lockLease != "0",
		LockKeyHasLease:               lockLease != "0",
		LockGrantedTTLDefault:         lockGrantedTTL == 60 && lockTTL > 0,
		LockLeaseAliveAfterUnlock:     lockTTLAfterUnlock > 0,
		LockKeyDeleted:                lockDeleted,
		CampaignLeaseAllocated:        campaignLease != "" && campaignLease != "0",
		CampaignKeyLeaseMatches:       campaignKeyLease == campaignLease,
		CampaignGrantedTTLDefault:     campaignGrantedTTL == 60 && campaignTTL > 0,
		CampaignLeaseAliveAfterResign: campaignTTLAfterResign > 0,
		CampaignKeyDeleted:            campaignDeleted,
		IndependentLeases:             lockLease != campaignLease,
	}
}
