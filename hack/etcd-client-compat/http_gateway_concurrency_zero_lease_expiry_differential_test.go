package compat

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type httpConcurrencyZeroLeaseExpiryOutcome struct {
	DefaultGrantedTTLs bool
	TTLsCountDown      bool
	LeasesExpired      bool
	KeysExpired        bool
	IndependentLeases  bool
}

func TestHTTPGatewayConcurrencyZeroLeaseExpiryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	var referenceOutcome, kubebrainOutcome httpConcurrencyZeroLeaseExpiryOutcome
	t.Run("parallel endpoints", func(t *testing.T) {
		t.Run("reference", func(t *testing.T) {
			t.Parallel()
			referenceOutcome = runHTTPConcurrencyZeroLeaseExpiryScenario(t, reference)
		})
		t.Run("kubebrain", func(t *testing.T) {
			t.Parallel()
			kubebrainOutcome = runHTTPConcurrencyZeroLeaseExpiryScenario(t, kubebrain)
		})
	})
	require.Equal(t, httpConcurrencyZeroLeaseExpiryOutcome{
		DefaultGrantedTTLs: true, TTLsCountDown: true, LeasesExpired: true,
		KeysExpired: true, IndependentLeases: true,
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, kubebrainOutcome)
}

func runHTTPConcurrencyZeroLeaseExpiryScenario(t *testing.T, endpoint string) httpConcurrencyZeroLeaseExpiryOutcome {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	post := func(path string, body any) (int, map[string]any, error) {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(raw))
		if err != nil {
			return 0, nil, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			return 0, nil, err
		}
		decoded := make(map[string]any)
		if err := json.Unmarshal(responseBody, &decoded); err != nil {
			return 0, nil, fmt.Errorf("decode %s response %q: %w", path, responseBody, err)
		}
		return response.StatusCode, decoded, nil
	}
	mustPost := func(path string, body any) map[string]any {
		t.Helper()
		statusCode, response, err := post(path, body)
		require.NoError(t, err, path)
		require.Equal(t, http.StatusOK, statusCode, "%s: %#v", path, response)
		return response
	}
	stringField := func(value map[string]any, field string) string {
		t.Helper()
		fieldValue, ok := value[field].(string)
		require.True(t, ok, "missing string field %q in %#v", field, value)
		return fieldValue
	}
	leaseTTL := func(leaseID string) (int64, int64) {
		t.Helper()
		response := mustPost("/v3/lease/timetolive", map[string]any{"ID": leaseID})
		var ttl int64
		rawTTL, ttlFound := response["TTL"]
		if ttlFound {
			ttlString, ok := rawTTL.(string)
			require.True(t, ok, "non-string TTL in %#v", response)
			var err error
			ttl, err = strconv.ParseInt(ttlString, 10, 64)
			require.NoError(t, err)
		}
		rawGrantedTTL, found := response["grantedTTL"]
		if !found {
			return ttl, 0
		}
		grantedTTLString, ok := rawGrantedTTL.(string)
		require.True(t, ok, "non-string grantedTTL in %#v", response)
		grantedTTL, err := strconv.ParseInt(grantedTTLString, 10, 64)
		require.NoError(t, err)
		return ttl, grantedTTL
	}
	keyLease := func(encodedKey string) string {
		t.Helper()
		response := mustPost("/v3/kv/range", map[string]any{"key": encodedKey})
		kvs := requireSliceField(t, response, "kvs")
		require.Len(t, kvs, 1)
		return stringField(kvs[0].(map[string]any), "lease")
	}
	keyMissing := func(encodedKey string) bool {
		statusCode, response, err := post("/v3/kv/range", map[string]any{"key": encodedKey})
		if err != nil || statusCode != http.StatusOK {
			return false
		}
		kvs, found := response["kvs"]
		return !found || len(kvs.([]any)) == 0
	}

	lock := mustPost("/v3/lock/lock", map[string]any{"name": encode("/a362/expiry/lock")})
	lockKey := stringField(lock, "key")
	lockLease := keyLease(lockKey)
	campaign := mustPost("/v3/election/campaign", map[string]any{
		"name": encode("/a362/expiry/election"), "value": encode("leader"),
	})
	leader := requireMapField(t, campaign, "leader")
	campaignKey := stringField(leader, "key")
	campaignLease := stringField(leader, "lease")
	require.Equal(t, campaignLease, keyLease(campaignKey))
	t.Cleanup(func() {
		_, _, _ = post("/v3/lease/revoke", map[string]any{"ID": lockLease})
		_, _, _ = post("/v3/lease/revoke", map[string]any{"ID": campaignLease})
	})

	lockInitialTTL, lockGrantedTTL := leaseTTL(lockLease)
	campaignInitialTTL, campaignGrantedTTL := leaseTTL(campaignLease)
	time.Sleep(2 * time.Second)
	lockLaterTTL, _ := leaseTTL(lockLease)
	campaignLaterTTL, _ := leaseTTL(campaignLease)

	require.Eventually(t, func() bool {
		lockTTL, _ := leaseTTL(lockLease)
		campaignTTL, _ := leaseTTL(campaignLease)
		return lockTTL == -1 && campaignTTL == -1 && keyMissing(lockKey) && keyMissing(campaignKey)
	}, 75*time.Second, time.Second)
	lockExpiredTTL, _ := leaseTTL(lockLease)
	campaignExpiredTTL, _ := leaseTTL(campaignLease)

	return httpConcurrencyZeroLeaseExpiryOutcome{
		DefaultGrantedTTLs: lockGrantedTTL == 60 && campaignGrantedTTL == 60,
		TTLsCountDown: lockInitialTTL > lockLaterTTL && lockLaterTTL > 0 &&
			campaignInitialTTL > campaignLaterTTL && campaignLaterTTL > 0,
		LeasesExpired:     lockExpiredTTL == -1 && campaignExpiredTTL == -1,
		KeysExpired:       keyMissing(lockKey) && keyMissing(campaignKey),
		IndependentLeases: lockLease != campaignLease,
	}
}
