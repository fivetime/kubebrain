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

type httpConcurrencyErrorOutcome struct {
	Name        string
	HTTPStatus  int
	Code        int
	Message     string
	RevisionGap int64
	SeedValue   string
}

func TestHTTPGatewayConcurrencyErrorsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcomes := runHTTPConcurrencyErrorMatrix(t, reference)
	require.Equal(t, []httpConcurrencyErrorOutcome{
		{Name: "lock-missing-lease", HTTPStatus: 500, Code: 2, Message: "etcdserver: requested lease not found", SeedValue: "seed"},
		{Name: "unlock-empty-key", HTTPStatus: 500, Code: 2, Message: "etcdserver: key is not provided", SeedValue: "seed"},
		{Name: "campaign-missing-lease", HTTPStatus: 500, Code: 2, Message: "etcdserver: requested lease not found", SeedValue: "seed"},
		{Name: "proclaim-missing-leader", HTTPStatus: 500, Code: 2, Message: `"leader" field must be provided`, SeedValue: "seed"},
		{Name: "proclaim-empty-leader-key", HTTPStatus: 500, Code: 2, Message: "etcdserver: key is not provided", SeedValue: "seed"},
		{Name: "resign-missing-leader", HTTPStatus: 500, Code: 2, Message: `"leader" field must be provided`, SeedValue: "seed"},
		{Name: "resign-empty-leader-key", HTTPStatus: 500, Code: 2, Message: "etcdserver: key is not provided", SeedValue: "seed"},
		{Name: "leader-not-found", HTTPStatus: 500, Code: 2, Message: "election: no leader", SeedValue: "seed"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runHTTPConcurrencyErrorMatrix(t, kubebrain))
}

func runHTTPConcurrencyErrorMatrix(t *testing.T, endpoint string) []httpConcurrencyErrorOutcome {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	prefix := fmt.Sprintf("/a3374/http-concurrency-errors/%d/", time.Now().UnixNano())
	encode := func(value []byte) string { return base64.StdEncoding.EncodeToString(value) }
	post := func(path string, body any) (int, map[string]any) {
		t.Helper()
		rawBody, err := json.Marshal(body)
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(rawBody))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, path)
		responseBody, err := io.ReadAll(response.Body)
		require.NoError(t, err, path)
		require.NoError(t, response.Body.Close())
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(responseBody, &decoded), "%s: %s", path, responseBody)
		return response.StatusCode, decoded
	}
	revision := func(response map[string]any) int64 {
		t.Helper()
		header, ok := response["header"].(map[string]any)
		require.True(t, ok, "missing header in %#v", response)
		raw, ok := header["revision"].(string)
		require.True(t, ok, "missing revision in %#v", header)
		value, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err)
		return value
	}
	seedKey := []byte(prefix + "seed")
	t.Cleanup(func() {
		statusCode, response := post("/v3/kv/deleterange", map[string]any{
			"key": encode([]byte(prefix)), "range_end": encode([]byte(clientPrefixRangeEnd(prefix))),
		})
		require.Equal(t, http.StatusOK, statusCode, response)
		require.Equal(t, "1", response["deleted"], response)
		statusCode, response = post("/v3/kv/range", map[string]any{
			"key": encode([]byte(prefix)), "range_end": encode([]byte(clientPrefixRangeEnd(prefix))),
			"limit": "1",
		})
		require.Equal(t, http.StatusOK, statusCode, response)
		require.NotContains(t, response, "kvs", response)
		require.NotContains(t, response, "count", "protojson must omit the zero count: %#v", response)
	})
	seedStatus, seed := post("/v3/kv/put", map[string]any{"key": encode(seedKey), "value": encode([]byte("seed"))})
	require.Equal(t, http.StatusOK, seedStatus, seed)
	seedRevision := revision(seed)
	seedState := func(name string) (int64, string) {
		t.Helper()
		statusCode, ranged := post("/v3/kv/range", map[string]any{"key": encode(seedKey)})
		require.Equal(t, http.StatusOK, statusCode, name)
		kvs, ok := ranged["kvs"].([]any)
		require.True(t, ok, "%s: %#v", name, ranged)
		require.Len(t, kvs, 1, name)
		kv, ok := kvs[0].(map[string]any)
		require.True(t, ok, "%s: %#v", name, kvs[0])
		rawValue, ok := kv["value"].(string)
		require.True(t, ok, "%s: %#v", name, kv)
		value, err := base64.StdEncoding.DecodeString(rawValue)
		require.NoError(t, err, name)
		return revision(ranged) - seedRevision, string(value)
	}
	leaseID := time.Now().UnixNano() & ((1 << 62) - 1)
	grantStatus, grant := post("/v3/lease/grant", map[string]any{
		"ID": strconv.FormatInt(leaseID, 10), "TTL": "60",
	})
	require.Equal(t, http.StatusOK, grantStatus, grant)
	t.Cleanup(func() {
		statusCode, response := post("/v3/lease/revoke", map[string]any{
			"ID": strconv.FormatInt(leaseID, 10),
		})
		require.Equal(t, http.StatusOK, statusCode, response)
	})
	emptyLeader := fmt.Sprintf(
		`{"leader":{"name":%q,"key":"","rev":"1","lease":%q}}`,
		encode([]byte(prefix+"election")), strconv.FormatInt(leaseID, 10),
	)
	cases := []struct {
		name string
		path string
		body string
	}{
		{name: "lock-missing-lease", path: "/v3/lock/lock", body: fmt.Sprintf(`{"name":%q,"lease":"999999"}`, encode([]byte(prefix+"lock")))},
		{name: "unlock-empty-key", path: "/v3/lock/unlock", body: `{"key":""}`},
		{name: "campaign-missing-lease", path: "/v3/election/campaign", body: fmt.Sprintf(`{"name":%q,"lease":"999999","value":"dg=="}`, encode([]byte(prefix+"election")))},
		{name: "proclaim-missing-leader", path: "/v3/election/proclaim", body: `{}`},
		{name: "proclaim-empty-leader-key", path: "/v3/election/proclaim", body: emptyLeader},
		{name: "resign-missing-leader", path: "/v3/election/resign", body: `{}`},
		{name: "resign-empty-leader-key", path: "/v3/election/resign", body: emptyLeader},
		{name: "leader-not-found", path: "/v3/election/leader", body: fmt.Sprintf(`{"name":%q}`, encode([]byte(prefix+"no-leader")))},
	}
	outcomes := make([]httpConcurrencyErrorOutcome, 0, len(cases))
	for _, testCase := range cases {
		responseStatus, decoded := post(testCase.path, json.RawMessage(testCase.body))
		code, ok := decoded["code"].(float64)
		require.True(t, ok, "%s: %#v", testCase.name, decoded)
		message, ok := decoded["message"].(string)
		require.True(t, ok, "%s: %#v", testCase.name, decoded)
		gap, value := seedState(testCase.name)
		require.Zero(t, gap, testCase.name)
		require.Equal(t, "seed", value, testCase.name)
		outcomes = append(outcomes, httpConcurrencyErrorOutcome{
			Name: testCase.name, HTTPStatus: responseStatus, Code: int(code), Message: message,
			RevisionGap: gap, SeedValue: value,
		})
	}
	return outcomes
}
