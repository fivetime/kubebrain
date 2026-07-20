package compat

import (
	"bytes"
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
)

type httpGatewayOutcome struct {
	PutContentType       string
	RangeContentType     string
	RangeKey             string
	RangeValue           string
	RangeVersion         string
	TxnSucceeded         bool
	TxnPreviousValue     string
	UpdatedValue         string
	UpdatedVersion       string
	LeaseID              string
	LeaseTTL             string
	LeaseKeyAttached     bool
	MemberListNonEmpty   bool
	StatusVersionPresent bool
	AuthEnabled          bool
	DeleteCount          string
	InvalidStatus        int
	InvalidCode          int
}

type httpGatewayAuthOutcome struct {
	AnonymousStatus  int
	AnonymousCode    int
	TokenNonEmpty    bool
	AuthorizedStatus int
	DisableStatus    int
}

func TestHTTPGatewayDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	require.Equal(t, runHTTPGatewayScenario(t, reference), runHTTPGatewayScenario(t, kubebrain))
}

func TestHTTPGatewayAuthDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	require.Equal(t, runHTTPGatewayAuthScenario(t, reference), runHTTPGatewayAuthScenario(t, kubebrain))
}

func runHTTPGatewayScenario(t *testing.T, endpoint string) httpGatewayOutcome {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	key := []byte("/a355/http-gateway")
	leasedKey := []byte("/a355/http-gateway-lease")
	const leaseID = "737373"

	post := func(path, body string) (int, http.Header, map[string]any) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewBufferString(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, path)
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		require.NoError(t, err, path)
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(raw, &decoded), "%s: %s", path, raw)
		return response.StatusCode, response.Header, decoded
	}
	encoded := func(value []byte) string { return base64.StdEncoding.EncodeToString(value) }
	stringField := func(value map[string]any, field string) string {
		t.Helper()
		got, ok := value[field].(string)
		require.True(t, ok, "missing string field %q in %#v", field, value)
		return got
	}
	boolField := func(value map[string]any, field string) bool {
		t.Helper()
		got, ok := value[field].(bool)
		require.True(t, ok, "missing bool field %q in %#v", field, value)
		return got
	}
	optionalBoolField := func(value map[string]any, field string) bool {
		t.Helper()
		got, found := value[field]
		if !found {
			return false
		}
		enabled, ok := got.(bool)
		require.True(t, ok, "non-bool field %q in %#v", field, value)
		return enabled
	}

	keyJSON := encoded(key)
	leasedKeyJSON := encoded(leasedKey)
	_, _, _ = post("/v3/kv/deleterange", fmt.Sprintf(`{"key":%q}`, keyJSON))
	_, _, _ = post("/v3/kv/deleterange", fmt.Sprintf(`{"key":%q}`, leasedKeyJSON))
	_, _, _ = post("/v3/lease/revoke", fmt.Sprintf(`{"ID":%q}`, leaseID))

	status, header, put := post("/v3/kv/put",
		fmt.Sprintf(`{"key":%q,"value":%q}`, keyJSON, encoded([]byte("value-one"))))
	require.Equal(t, http.StatusOK, status)
	putHeader := requireMapField(t, put, "header")
	putRevision := stringField(putHeader, "revision")

	status, rangeHeader, ranged := post("/v3/kv/range", fmt.Sprintf(`{"key":%q}`, keyJSON))
	require.Equal(t, http.StatusOK, status)
	kvs := requireSliceField(t, ranged, "kvs")
	require.Len(t, kvs, 1)
	kv, ok := kvs[0].(map[string]any)
	require.True(t, ok)

	status, _, txn := post("/v3/kv/txn", fmt.Sprintf(`{
		"compare":[{"key":%q,"target":"MOD","result":"EQUAL","mod_revision":%q}],
		"success":[{"request_put":{"key":%q,"value":%q,"prev_kv":true}}],
		"failure":[{"request_range":{"key":%q}}]
	}`, keyJSON, putRevision, keyJSON, encoded([]byte("value-two")), keyJSON))
	require.Equal(t, http.StatusOK, status)
	responses := requireSliceField(t, txn, "responses")
	require.Len(t, responses, 1)
	putResponse := requireMapField(t, responses[0].(map[string]any), "response_put")
	previous := requireMapField(t, putResponse, "prev_kv")

	status, _, updated := post("/v3/kv/range", fmt.Sprintf(`{"key":%q}`, keyJSON))
	require.Equal(t, http.StatusOK, status)
	updatedKV := requireSliceField(t, updated, "kvs")[0].(map[string]any)

	status, _, lease := post("/v3/lease/grant", fmt.Sprintf(`{"TTL":"60","ID":%q}`, leaseID))
	require.Equal(t, http.StatusOK, status)
	status, _, _ = post("/v3/kv/put",
		fmt.Sprintf(`{"key":%q,"value":%q,"lease":%q}`, leasedKeyJSON, encoded([]byte("leased")), leaseID))
	require.Equal(t, http.StatusOK, status)
	status, _, ttl := post("/v3/lease/timetolive", fmt.Sprintf(`{"ID":%q,"keys":true}`, leaseID))
	require.Equal(t, http.StatusOK, status)
	leaseKeys := requireSliceField(t, ttl, "keys")

	status, _, members := post("/v3/cluster/member/list", `{}`)
	require.Equal(t, http.StatusOK, status)
	status, _, maintenance := post("/v3/maintenance/status", `{}`)
	require.Equal(t, http.StatusOK, status)
	status, _, auth := post("/v3/auth/status", `{}`)
	require.Equal(t, http.StatusOK, status)

	status, _, deleted := post("/v3/kv/deleterange", fmt.Sprintf(`{"key":%q}`, keyJSON))
	require.Equal(t, http.StatusOK, status)
	_, _, _ = post("/v3/lease/revoke", fmt.Sprintf(`{"ID":%q}`, leaseID))

	invalidStatus, _, invalid := post("/v3/kv/range", `{"key":"***"}`)
	invalidCodeValue, ok := invalid["code"].(float64)
	require.True(t, ok, "missing numeric code in %#v", invalid)

	return httpGatewayOutcome{
		PutContentType:       header.Get("Content-Type"),
		RangeContentType:     rangeHeader.Get("Content-Type"),
		RangeKey:             stringField(kv, "key"),
		RangeValue:           stringField(kv, "value"),
		RangeVersion:         stringField(kv, "version"),
		TxnSucceeded:         boolField(txn, "succeeded"),
		TxnPreviousValue:     stringField(previous, "value"),
		UpdatedValue:         stringField(updatedKV, "value"),
		UpdatedVersion:       stringField(updatedKV, "version"),
		LeaseID:              stringField(lease, "ID"),
		LeaseTTL:             stringField(lease, "TTL"),
		LeaseKeyAttached:     len(leaseKeys) == 1 && leaseKeys[0] == leasedKeyJSON,
		MemberListNonEmpty:   len(requireSliceField(t, members, "members")) > 0,
		StatusVersionPresent: stringField(maintenance, "version") != "",
		AuthEnabled:          optionalBoolField(auth, "enabled"),
		DeleteCount:          stringField(deleted, "deleted"),
		InvalidStatus:        invalidStatus,
		InvalidCode:          int(invalidCodeValue),
	}
}

func runHTTPGatewayAuthScenario(t *testing.T, endpoint string) httpGatewayAuthOutcome {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	post := func(path, body, token string) (int, map[string]any) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewBufferString(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		if token != "" {
			request.Header.Set("Authorization", token)
		}
		response, err := client.Do(request)
		require.NoError(t, err, path)
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		require.NoError(t, err, path)
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(raw, &decoded), "%s: %s", path, raw)
		return response.StatusCode, decoded
	}

	_, _ = post("/v3/auth/user/delete", `{"name":"root"}`, "")
	_, _ = post("/v3/auth/role/delete", `{"role":"root"}`, "")
	status, _ := post("/v3/auth/user/add", `{"name":"root","password":"a355-password"}`, "")
	require.Equal(t, http.StatusOK, status)
	status, _ = post("/v3/auth/role/add", `{"name":"root"}`, "")
	require.Equal(t, http.StatusOK, status)
	status, _ = post("/v3/auth/user/grant", `{"user":"root","role":"root"}`, "")
	require.Equal(t, http.StatusOK, status)
	status, _ = post("/v3/auth/enable", `{}`, "")
	require.Equal(t, http.StatusOK, status)

	anonymousStatus, anonymous := post("/v3/kv/range", `{"key":"L2EzNTUv"}`, "")
	codeValue, ok := anonymous["code"].(float64)
	require.True(t, ok, "missing anonymous error code in %#v", anonymous)

	status, authenticated := post("/v3/auth/authenticate",
		`{"name":"root","password":"a355-password"}`, "")
	require.Equal(t, http.StatusOK, status)
	token, ok := authenticated["token"].(string)
	require.True(t, ok)
	require.NotEmpty(t, token)

	authorizedStatus, _ := post("/v3/kv/range", `{"key":"L2EzNTUv"}`, token)
	disableStatus, _ := post("/v3/auth/disable", `{}`, token)
	require.Equal(t, http.StatusOK, authorizedStatus)
	status, _ = post("/v3/auth/user/delete", `{"name":"root"}`, token)
	require.Equal(t, http.StatusOK, status)
	status, _ = post("/v3/auth/role/delete", `{"role":"root"}`, token)
	require.Equal(t, http.StatusOK, status)

	return httpGatewayAuthOutcome{
		AnonymousStatus:  anonymousStatus,
		AnonymousCode:    int(codeValue),
		TokenNonEmpty:    token != "",
		AuthorizedStatus: authorizedStatus,
		DisableStatus:    disableStatus,
	}
}

func requireMapField(t *testing.T, value map[string]any, field string) map[string]any {
	t.Helper()
	got, ok := value[field].(map[string]any)
	require.True(t, ok, "missing object field %q in %#v", field, value)
	return got
}

func requireSliceField(t *testing.T, value map[string]any, field string) []any {
	t.Helper()
	got, ok := value[field].([]any)
	require.True(t, ok, "missing array field %q in %#v", field, value)
	return got
}
