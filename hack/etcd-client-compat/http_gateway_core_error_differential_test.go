package compat

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type httpGatewayCoreErrorOutcome struct {
	Name       string
	HTTPStatus int
	Code       int
	Message    string
}

func TestHTTPGatewayCoreErrorsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}

	referenceOutcomes := runHTTPGatewayCoreErrorMatrix(t, reference)
	require.Equal(t, []httpGatewayCoreErrorOutcome{
		{Name: "range-empty-key", HTTPStatus: 400, Code: 3, Message: "etcdserver: key is not provided"},
		{Name: "put-empty-key", HTTPStatus: 400, Code: 3, Message: "etcdserver: key is not provided"},
		{Name: "delete-empty-key", HTTPStatus: 400, Code: 3, Message: "etcdserver: key is not provided"},
		{Name: "txn-empty-op", HTTPStatus: 400, Code: 3, Message: "etcdserver: key not found"},
		{Name: "compact-negative-revision", HTTPStatus: 400, Code: 11, Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "revoke-zero-lease", HTTPStatus: 404, Code: 5, Message: "etcdserver: requested lease not found"},
		{Name: "remove-zero-member", HTTPStatus: 404, Code: 5, Message: "etcdserver: member not found"},
		{Name: "hashkv-future-revision", HTTPStatus: 400, Code: 11, Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "get-empty-user", HTTPStatus: 400, Code: 9, Message: "etcdserver: user name not found"},
		{Name: "get-empty-role", HTTPStatus: 400, Code: 9, Message: "etcdserver: role name not found"},
		{Name: "grant-missing-permission", HTTPStatus: 400, Code: 3, Message: "etcdserver: permission not given"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runHTTPGatewayCoreErrorMatrix(t, kubebrain))
}

func runHTTPGatewayCoreErrorMatrix(t *testing.T, endpoint string) []httpGatewayCoreErrorOutcome {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	cases := []struct {
		name string
		path string
		body string
	}{
		{name: "range-empty-key", path: "/v3/kv/range", body: `{}`},
		{name: "put-empty-key", path: "/v3/kv/put", body: `{}`},
		{name: "delete-empty-key", path: "/v3/kv/deleterange", body: `{}`},
		{name: "txn-empty-op", path: "/v3/kv/txn", body: `{"success":[{}]}`},
		{name: "compact-negative-revision", path: "/v3/kv/compaction", body: `{"revision":"-1"}`},
		{name: "revoke-zero-lease", path: "/v3/lease/revoke", body: `{"ID":"0"}`},
		{name: "remove-zero-member", path: "/v3/cluster/member/remove", body: `{"ID":"0"}`},
		{name: "hashkv-future-revision", path: "/v3/maintenance/hashkv", body: `{"revision":"9223372036854775807"}`},
		{name: "get-empty-user", path: "/v3/auth/user/get", body: `{}`},
		{name: "get-empty-role", path: "/v3/auth/role/get", body: `{}`},
		{name: "grant-missing-permission", path: "/v3/auth/role/grant", body: `{"name":"missing"}`},
	}

	outcomes := make([]httpGatewayCoreErrorOutcome, 0, len(cases))
	for _, testCase := range cases {
		request, err := http.NewRequest(http.MethodPost, baseURL+testCase.path, bytes.NewBufferString(testCase.body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, testCase.name)
		responseBody, err := io.ReadAll(response.Body)
		require.NoError(t, err, testCase.name)
		require.NoError(t, response.Body.Close())
		var decoded struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		require.NoError(t, json.Unmarshal(responseBody, &decoded), "%s: %s", testCase.name, responseBody)
		outcomes = append(outcomes, httpGatewayCoreErrorOutcome{
			Name: testCase.name, HTTPStatus: response.StatusCode, Code: decoded.Code, Message: decoded.Message,
		})
	}
	return outcomes
}
