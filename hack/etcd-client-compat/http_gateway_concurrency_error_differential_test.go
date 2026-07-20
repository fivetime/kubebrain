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

type httpConcurrencyErrorOutcome struct {
	Name       string
	HTTPStatus int
	Code       int
	Message    string
}

func TestHTTPGatewayConcurrencyErrorsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcomes := runHTTPConcurrencyErrorMatrix(t, reference)
	require.Equal(t, []httpConcurrencyErrorOutcome{
		{Name: "lock-missing-lease", HTTPStatus: 500, Code: 2, Message: "etcdserver: requested lease not found"},
		{Name: "unlock-empty-key", HTTPStatus: 500, Code: 2, Message: "etcdserver: key is not provided"},
		{Name: "campaign-missing-lease", HTTPStatus: 500, Code: 2, Message: "etcdserver: requested lease not found"},
		{Name: "proclaim-missing-leader", HTTPStatus: 500, Code: 2, Message: `"leader" field must be provided`},
		{Name: "resign-missing-leader", HTTPStatus: 500, Code: 2, Message: `"leader" field must be provided`},
		{Name: "leader-not-found", HTTPStatus: 500, Code: 2, Message: "election: no leader"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runHTTPConcurrencyErrorMatrix(t, kubebrain))
}

func runHTTPConcurrencyErrorMatrix(t *testing.T, endpoint string) []httpConcurrencyErrorOutcome {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	cases := []struct {
		name string
		path string
		body string
	}{
		{name: "lock-missing-lease", path: "/v3/lock/lock", body: `{"name":"L2EzNjAvbG9jaw==","lease":"999999"}`},
		{name: "unlock-empty-key", path: "/v3/lock/unlock", body: `{"key":""}`},
		{name: "campaign-missing-lease", path: "/v3/election/campaign", body: `{"name":"L2EzNjAvZWxlY3Rpb24=","lease":"999999","value":"dg=="}`},
		{name: "proclaim-missing-leader", path: "/v3/election/proclaim", body: `{}`},
		{name: "resign-missing-leader", path: "/v3/election/resign", body: `{}`},
		{name: "leader-not-found", path: "/v3/election/leader", body: `{"name":"L2EzNjAvbm8tbGVhZGVy"}`},
	}
	outcomes := make([]httpConcurrencyErrorOutcome, 0, len(cases))
	for _, testCase := range cases {
		request, err := http.NewRequest(http.MethodPost, baseURL+testCase.path, bytes.NewBufferString(testCase.body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, testCase.name)
		responseBody, err := io.ReadAll(response.Body)
		require.NoError(t, err, testCase.name)
		require.NoError(t, response.Body.Close())
		decoded := make(map[string]any)
		require.NoError(t, json.Unmarshal(responseBody, &decoded), "%s: %s", testCase.name, responseBody)
		code, ok := decoded["code"].(float64)
		require.True(t, ok, "%s: %#v", testCase.name, decoded)
		message, ok := decoded["message"].(string)
		require.True(t, ok, "%s: %#v", testCase.name, decoded)
		outcomes = append(outcomes, httpConcurrencyErrorOutcome{
			Name: testCase.name, HTTPStatus: response.StatusCode, Code: int(code), Message: message,
		})
	}
	return outcomes
}
