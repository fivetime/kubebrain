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

type httpGatewayIntegerBoundaryOutcome struct {
	Name          string
	Status        int
	Code          int
	Message       string
	HeaderPresent bool
}

func TestHTTPGatewayIntegerBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}

	referenceOutcomes := runHTTPGatewayIntegerBoundaryScenario(t, reference)
	require.Equal(t, []httpGatewayIntegerBoundaryOutcome{
		{Name: "uint64-max-string", Status: http.StatusOK, HeaderPresent: true},
		{Name: "uint64-max-number", Status: http.StatusOK, HeaderPresent: true},
		{Name: "uint64-overflow-string", Status: http.StatusBadRequest, Code: 3, Message: `proto: (line 1:28): invalid value for uint64 field memberID: "18446744073709551616"`},
		{Name: "uint64-overflow-number", Status: http.StatusBadRequest, Code: 3, Message: "proto: (line 1:28): invalid value for uint64 field memberID: 18446744073709551616"},
		{Name: "uint64-negative", Status: http.StatusBadRequest, Code: 3, Message: "proto: (line 1:28): invalid value for uint64 field memberID: -1"},
		{Name: "int64-min-string", Status: http.StatusOK, HeaderPresent: true},
		{Name: "int64-min-number", Status: http.StatusOK, HeaderPresent: true},
		{Name: "int64-max-string", Status: http.StatusBadRequest, Code: 11, Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "int64-max-number", Status: http.StatusBadRequest, Code: 11, Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "int64-positive-overflow-string", Status: http.StatusBadRequest, Code: 3, Message: `proto: (line 1:26): invalid value for int64 field revision: "9223372036854775808"`},
		{Name: "int64-positive-overflow-number", Status: http.StatusBadRequest, Code: 3, Message: "proto: (line 1:26): invalid value for int64 field revision: 9223372036854775808"},
		{Name: "int64-negative-overflow-string", Status: http.StatusBadRequest, Code: 3, Message: `proto: (line 1:26): invalid value for int64 field revision: "-9223372036854775809"`},
		{Name: "int64-negative-overflow-number", Status: http.StatusBadRequest, Code: 3, Message: "proto: (line 1:26): invalid value for int64 field revision: -9223372036854775809"},
		{Name: "int64-fraction", Status: http.StatusBadRequest, Code: 3, Message: "proto: (line 1:26): invalid value for int64 field revision: 1.5"},
		{Name: "ttl-max-string", Status: http.StatusBadRequest, Code: 11, Message: "etcdserver: too large lease TTL"},
		{Name: "ttl-max-number", Status: http.StatusBadRequest, Code: 11, Message: "etcdserver: too large lease TTL"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runHTTPGatewayIntegerBoundaryScenario(t, kubebrain))
}

func runHTTPGatewayIntegerBoundaryScenario(t *testing.T, endpoint string) []httpGatewayIntegerBoundaryOutcome {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := strings.TrimRight(endpoint, "/")
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "uint64-max-string", path: "/v3/maintenance/alarm", body: `{"action":"GET","memberID":"18446744073709551615"}`},
		{name: "uint64-max-number", path: "/v3/maintenance/alarm", body: `{"action":"GET","memberID":18446744073709551615}`},
		{name: "uint64-overflow-string", path: "/v3/maintenance/alarm", body: `{"action":"GET","memberID":"18446744073709551616"}`},
		{name: "uint64-overflow-number", path: "/v3/maintenance/alarm", body: `{"action":"GET","memberID":18446744073709551616}`},
		{name: "uint64-negative", path: "/v3/maintenance/alarm", body: `{"action":"GET","memberID":-1}`},
		{name: "int64-min-string", path: "/v3/kv/range", body: `{"key":"Lw==","revision":"-9223372036854775808"}`},
		{name: "int64-min-number", path: "/v3/kv/range", body: `{"key":"Lw==","revision":-9223372036854775808}`},
		{name: "int64-max-string", path: "/v3/kv/range", body: `{"key":"Lw==","revision":"9223372036854775807"}`},
		{name: "int64-max-number", path: "/v3/kv/range", body: `{"key":"Lw==","revision":9223372036854775807}`},
		{name: "int64-positive-overflow-string", path: "/v3/kv/range", body: `{"key":"Lw==","revision":"9223372036854775808"}`},
		{name: "int64-positive-overflow-number", path: "/v3/kv/range", body: `{"key":"Lw==","revision":9223372036854775808}`},
		{name: "int64-negative-overflow-string", path: "/v3/kv/range", body: `{"key":"Lw==","revision":"-9223372036854775809"}`},
		{name: "int64-negative-overflow-number", path: "/v3/kv/range", body: `{"key":"Lw==","revision":-9223372036854775809}`},
		{name: "int64-fraction", path: "/v3/kv/range", body: `{"key":"Lw==","revision":1.5}`},
		{name: "ttl-max-string", path: "/v3/lease/grant", body: `{"ID":"0","TTL":"9223372036854775807"}`},
		{name: "ttl-max-number", path: "/v3/lease/grant", body: `{"ID":0,"TTL":9223372036854775807}`},
	}

	outcomes := make([]httpGatewayIntegerBoundaryOutcome, 0, len(tests))
	for _, test := range tests {
		request, err := http.NewRequest(http.MethodPost, baseURL+test.path, bytes.NewBufferString(test.body))
		require.NoError(t, err, test.name)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, test.name)
		raw, readErr := io.ReadAll(response.Body)
		require.NoError(t, readErr, test.name)
		require.NoError(t, response.Body.Close(), test.name)

		var decoded struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Header  json.RawMessage `json:"header"`
		}
		require.NoError(t, json.Unmarshal(raw, &decoded), "%s: %s", test.name, raw)
		// protojson uses a non-breaking space after "proto:" in decode errors.
		message := strings.ReplaceAll(decoded.Message, "\u00a0", " ")
		outcomes = append(outcomes, httpGatewayIntegerBoundaryOutcome{
			Name: test.name, Status: response.StatusCode, Code: decoded.Code, Message: message,
			HeaderPresent: len(decoded.Header) > 0 && string(decoded.Header) != "null",
		})
	}
	return outcomes
}
