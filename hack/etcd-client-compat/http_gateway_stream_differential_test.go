package compat

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type httpGatewayStreamOutcome struct {
	WatchContentType       string
	WatchChunked           bool
	WatchCreated           bool
	WatchEventKey          string
	WatchEventValue        string
	WatchPreviousValue     string
	ObserveContentType     string
	ObserveChunked         bool
	ObserveInitialValue    string
	ObserveProclaimedValue string
}

func TestHTTPGatewayStreamsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcome := runHTTPGatewayStreamScenario(t, reference, "reference", "8563571")
	kubebrainOutcome := runHTTPGatewayStreamScenario(t, kubebrain, "kubebrain", "8563572")
	require.Equal(t, referenceOutcome, kubebrainOutcome)
}

func runHTTPGatewayStreamScenario(t *testing.T, endpoint, instance, leaseID string) httpGatewayStreamOutcome {
	t.Helper()
	client := &http.Client{Timeout: 15 * time.Second}
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
	openStream := func(path string, body any) (*http.Response, *bufio.Reader) {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(raw))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err, path)
		require.Equal(t, http.StatusOK, response.StatusCode)
		return response, bufio.NewReader(response.Body)
	}
	readResult := func(reader *bufio.Reader) map[string]any {
		t.Helper()
		line, err := reader.ReadBytes('\n')
		require.NoError(t, err)
		chunk := make(map[string]any)
		require.NoError(t, json.Unmarshal(line, &chunk), "%s", line)
		return requireMapField(t, chunk, "result")
	}
	stringField := func(value map[string]any, field string) string {
		t.Helper()
		fieldValue, ok := value[field].(string)
		require.True(t, ok, "missing string field %q in %#v", field, value)
		return fieldValue
	}

	key := "/a357/http-stream/" + instance
	post("/v3/kv/deleterange", map[string]any{"key": encode(key)})
	post("/v3/kv/put", map[string]any{"key": encode(key), "value": encode("before")})
	watchResponse, watchReader := openStream("/v3/watch", map[string]any{
		"create_request": map[string]any{"key": encode(key), "prev_kv": true},
	})
	created := readResult(watchReader)
	post("/v3/kv/put", map[string]any{"key": encode(key), "value": encode("after")})
	eventResponse := readResult(watchReader)
	require.NoError(t, watchResponse.Body.Close())
	events := requireSliceField(t, eventResponse, "events")
	require.Len(t, events, 1)
	event := events[0].(map[string]any)
	kv := requireMapField(t, event, "kv")
	previous := requireMapField(t, event, "prev_kv")

	name := "/a357/http-observe/" + instance
	post("/v3/lease/revoke", map[string]any{"ID": leaseID})
	post("/v3/lease/grant", map[string]any{"ID": leaseID, "TTL": "30"})
	t.Cleanup(func() { post("/v3/lease/revoke", map[string]any{"ID": leaseID}) })
	campaign := post("/v3/election/campaign", map[string]any{
		"name": encode(name), "lease": leaseID, "value": encode("leader-one"),
	})
	leader := requireMapField(t, campaign, "leader")
	observeResponse, observeReader := openStream("/v3/election/observe", map[string]any{"name": encode(name)})
	initial := requireMapField(t, readResult(observeReader), "kv")
	post("/v3/election/proclaim", map[string]any{"leader": leader, "value": encode("leader-two")})
	proclaimed := requireMapField(t, readResult(observeReader), "kv")
	require.NoError(t, observeResponse.Body.Close())
	post("/v3/election/resign", map[string]any{"leader": leader})

	return httpGatewayStreamOutcome{
		WatchContentType:       watchResponse.Header.Get("Content-Type"),
		WatchChunked:           containsString(watchResponse.TransferEncoding, "chunked"),
		WatchCreated:           created["created"] == true,
		WatchEventKey:          stringField(kv, "key"),
		WatchEventValue:        stringField(kv, "value"),
		WatchPreviousValue:     stringField(previous, "value"),
		ObserveContentType:     observeResponse.Header.Get("Content-Type"),
		ObserveChunked:         containsString(observeResponse.TransferEncoding, "chunked"),
		ObserveInitialValue:    stringField(initial, "value"),
		ObserveProclaimedValue: stringField(proclaimed, "value"),
	}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
