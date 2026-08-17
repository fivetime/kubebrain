package compat

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestHTTPHealthSurvivesCombinedBackendFault verifies that member liveness and
// serializable health remain observable from a protected local checkpoint while
// readiness still fails closed when its linearizable read barrier is unavailable.
func TestHTTPHealthSurvivesCombinedBackendFault(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_HTTP_HEALTH_COMBINED_FAULT_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_HTTP_HEALTH_COMBINED_FAULT_COMMAND to run destructive combined backend fault")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for combined backend fault HTTP health")
	}
	infoEndpoint := os.Getenv("KUBEBRAIN_INFO_ENDPOINT")
	if infoEndpoint == "" {
		t.Fatal("set KUBEBRAIN_INFO_ENDPOINT explicitly for checkpoint fallback metrics")
	}
	checkpointWait := 90 * time.Second
	if configured := os.Getenv("KUBEBRAIN_HTTP_HEALTH_CHECKPOINT_WAIT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		require.NoError(t, err)
		require.Positive(t, parsed)
		checkpointWait = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkpointWait+2*time.Minute)
	defer cancel()
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, etcdClient.Close()) })
	key := fmt.Sprintf("/compat/http-health-combined/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		assert.Eventually(t, func() bool {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanupCancel()
			_, cleanupErr := etcdClient.Delete(cleanupCtx, key)
			return cleanupErr == nil
		}, 30*time.Second, 200*time.Millisecond)
	})
	_, err = etcdClient.Put(ctx, key, "checkpoint fixture")
	require.NoError(t, err)
	select {
	case <-time.After(checkpointWait):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	httpClient := &http.Client{Timeout: 8 * time.Second}
	httpEndpoint := "http://" + strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	infoHTTPEndpoint := "http://" + strings.TrimPrefix(strings.TrimPrefix(infoEndpoint, "http://"), "https://")
	requestAt := func(baseURL, path string) (int, string, error) {
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if requestErr != nil {
			return 0, "", requestErr
		}
		response, requestErr := httpClient.Do(req)
		if requestErr != nil {
			return 0, "", requestErr
		}
		defer response.Body.Close()
		body, requestErr := io.ReadAll(response.Body)
		return response.StatusCode, string(body), requestErr
	}
	request := func(path string) (int, string, error) { return requestAt(httpEndpoint, path) }
	baselineVersionStatus, baselineVersion, err := request("/version")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, baselineVersionStatus)
	baselineMetricsStatus, baselineMetrics, err := requestAt(infoHTTPEndpoint, "/metrics")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, baselineMetricsStatus)
	for _, check := range []string{"alarm", "serializable_read", "data_corruption"} {
		_, present := prometheusCounterSample(baselineMetrics, "health_checkpoint_fallback", "check", check)
		require.Truef(t, present, "%s fallback metric must be initialized before the first fallback", check)
	}

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	readyPath := filepath.Join(t.TempDir(), "combined-fault-ready")
	t.Setenv("KUBEBRAIN_COMBINED_FAULT_READY_FILE", readyPath)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(readyPath)
		return statErr == nil
	}, 3*time.Minute, 100*time.Millisecond, "combined backend fault must signal both quorums ready")
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer callCancel()
		_, putErr := etcdClient.Put(callCtx, key, fmt.Sprintf("%d", time.Now().UnixNano()))
		return putErr != nil && isMutationFailoverAmbiguous(putErr)
	}, 15*time.Second, 25*time.Millisecond, "combined fault must expose a mutation-unavailable window")

	versionStatus, versionBody, versionErr := request("/version")
	healthCallCtx, healthCallCancel := context.WithTimeout(ctx, time.Second)
	grpcHealth, grpcHealthErr := healthpb.NewHealthClient(etcdClient.ActiveConnection()).Check(
		healthCallCtx, &healthpb.HealthCheckRequest{},
	)
	healthCallCancel()
	healthListCtx, healthListCancel := context.WithTimeout(ctx, time.Second)
	grpcHealthList, grpcHealthListErr := healthpb.NewHealthClient(etcdClient.ActiveConnection()).List(
		healthListCtx, &healthpb.HealthListRequest{},
	)
	healthListCancel()
	healthStatus, healthBody, healthErr := request("/health?serializable=true")
	livezStatus, livezBody, livezErr := request("/livez?verbose")
	readyzStatus, readyzBody, readyzErr := request("/readyz?verbose")
	metricsStatus, metricsBody, metricsErr := requestAt(infoHTTPEndpoint, "/metrics")
	result := <-commandDone
	require.NoErrorf(t, result.err, "combined backend fault command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("combined backend fault command: %s", strings.TrimSpace(string(result.output)))

	require.NoError(t, versionErr)
	require.Equal(t, http.StatusOK, versionStatus)
	require.JSONEq(t, baselineVersion, versionBody)
	require.NoError(t, grpcHealthErr)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, grpcHealth.GetStatus())
	require.NoError(t, grpcHealthListErr)
	require.Equal(t, map[string]*healthpb.HealthCheckResponse{
		"": {Status: healthpb.HealthCheckResponse_SERVING},
	}, grpcHealthList.GetStatuses())
	require.NoError(t, healthErr)
	require.Equal(t, http.StatusOK, healthStatus)
	require.JSONEq(t, `{"health":"true","reason":""}`, healthBody)
	require.NoError(t, livezErr)
	require.Equal(t, http.StatusOK, livezStatus)
	require.Equal(t, "[+]serializable_read ok\nok\n", livezBody)
	require.NoError(t, readyzErr)
	require.Equal(t, http.StatusServiceUnavailable, readyzStatus)
	require.Contains(t, readyzBody, "[+]data_corruption ok\n")
	require.Contains(t, readyzBody, "[+]serializable_read ok\n")
	require.Contains(t, readyzBody, "[-]linearizable_read failed:")
	require.NoError(t, metricsErr)
	require.Equal(t, http.StatusOK, metricsStatus)
	for check, minimumDelta := range map[string]float64{
		"alarm": 1, "serializable_read": 3, "data_corruption": 1,
	} {
		before := prometheusCounterValue(baselineMetrics, "health_checkpoint_fallback", "check", check)
		after := prometheusCounterValue(metricsBody, "health_checkpoint_fallback", "check", check)
		require.GreaterOrEqualf(t, after-before, minimumDelta, "%s fallback metric delta", check)
	}
}

func prometheusCounterValue(text, name, label, value string) float64 {
	parsed, _ := prometheusCounterSample(text, name, label, value)
	return parsed
}

func prometheusCounterSample(text, name, label, value string) (float64, bool) {
	needle := label + "=\"" + value + "\""
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+"{") || !strings.Contains(line, needle) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		parsed, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func TestPrometheusCounterValue(t *testing.T) {
	metricsText := "# TYPE health_checkpoint_fallback counter\n" +
		"health_checkpoint_fallback{check=\"alarm\",cluster=\"kb\"} 2\n" +
		"health_checkpoint_fallback{check=\"serializable_read\",cluster=\"kb\"} 3.5\n"
	require.Equal(t, 2.0, prometheusCounterValue(metricsText, "health_checkpoint_fallback", "check", "alarm"))
	require.Equal(t, 3.5, prometheusCounterValue(metricsText, "health_checkpoint_fallback", "check", "serializable_read"))
	require.Zero(t, prometheusCounterValue(metricsText, "health_checkpoint_fallback", "check", "data_corruption"))
	_, present := prometheusCounterSample(metricsText, "health_checkpoint_fallback", "check", "alarm")
	require.True(t, present)
	_, present = prometheusCounterSample(metricsText, "health_checkpoint_fallback", "check", "data_corruption")
	require.False(t, present)
}
