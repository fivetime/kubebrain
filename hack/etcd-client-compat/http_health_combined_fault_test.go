package compat

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	request := func(path string) (int, string, error) {
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, httpEndpoint+path, nil)
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
	baselineVersionStatus, baselineVersion, err := request("/version")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, baselineVersionStatus)

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
	healthStatus, healthBody, healthErr := request("/health?serializable=true")
	livezStatus, livezBody, livezErr := request("/livez?verbose")
	readyzStatus, readyzBody, readyzErr := request("/readyz?verbose")
	result := <-commandDone
	require.NoErrorf(t, result.err, "combined backend fault command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("combined backend fault command: %s", strings.TrimSpace(string(result.output)))

	require.NoError(t, versionErr)
	require.Equal(t, http.StatusOK, versionStatus)
	require.JSONEq(t, baselineVersion, versionBody)
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
}
