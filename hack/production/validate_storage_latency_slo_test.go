package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageLatencyGateMatchesProductionAlerts(t *testing.T) {
	scriptBytes, err := os.ReadFile("validate-storage-latency-slo.sh")
	require.NoError(t, err)
	monitoringBytes, err := os.ReadFile(filepath.Join("..", "..", "deploy", "production", "monitoring.yaml"))
	require.NoError(t, err)
	script, monitoring := string(scriptBytes), string(monitoringBytes)
	for _, metric := range []string{
		"etcd_disk_wal_fsync_duration_seconds",
		"tikv_raftstore_store_write_raftdb_duration_seconds",
		"tikv_raftstore_store_write_kvdb_duration_seconds",
	} {
		require.Contains(t, script, metric+"_bucket")
		require.Contains(t, script, metric+"_count")
		require.Contains(t, monitoring, metric+"_bucket")
		require.Contains(t, monitoring, metric+"_count")
		expression := `histogram_quantile(0.99, sum by (instance, le) (rate(` + metric +
			`_bucket{namespace="tidb-cluster",service="kb-` + map[string]string{
			"etcd_disk_wal_fsync_duration_seconds":               "pd",
			"tikv_raftstore_store_write_raftdb_duration_seconds": "tikv",
			"tikv_raftstore_store_write_kvdb_duration_seconds":   "tikv",
		}[metric] + `-metrics"}[5m])))`
		require.Contains(t, monitoring, expression)
	}
	require.Contains(t, script, `TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"`)
	require.Contains(t, script, `TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"`)
	require.Equal(t, 3, strings.Count(script, "histogram_quantile(0.99"))
	require.Equal(t, 3, strings.Count(script, "[5m]"))
	require.Equal(t, 3, strings.Count(script, "validate_count "))
}

func TestValidateStorageLatencySLOFailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	curl := filepath.Join(tempDir, "curl")
	require.NoError(t, os.WriteFile(curl, []byte(`#!/usr/bin/env bash
set -euo pipefail
query=''
while (($#)); do
  if [[ "$1" == --data-urlencode ]]; then query="${2#query=}"; shift 2; else shift; fi
done
[[ -z "${FAKE_QUERY_LOG:-}" ]] || printf '%s\n' "$query" >>"$FAKE_QUERY_LOG"
if [[ "${FAKE_PROM_ERROR:-false}" == true ]]; then
  printf '{"status":"error","error":"backend unavailable"}\n'
  exit 0
fi
if [[ "$query" == count\(* ]]; then
  value="${FAKE_SERIES_COUNT:-3}"
  printf '{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"%s"]}]}}\n' "$value"
else
  value="${FAKE_LATENCY:-0.125}"
  results="${FAKE_LATENCY_RESULTS:-3}"
  printf '{"status":"success","data":{"resultType":"vector","result":['
  for ((i=0; i<results; i++)); do
    ((i == 0)) || printf ','
    printf '{"metric":{"instance":"storage-%d"},"value":[1,"%s"]}' "$i" "$value"
  done
  printf ']}}\n'
fi
`), 0o755))
	queryLog := filepath.Join(tempDir, "queries.log")
	base := []string{
		"PROMETHEUS_URL=https://prometheus.example.test",
		"CURL=" + curl,
		"JQ=jq",
		"TIDB_NAMESPACE=storage-a",
		"TIDB_CLUSTER=tenant-a",
		"FAKE_QUERY_LOG=" + queryLog,
		"EXPECTED_PD_MEMBERS=3",
		"EXPECTED_TIKV_STORES=3",
		"MAX_STORAGE_P99_SECONDS=1",
	}

	output, err := runProductionScriptCommand(t, "validate-storage-latency-slo.sh", base)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "storage latency SLO gate passed")
	queries, err := os.ReadFile(queryLog)
	require.NoError(t, err)
	require.Equal(t, 6, strings.Count(string(queries), "\n"))
	require.Equal(t, 2, strings.Count(string(queries), `namespace="storage-a",service="tenant-a-pd-metrics"`))
	require.Equal(t, 4, strings.Count(string(queries), `namespace="storage-a",service="tenant-a-tikv-metrics"`))

	for _, tc := range []struct {
		name string
		env  string
		want string
	}{
		{name: "latency exceeds SLO", env: "FAKE_LATENCY=1.25", want: "exceeds 1s"},
		{name: "latency is NaN", env: "FAKE_LATENCY=NaN", want: "malformed or exceeds"},
		{name: "latency replica missing", env: "FAKE_LATENCY_RESULTS=2", want: "expected 3 series"},
		{name: "latency replica duplicated", env: "FAKE_LATENCY_RESULTS=4", want: "expected 3 series"},
		{name: "count family missing", env: "FAKE_SERIES_COUNT=2", want: "expected 3 series"},
		{name: "prometheus error", env: "FAKE_PROM_ERROR=true", want: "Prometheus query failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failedOutput, failedErr := runProductionScriptCommand(t, "validate-storage-latency-slo.sh", append(base, tc.env))
			require.Error(t, failedErr, string(failedOutput))
			require.Contains(t, string(failedOutput), tc.want)
		})
	}
}

func TestValidateStorageLatencySLORejectsUnsafeConfiguration(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-storage-latency-slo.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "PROMETHEUS_URL is required")

	output, err = runProductionScriptCommand(t, "validate-storage-latency-slo.sh", []string{
		"PROMETHEUS_URL=http://prometheus.example.test",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "must use https")

	output, err = runProductionScriptCommand(t, "validate-storage-latency-slo.sh", []string{
		"PROMETHEUS_URL=https://prometheus.example.test",
		"MAX_STORAGE_P99_SECONDS=0",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "must be greater than zero")

	output, err = runProductionScriptCommand(t, "validate-storage-latency-slo.sh", []string{
		"PROMETHEUS_URL=https://prometheus.example.test",
		"TIDB_NAMESPACE=tenant/a",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TIDB_NAMESPACE must be a DNS label")

	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("first\nsecond\n"), 0o600))
	output, err = runProductionScriptCommand(t, "validate-storage-latency-slo.sh", []string{
		"PROMETHEUS_URL=https://prometheus.example.test",
		"PROMETHEUS_BEARER_TOKEN_FILE=" + tokenPath,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "one non-empty header-safe token")
}
