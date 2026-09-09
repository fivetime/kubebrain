package tikv

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	clientmetrics "github.com/tikv/client-go/v2/metrics"
)

// Exercise the actual default gatherer used by /metrics. Observing a histogram
// alone succeeds even when it was never registered, which hid client RPC and
// backoff timings from deployment diagnostics.
func TestClientMetricsExportedByDefaultGatherer(t *testing.T) {
	clientmetrics.TiKVTxnCmdHistogram.WithLabelValues("get", "general").Observe(0.001)
	clientmetrics.TiKVBackoffHistogram.WithLabelValues("tikvRPC").Observe(0.002)
	clientmetrics.TiKVSendReqHistogram.WithLabelValues("Get", "1", "false", "general").Observe(0.003)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	wanted := map[string]bool{
		"tikv_client_go_txn_cmd_duration_seconds": false,
		"tikv_client_go_backoff_seconds":          false,
		"tikv_client_go_request_seconds":          false,
	}
	for _, family := range families {
		if _, ok := wanted[family.GetName()]; !ok {
			continue
		}
		for _, metric := range family.Metric {
			if metric.GetHistogram().GetSampleCount() > 0 {
				wanted[family.GetName()] = true
			}
		}
	}
	for name, observed := range wanted {
		require.True(t, observed, "observed client metric must be exported: %s", name)
	}
}
