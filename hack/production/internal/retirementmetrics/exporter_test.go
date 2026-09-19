package retirementmetrics

import (
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	kbprom "github.com/kubewharf/kubebrain/pkg/metrics/prometheus"
	"github.com/stretchr/testify/require"
)

func TestProductionExporterClusterBinding(t *testing.T) {
	// The product adapter owns package-global registration. Use a fresh process
	// per iteration instead of mutating its global registry during other tests.
	if os.Getenv("KUBEBRAIN_EXPORTER_BINDING_TEST") != "1" {
		binary, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestProductionExporterClusterBinding$")
		cmd.Env = append(os.Environ(), "KUBEBRAIN_EXPORTER_BINDING_TEST=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, ctx.Err(), string(output))
		require.NoError(t, err, string(output))
		return
	}
	m := kbprom.NewMetrics(metrics.Tag("cluster", "test"))
	for _, stage := range []struct {
		name     string
		outcomes []string
	}{
		{"local", []string{"confirmed", "unconfirmed", "deadline", "canceled"}},
		{"peer", []string{"confirmed", "unconfirmed", "missing_condition", "canceled_before_send"}},
	} {
		for _, outcome := range stage.outcomes {
			require.NoError(t, m.EmitCounter("leader.retirement."+stage.name+".result", 0, metrics.Tag("outcome", outcome)))
		}
	}
	scrape := func() []byte {
		response := httptest.NewRecorder()
		m.GetHttpHandlers()["/metrics"].ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		require.Equal(t, 200, response.Code)
		return response.Body.Bytes()
	}
	raw := scrape()
	before, err := ParseForCluster(raw, "test")
	require.NoError(t, err)
	require.Len(t, before, 8)
	for _, value := range before {
		require.Zero(t, value)
	}
	_, err = Parse(raw)
	require.Error(t, err, "unbound parser must reject production labels")
	_, err = ParseForCluster(raw, "other")
	require.Error(t, err)
	require.NoError(t, m.EmitCounter("leader.retirement.peer.result", 1, metrics.Tag("outcome", "confirmed")))
	after, err := ParseForCluster(scrape(), "test")
	require.NoError(t, err)
	p := Process{PodUID: "pod", ContainerID: "container", StartedAt: "start", Cluster: "test"}
	delta, err := Delta(before, after, Key{"peer", "confirmed"}, p, p)
	require.NoError(t, err)
	require.Equal(t, float64(1), delta)
}

func TestClusterLabelRejection(t *testing.T) {
	valid := `# TYPE leader_retirement_peer_result counter
leader_retirement_peer_result{cluster="test",outcome="confirmed"} 0
`
	for _, raw := range []string{
		strings.ReplaceAll(valid, `cluster="test",`, ""),
		strings.ReplaceAll(valid, `cluster="test"`, `cluster="other"`),
		strings.ReplaceAll(valid, `cluster="test"`, `cluster="test",holder="private"`),
		strings.ReplaceAll(valid, `cluster="test"`, `cluster="test",cluster="test"`),
	} {
		_, err := ParseForCluster([]byte(raw), "test")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private")
	}
}
