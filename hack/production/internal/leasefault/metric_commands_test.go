package leasefault

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func metricTargets(t *testing.T, p ObservationCommandPlan) []MetricCommandTarget {
	t.Helper()
	var targets []MetricCommandTarget
	for i, e := range p.Bindings.Metrics {
		f, err := os.CreateTemp(p.OwnerDirectory, "metric-*.log")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		targets = append(targets, MetricCommandTarget{PodName: p.Bindings.Network.PodName, PodUID: e.Binding.PodUID, InfoPort: 18600 + 2*i, AnonymousPort: 18601 + 2*i, Stderr: f})
	}
	return targets
}

func TestMetricCommands(t *testing.T) {
	p := commandPlan(t)
	p.Bindings.Metrics = append(p.Bindings.Metrics, p.Bindings.Metrics[0])
	p.Bindings.Metrics[1].Offset = 29*time.Second + 999999999*time.Nanosecond
	targets := metricTargets(t, p)
	c, err := p.MetricCommands("/approved/protected-metrics-worker.sh", targets)
	require.NoError(t, err)
	require.Len(t, c, 2)
	require.Equal(t, []string{p.Bindings.Network.PodName, "0"}, c[0].Args)
	require.Equal(t, []string{p.Bindings.Network.PodName, "29999999999"}, c[1].Args)
	require.Contains(t, c[0].Env, "stack_info_port=18600")
	require.Contains(t, c[1].Env, "stack_anonymous_port=18603")
	p.Env[0] = "PATH=/changed"
	c[0].Env[0] = "PATH=/changed-again"
	require.Equal(t, "PATH=/approved/bin", c[1].Env[0])
	require.Equal(t, targets[1].Stderr, c[1].Stderr)
}

func TestMetricCommandsReject(t *testing.T) {
	for _, mode := range []string{"count", "uid", "name", "original-uid-other-name", "original-name-other-uid", "same-name-other-uid", "same-uid-other-name", "privileged", "range", "same-ports", "stack-port", "worker-port", "no-log", "public-log", "shared-log", "stack-log", "relative-executable", "invalid-offset"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			p.Bindings.Metrics = append(p.Bindings.Metrics, p.Bindings.Metrics[0])
			targets := metricTargets(t, p)
			executable := "/approved/protected-metrics-worker.sh"
			switch mode {
			case "count":
				targets = targets[:1]
			case "uid":
				targets[0].PodUID = "different"
			case "name":
				targets[0].PodName = "--help"
			case "original-uid-other-name":
				targets[0].PodName = "other-pod"
			case "original-name-other-uid":
				p.Bindings.Metrics[0].Binding.PodUID = "other-uid"
				targets[0].PodUID = "other-uid"
			case "same-name-other-uid", "same-uid-other-name":
				for i := range targets {
					p.Bindings.Metrics[i].Binding.PodUID = "peer-uid"
					targets[i].PodUID, targets[i].PodName = "peer-uid", "peer-pod"
				}
				if mode == "same-name-other-uid" {
					p.Bindings.Metrics[1].Binding.PodUID = "third-uid"
					targets[1].PodUID = "third-uid"
				} else {
					targets[1].PodName = "third-pod"
				}
			case "privileged":
				targets[0].InfoPort = 80
			case "range":
				targets[0].InfoPort = 65536
			case "same-ports":
				targets[0].InfoPort = targets[0].AnonymousPort
			case "stack-port":
				targets[0].InfoPort = p.Bindings.StackPorts[0]
			case "worker-port":
				targets[1].InfoPort = targets[0].InfoPort
			case "no-log":
				targets[0].Stderr = nil
			case "public-log":
				require.NoError(t, targets[0].Stderr.Chmod(0644))
			case "shared-log":
				targets[1].Stderr = targets[0].Stderr
			case "stack-log":
				targets[0].Stderr = p.BeforeLog
			case "relative-executable":
				executable = filepath.Base(executable)
			case "invalid-offset":
				p.Bindings.Metrics[0].Offset = 30 * time.Second
			}
			c, err := p.MetricCommands(executable, targets)
			require.Error(t, err)
			require.Nil(t, c)
		})
	}
}

func TestMetricCommandsDistinctPodsAndRepeatedSamples(t *testing.T) {
	p := commandPlan(t)
	p.Bindings.Metrics = append(p.Bindings.Metrics, p.Bindings.Metrics[0], p.Bindings.Metrics[0])
	for i := 1; i < 3; i++ {
		p.Bindings.Metrics[i].Binding.PodUID = "peer-uid"
		p.Bindings.Metrics[i].Offset = time.Duration(i) * time.Second
	}
	targets := metricTargets(t, p)
	targets[1].PodName, targets[2].PodName = "peer-pod", "peer-pod"
	commands, err := p.MetricCommands("/approved/protected-metrics-worker.sh", targets)
	require.NoError(t, err)
	require.Len(t, commands, 3)
	require.Equal(t, []string{p.Bindings.Network.PodName, "0"}, commands[0].Args)
	require.Equal(t, []string{"peer-pod", "1000000000"}, commands[1].Args)
	require.Equal(t, []string{"peer-pod", "2000000000"}, commands[2].Args)
}
