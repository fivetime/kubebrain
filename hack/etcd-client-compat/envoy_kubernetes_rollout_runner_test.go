package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvoyKubernetesRolloutRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-envoy-kubernetes-rollout.sh")
	require.NoError(t, err)
	text := string(script)
	for _, required := range []string{
		"set KUBE_CONTEXT explicitly",
		"exactly three Ready Envoy Pods",
		"three distinct Kubernetes nodes",
		"exactly three ready EndpointSlice targets",
		"create service nodeport",
		"NODE_PORT must be empty or a Kubernetes NodePort",
		"NODE_ENDPOINT_PORT must be empty or a TCP port",
		"kubebrain-envoy-rollout-gate",
		"KUBEBRAIN_ENVOY_ROLLOUT_ENDPOINT",
		"TestEnvoyKubernetesRollout",
		"trap cleanup EXIT",
	} {
		require.Contains(t, text, required)
	}
}

func TestEnvoyKubernetesRolloutRunnerRejectsImplicitContext(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-envoy-kubernetes-rollout.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBE_CONTEXT explicitly")
	require.NotContains(t, string(output), "missing required command")
}
