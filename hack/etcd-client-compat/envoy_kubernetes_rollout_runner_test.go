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
		"ROLLOUT_CYCLES must be an integer in [1,10]",
		`ROLLOUT_CYCLES="${ROLLOUT_CYCLES:-3}"`,
		"KUBEBRAIN_ENVOY_ROLLOUT_CYCLES",
		"TLS_CA_FILE, TLS_CERT_FILE, TLS_KEY_FILE, and TLS_SERVER_NAME must all be set for TLS passthrough",
		"KUBEBRAIN_ENVOY_ROLLOUT_TLS",
		"TLS_ROTATION_COMMAND requires all TLS passthrough inputs",
		"TLS_ROTATION_COMMAND must name an executable regular file",
		"KUBEBRAIN_ENVOY_ROLLOUT_TLS_ROTATION_COMMAND",
		"TLS_ROTATED_CA_FILE, TLS_ROTATED_CERT_FILE, TLS_ROTATED_KEY_FILE, TLS_ROTATED_SERVER_NAME, and TLS_RETIRED_CA_FILE must all be set",
		"rotated TLS inputs require TLS_ROTATION_COMMAND",
		"KUBERNETES_ENVOY_ROTATED_TLS_CA_FILE",
		"KUBERNETES_ENVOY_RETIRED_TLS_CA_FILE",
		"KUBERNETES_ENVOY_ROLLOUT_TLS_SERVER_NAME",
		"kubebrain-envoy-rollout-gate",
		"KUBEBRAIN_ENVOY_ROLLOUT_ENDPOINT",
		"TestEnvoyKubernetesRollout",
		"trap cleanup EXIT",
	} {
		require.Contains(t, text, required)
	}
}

func TestEnvoyKubernetesRolloutRunnerRejectsRotationWithoutTLS(t *testing.T) {
	command := t.TempDir() + "/rotate"
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\nexit 0\n"), 0o700))
	output, err := runCompatScriptCommand(t, "run-envoy-kubernetes-rollout.sh", []string{
		"KUBE_CONTEXT=explicit-test-context",
		"TLS_ROTATION_COMMAND=" + command,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TLS_ROTATION_COMMAND requires all TLS passthrough inputs")
	require.NotContains(t, string(output), "missing required command")
}

func TestEnvoyKubernetesRolloutRunnerRejectsNonExecutableRotation(t *testing.T) {
	directory := t.TempDir()
	command := directory + "/rotate"
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\nexit 0\n"), 0o600))
	files := make([]string, 3)
	for index, name := range []string{"ca", "cert", "key"} {
		files[index] = directory + "/" + name
		require.NoError(t, os.WriteFile(files[index], []byte(name), 0o600))
	}
	output, err := runCompatScriptCommand(t, "run-envoy-kubernetes-rollout.sh", []string{
		"KUBE_CONTEXT=explicit-test-context",
		"TLS_CA_FILE=" + files[0], "TLS_CERT_FILE=" + files[1], "TLS_KEY_FILE=" + files[2],
		"TLS_SERVER_NAME=instance.example", "TLS_ROTATION_COMMAND=" + command,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TLS_ROTATION_COMMAND must name an executable regular file")
	require.NotContains(t, string(output), "missing required command")
}

func TestEnvoyKubernetesRolloutRunnerRejectsPartialRotatedTLS(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-envoy-kubernetes-rollout.sh", []string{
		"KUBE_CONTEXT=explicit-test-context", "TLS_ROTATED_CA_FILE=/missing/next-ca.crt",
	})
	require.Error(t, err)
	require.Contains(t, string(output),
		"TLS_ROTATED_CA_FILE, TLS_ROTATED_CERT_FILE, TLS_ROTATED_KEY_FILE, TLS_ROTATED_SERVER_NAME, and TLS_RETIRED_CA_FILE must all be set")
	require.NotContains(t, string(output), "missing required command")
}

func TestEnvoyKubernetesRolloutRunnerRejectsImplicitContext(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-envoy-kubernetes-rollout.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBE_CONTEXT explicitly")
	require.NotContains(t, string(output), "missing required command")
}

func TestEnvoyKubernetesRolloutRunnerRejectsPartialTLS(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-envoy-kubernetes-rollout.sh", []string{
		"KUBE_CONTEXT=explicit-test-context",
		"TLS_CA_FILE=/missing/ca.crt",
	})
	require.Error(t, err)
	require.Contains(t, string(output),
		"TLS_CA_FILE, TLS_CERT_FILE, TLS_KEY_FILE, and TLS_SERVER_NAME must all be set for TLS passthrough")
	require.NotContains(t, string(output), "missing required command")
}
