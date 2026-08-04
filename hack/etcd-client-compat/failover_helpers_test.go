package compat

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func waitForKubeBrainRollout(t *testing.T, ctx context.Context, namespace string) ([]byte, error) {
	t.Helper()
	workload := os.Getenv("KUBEBRAIN_FAILOVER_WORKLOAD")
	if workload == "" {
		workload = "statefulset/kubebrain"
	}
	args := kubeBrainRolloutArgs(namespace, workload, os.Getenv("KUBEBRAIN_FAILOVER_CONTEXT"))
	return runCompatKubectlContext(t, ctx, args...)
}

func kubeBrainRolloutArgs(namespace, workload, kubeContext string) []string {
	args := make([]string, 0, 9)
	if kubeContext != "" {
		args = append(args, "--context", kubeContext)
	}
	args = append(args,
		"-n", namespace,
		"rollout", "status", workload, "--timeout=75s",
	)
	return args
}

func TestKubeBrainRolloutArgsUsesExplicitContext(t *testing.T) {
	require.Equal(t, []string{
		"--context", "kind-kubebrain-dbaas", "-n", "kubebrain-dev",
		"rollout", "status", "statefulset/kubebrain", "--timeout=75s",
	}, kubeBrainRolloutArgs("kubebrain-dev", "statefulset/kubebrain", "kind-kubebrain-dbaas"))
}
