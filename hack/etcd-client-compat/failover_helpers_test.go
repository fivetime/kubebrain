package compat

import (
	"context"
	"os"
	"testing"
)

func waitForKubeBrainRollout(t *testing.T, ctx context.Context, namespace string) ([]byte, error) {
	t.Helper()
	workload := os.Getenv("KUBEBRAIN_FAILOVER_WORKLOAD")
	if workload == "" {
		workload = "statefulset/kubebrain"
	}
	return runCompatKubectlContext(
		t, ctx,
		"-n", namespace,
		"rollout", "status", workload, "--timeout=75s",
	)
}
