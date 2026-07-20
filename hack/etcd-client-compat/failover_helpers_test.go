package compat

import (
	"context"
	"os"
	"os/exec"
)

func waitForKubeBrainRollout(ctx context.Context, namespace string) ([]byte, error) {
	workload := os.Getenv("KUBEBRAIN_FAILOVER_WORKLOAD")
	if workload == "" {
		workload = "statefulset/kubebrain"
	}
	return exec.CommandContext(
		ctx,
		"kubectl", "-n", namespace,
		"rollout", "status", workload, "--timeout=75s",
	).CombinedOutput()
}
