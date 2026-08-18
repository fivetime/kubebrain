// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import "github.com/kubewharf/kubebrain/pkg/metrics"

const (
	watchGenerationRecoveryRetry     = "retry"
	watchGenerationRecoveryRecovered = "recovered"
	watchGenerationRecoveryCompacted = "compacted"
	watchGenerationRecoveryFailed    = "failed"
)

var watchGenerationRecoveryOutcomes = []string{
	watchGenerationRecoveryRetry,
	watchGenerationRecoveryRecovered,
	watchGenerationRecoveryCompacted,
	watchGenerationRecoveryFailed,
}

func initWatchGenerationRecoveryMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, outcome := range watchGenerationRecoveryOutcomes {
		_ = metricCli.EmitCounter("watch.generation.recovery", int64(0), metrics.Tag("outcome", outcome))
	}
}

func emitWatchGenerationRecovery(metricCli metrics.Metrics, outcome string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("watch.generation.recovery", 1, metrics.Tag("outcome", outcome))
}
