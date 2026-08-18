// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package backend

import "github.com/kubewharf/kubebrain/pkg/metrics"

const (
	orphanIndexHealOutcomeHealed     = "healed"
	orphanIndexHealOutcomeConcurrent = "concurrent"
	orphanIndexHealOutcomeFenced     = "fenced"
	orphanIndexHealOutcomeFailed     = "failed"
)

var orphanIndexHealOutcomes = []string{
	orphanIndexHealOutcomeHealed,
	orphanIndexHealOutcomeConcurrent,
	orphanIndexHealOutcomeFenced,
	orphanIndexHealOutcomeFailed,
}

func initOrphanIndexHealMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, outcome := range orphanIndexHealOutcomes {
		_ = metricCli.EmitCounter("backend.orphan_index.heal_outcome", int64(0), metrics.Tag("outcome", outcome))
	}
}

func emitOrphanIndexHealOutcome(metricCli metrics.Metrics, outcome string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("backend.orphan_index.heal_outcome", 1, metrics.Tag("outcome", outcome))
}
