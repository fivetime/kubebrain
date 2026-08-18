// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package backend

import "github.com/kubewharf/kubebrain/pkg/metrics"

const (
	watcherSlowConsumerOutcomeCatchUp   = "catch_up"
	watcherSlowConsumerOutcomeRecovered = "recovered"
	watcherSlowConsumerOutcomeDropped   = "dropped"
)

var watcherSlowConsumerOutcomes = []string{
	watcherSlowConsumerOutcomeCatchUp,
	watcherSlowConsumerOutcomeRecovered,
	watcherSlowConsumerOutcomeDropped,
}

func initWatcherSlowConsumerMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, outcome := range watcherSlowConsumerOutcomes {
		_ = metricCli.EmitCounter("watcher_hub.slow_consumer.outcome", int64(0), metrics.Tag("outcome", outcome))
	}
}

func emitWatcherSlowConsumerOutcome(metricCli metrics.Metrics, outcome string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("watcher_hub.slow_consumer.outcome", 1, metrics.Tag("outcome", outcome))
}
