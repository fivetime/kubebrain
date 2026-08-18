// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import "github.com/kubewharf/kubebrain/pkg/metrics"

const (
	countProxyOutcomeHit       = "hit"
	countProxyOutcomeFailure   = "failure"
	countProxyOutcomeQuietSkip = "quiet_skip"
)

var countProxyOutcomes = []string{
	countProxyOutcomeHit,
	countProxyOutcomeFailure,
	countProxyOutcomeQuietSkip,
}

func initCountProxyMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, outcome := range countProxyOutcomes {
		_ = metricCli.EmitCounter("count.proxy.outcome", int64(0), metrics.Tag("outcome", outcome))
	}
}

func emitCountProxyOutcome(metricCli metrics.Metrics, outcome string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("count.proxy.outcome", 1, metrics.Tag("outcome", outcome))
}
