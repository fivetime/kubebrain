// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import "github.com/kubewharf/kubebrain/pkg/metrics"

const (
	clientAdmissionGuardConcurrency = "concurrency"
	clientAdmissionGuardRate        = "rate"
	clientAdmissionGuardWatch       = "watch"
)

var clientAdmissionGuards = []string{
	clientAdmissionGuardConcurrency,
	clientAdmissionGuardRate,
	clientAdmissionGuardWatch,
}

func initClientAdmissionMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitGauge("grpc.server.admission.inflight", int64(0))
	_ = metricCli.EmitGauge("watch.admission.active", int64(0))
	for _, guard := range clientAdmissionGuards {
		_ = metricCli.EmitCounter("client.admission.rejection", int64(0), metrics.Tag("guard", guard))
	}
}

func emitClientAdmissionRejection(metricCli metrics.Metrics, guard string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("client.admission.rejection", 1, metrics.Tag("guard", guard))
}

func initDeleteRangeAdmissionMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("delete_range.admission.rejected", int64(0))
}

func emitDeleteRangeAdmissionRejected(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("delete_range.admission.rejected", 1)
}
