// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import "github.com/kubewharf/kubebrain/pkg/metrics"

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
