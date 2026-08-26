// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import (
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const etcdRangeDurationMetric = "etcd.server.range_duration_seconds"

const (
	rangeStreamFailureBackend  = "backend"
	rangeStreamFailureSend     = "send"
	rangeStreamFailureProtocol = "protocol"
)

var rangeStreamFailureStages = []string{
	rangeStreamFailureBackend,
	rangeStreamFailureSend,
	rangeStreamFailureProtocol,
}

func initRangeStreamFailureMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, stage := range rangeStreamFailureStages {
		_ = metricCli.EmitCounter("read.range_stream.failure", int64(0), metrics.Tag("stage", stage))
	}
	_ = metricCli.EmitCounter("read.range_stream.proxy_retry", int64(0))
}

func emitRangeStreamFailure(metricCli metrics.Metrics, stage string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("read.range_stream.failure", 1, metrics.Tag("stage", stage))
}

// emitEtcdRangeDuration mirrors server/etcdserver/txn.RangeSecObserve. The
// caller brackets only the MVCC range operation, not RPC admission, auth,
// linearizable-read synchronization, or response transmission.
func emitEtcdRangeDuration(metricCli metrics.Metrics, duration time.Duration, err error) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram(
		etcdRangeDurationMetric,
		duration.Seconds(),
		metrics.Tag("success", strconv.FormatBool(err == nil)),
	)
}
