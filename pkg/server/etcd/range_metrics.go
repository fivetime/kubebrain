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
