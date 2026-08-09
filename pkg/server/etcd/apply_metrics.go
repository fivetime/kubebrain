// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import (
	"errors"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const etcdApplyDurationMetric = "etcd.server.apply_duration_seconds"

// emitEtcdApplyDuration mirrors server/etcdserver/txn.ApplySecObserve. Callers
// bracket the local leader's state-machine operation, never follower proxying,
// request validation, or malformed/invalid authentication metadata rejected
// before upstream builds an InternalRaftRequest. Authorization performed by an
// apply wrapper is deliberately inside this boundary.
func emitEtcdApplyDuration(metricCli metrics.Metrics, op string, duration time.Duration, err error) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram(
		etcdApplyDurationMetric,
		duration.Seconds(),
		metrics.Tag("version", "v3"),
		metrics.Tag("op", op),
		metrics.Tag("success", strconv.FormatBool(etcdApplySucceeded(err))),
	)
}

// Upstream deliberately classifies mvcc.ErrCompacted as a successful apply:
// the state machine applied the request and returned a semantic read result.
func etcdApplySucceeded(err error) bool {
	if err == nil || errors.Is(err, rpctypes.ErrCompacted) {
		return true
	}
	want := status.Convert(rpctypes.ErrGRPCCompacted)
	got := status.Convert(err)
	return got.Code() == want.Code() && got.Message() == want.Message()
}

// beginEtcdApply is used after request validation, leader routing and auth-info
// admission. The returned closure observes the final local apply result.
func beginEtcdApply(metricCli metrics.Metrics, op string, retErr *error) func() {
	start := time.Now()
	return func() {
		emitEtcdApplyDuration(metricCli, op, time.Since(start), *retErr)
	}
}
