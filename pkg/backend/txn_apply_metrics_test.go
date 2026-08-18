// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package backend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func TestUncertainTxnMetricsInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initUncertainTxnMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "txn.uncertain.resolution", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "retry")}},
		{kind: "counter", name: "txn.uncertain.resolution", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "committed")}},
		{kind: "counter", name: "txn.uncertain.resolution", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "not_committed")}},
		{kind: "counter", name: "txn.uncertain.resolution", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "witness_corrupt")}},
		{kind: "counter", name: "txn.uncertain.resolution", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "corrupt_alarm_failed")}},
	}, recorder.records)
}

func TestReadIntegrityFenceMetricsInitializeFixedTargetsAndOutcomes(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initReadIntegrityFenceMetrics(recorder)
	emitReadIntegrityFence(recorder, "object", "armed")
	emitReadIntegrityFence(recorder, "revision_index", "failed")

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "read.integrity.fence", value: int64(0), tags: []metrics.T{metrics.Tag("target", "object"), metrics.Tag("outcome", "armed")}},
		{kind: "counter", name: "read.integrity.fence", value: int64(0), tags: []metrics.T{metrics.Tag("target", "object"), metrics.Tag("outcome", "failed")}},
		{kind: "counter", name: "read.integrity.fence", value: int64(0), tags: []metrics.T{metrics.Tag("target", "revision_index"), metrics.Tag("outcome", "armed")}},
		{kind: "counter", name: "read.integrity.fence", value: int64(0), tags: []metrics.T{metrics.Tag("target", "revision_index"), metrics.Tag("outcome", "failed")}},
		{kind: "counter", name: "read.integrity.fence", value: 1, tags: []metrics.T{metrics.Tag("target", "object"), metrics.Tag("outcome", "armed")}},
		{kind: "counter", name: "read.integrity.fence", value: 1, tags: []metrics.T{metrics.Tag("target", "revision_index"), metrics.Tag("outcome", "failed")}},
	}, recorder.records)
}
