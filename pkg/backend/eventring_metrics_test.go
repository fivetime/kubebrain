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

func TestWatchMetricsInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initWatchMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "watch.event.buffer.stale_drop", value: int64(0)},
		{kind: "counter", name: "watch.event.buffer.full", value: int64(0)},
		{kind: "counter", name: "watch.collector.stalled", value: int64(0)},
		{kind: "counter", name: "watch.collector.skipped_revision", value: int64(0)},
		{kind: "counter", name: "watch.collector.recovery", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "replayed")}},
		{kind: "counter", name: "watch.collector.recovery", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "empty")}},
		{kind: "counter", name: "watch.collector.recovery", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "failed")}},
	}, recorder.records)
}
