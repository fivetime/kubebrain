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

func TestEventLogIntegrityMetricsInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initEventLogIntegrityMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "watch.event_log.corruption", value: int64(0), tags: []metrics.T{metrics.Tag("kind", "malformed")}},
		{kind: "counter", name: "watch.event_log.corruption", value: int64(0), tags: []metrics.T{metrics.Tag("kind", "witness_mismatch")}},
		{kind: "counter", name: "watch.event_log.corruption", value: int64(0), tags: []metrics.T{metrics.Tag("kind", "incomplete")}},
		{kind: "counter", name: "watch.event_log.corruption", value: int64(0), tags: []metrics.T{metrics.Tag("kind", "invalid_object")}},
		{kind: "counter", name: "watch.event_log.corrupt_alarm_failed", value: int64(0)},
	}, recorder.records)
}
