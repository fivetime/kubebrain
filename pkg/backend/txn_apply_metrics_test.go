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
