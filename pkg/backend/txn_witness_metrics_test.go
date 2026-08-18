// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package backend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func TestRestartWitnessCorruptionMetricsInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initRestartWitnessCorruptionMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "txn.witness.restart_corruption", value: 0, tags: []metrics.T{metrics.Tag("outcome", "armed")}},
		{kind: "counter", name: "txn.witness.restart_corruption", value: 0, tags: []metrics.T{metrics.Tag("outcome", "failed")}},
	}, recorder.records)
}
