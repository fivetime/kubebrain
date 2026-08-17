// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthRevisionMetricsInitializeErrorZeroAndRefreshPersistedRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	recorder := &recordingMetrics{}
	server.metricCli = recorder
	initAuthRevisionMetrics(recorder)

	server.RefreshAuthMetrics(context.Background())

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.Equal(t, []recordedCounter{
		{name: "auth.revision.refresh.err", value: int64(0)},
	}, recorder.counters)
	require.Equal(t, []recordedGauge{
		{name: "etcd_debugging.auth.revision", value: uint64(initialAuthRevision)},
	}, recorder.gauges)
}
