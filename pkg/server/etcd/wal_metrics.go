// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import "github.com/kubewharf/kubebrain/pkg/metrics"

var etcdWALDurationMetrics = []string{
	"etcd.disk.wal_fsync_duration_seconds",
	"etcd.disk.wal_write_duration_seconds",
}

const etcdWALWriteBytesMetric = "etcd.disk.wal_write_bytes_total"

func initEtcdWALMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	if registrar, ok := metricCli.(metrics.HistogramRegistrar); ok {
		for _, name := range etcdWALDurationMetrics {
			_ = registrar.RegisterHistogram(name)
		}
	}
	// KubeBrain has no embedded etcd WAL. TiKV/PD own the replicated log and
	// expose its I/O through storage-native metrics, so every process-local etcd
	// WAL byte and latency value is exactly zero.
	_ = metricCli.EmitGauge(etcdWALWriteBytesMetric, 0)
}
