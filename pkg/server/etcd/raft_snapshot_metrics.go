// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import "github.com/kubewharf/kubebrain/pkg/metrics"

var etcdRaftSnapshotDurationMetrics = []string{
	"etcd_debugging.snap.save_marshalling_duration_seconds",
	"etcd_debugging.snap.save_total_duration_seconds",
	"etcd.snap.fsync_duration_seconds",
	"etcd.snap_db.save_total_duration_seconds",
	"etcd.snap_db.fsync_duration_seconds",
}

func initEtcdRaftSnapshotMetrics(metricCli metrics.Metrics) {
	registrar, ok := metricCli.(metrics.HistogramRegistrar)
	if !ok {
		return
	}
	// These families describe local etcd raft snapshot and .snap.db files.
	// KubeBrain has neither; TiKV/PD own raft snapshots, while the distinct
	// client Maintenance Snapshot lifecycle is reported by backend_snapshot.
	for _, name := range etcdRaftSnapshotDurationMetrics {
		_ = registrar.RegisterHistogram(name)
	}
}
