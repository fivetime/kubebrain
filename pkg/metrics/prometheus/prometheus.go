// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

var (
	registerer = prometheus.DefaultRegisterer
	gather     = prometheus.DefaultGatherer
)

type prometheusWrapper struct {
	globalLabelNames []string
	globalLabels     []metrics.T

	counterVecMu  sync.RWMutex
	counterVecMap map[string]*prometheus.CounterVec

	gaugeVecMu  sync.RWMutex
	gaugeVecMap map[string]*prometheus.GaugeVec

	histogramVecMu  sync.RWMutex
	histogramVecMap map[string]*prometheus.HistogramVec
}

// NewMetrics returns the prometheus implement of metrics.Metrics
func NewMetrics(globalLabels ...metrics.T) metrics.Metrics {
	pw := &prometheusWrapper{
		globalLabels:    globalLabels,
		counterVecMap:   make(map[string]*prometheus.CounterVec),
		gaugeVecMap:     make(map[string]*prometheus.GaugeVec),
		histogramVecMap: make(map[string]*prometheus.HistogramVec),
	}

	pw.globalLabelNames = pw.extractLabelNames(globalLabels)
	return pw
}

// GetHttpHandlers implements metrics.Metrics interface
func (pw *prometheusWrapper) GetHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/metrics": promhttp.Handler(),
	}
}

// GetGrpcServerOption implements metrics.Metrics interface
func (pw *prometheusWrapper) GetGrpcServerOption() []grpc.ServerOption {
	return GetGrpcServerOptions()
}

// EmitCounter implements metrics.Metrics interface
func (pw *prometheusWrapper) EmitCounter(name string, value interface{}, labels ...metrics.T) error {
	flt, err := convert2float64(value)
	if err != nil {
		return err
	}
	counter, err := pw.mustGetCounterVec(name, labels).GetMetricWith(pw.labelsToMap(labels))
	if err != nil {
		return fmt.Errorf("get counter %q: %w", name, err)
	}
	counter.Add(flt)
	return nil
}

// EmitGauge implements metrics.Metrics interface
func (pw *prometheusWrapper) EmitGauge(name string, value interface{}, labels ...metrics.T) error {
	flt, err := convert2float64(value)
	if err != nil {
		return err
	}
	gauge, err := pw.mustGetGaugeVec(name, labels).GetMetricWith(pw.labelsToMap(labels))
	if err != nil {
		return fmt.Errorf("get gauge %q: %w", name, err)
	}
	gauge.Set(flt)
	return nil
}

// EmitHistogram implements metrics.Metrics interface
func (pw *prometheusWrapper) EmitHistogram(name string, value interface{}, labels ...metrics.T) error {
	flt, err := convert2float64(value)
	if err != nil {
		return err
	}
	histogram, err := pw.mustGetHistogramVec(name, labels).GetMetricWith(pw.labelsToMap(labels))
	if err != nil {
		return fmt.Errorf("get histogram %q: %w", name, err)
	}
	histogram.Observe(flt)
	return nil
}

// RegisterHistogram creates the labeled histogram child without adding a
// sample, so Prometheus exposes its buckets/sum/count with count zero.
func (pw *prometheusWrapper) RegisterHistogram(name string, labels ...metrics.T) error {
	_, err := pw.mustGetHistogramVec(name, labels).GetMetricWith(pw.labelsToMap(labels))
	if err != nil {
		return fmt.Errorf("register histogram %q: %w", name, err)
	}
	return nil
}

func convert2float64(i interface{}) (float64, error) {
	switch s := i.(type) {
	case int:
		return float64(s), nil
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case string:
		v, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return v, nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", i, i)
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", i, i)
	}
}

func (pw *prometheusWrapper) labelsToMap(labels []metrics.T) (ret map[string]string) {
	ret = make(map[string]string)

	for _, label := range pw.globalLabels {
		ret[label.Name] = validLabelValue(label.Value)
	}

	for _, label := range labels {
		ret[label.Name] = validLabelValue(label.Value)
	}
	return
}

func validLabelValue(value string) string {
	return strings.ToValidUTF8(value, "\uFFFD")
}

func (pw *prometheusWrapper) extractLabelNames(labels []metrics.T) (ret []string) {
	ret = make([]string, len(labels)+len(pw.globalLabelNames))
	copy(ret, pw.globalLabelNames)
	offset := len(pw.globalLabelNames)
	for i, label := range labels {
		ret[i+offset] = label.Name
	}

	return
}

func (pw *prometheusWrapper) mustGetGaugeVec(name string, labels []metrics.T) (vec *prometheus.GaugeVec) {
	// return direct if it's exist
	pw.gaugeVecMu.RLock()
	vec = pw.gaugeVecMap[name]
	pw.gaugeVecMu.RUnlock()
	if vec != nil {
		return vec
	}

	// create a new metric
	pw.gaugeVecMu.Lock()
	defer pw.gaugeVecMu.Unlock()

	// double check
	vec = pw.gaugeVecMap[name]
	if vec != nil {
		return vec
	}

	vec = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: formatName(name), Help: metricHelp(name)}, pw.extractLabelNames(labels))
	registerer.MustRegister(vec)
	pw.gaugeVecMap[name] = vec
	return vec
}

func (pw *prometheusWrapper) mustGetCounterVec(name string, labels []metrics.T) (vec *prometheus.CounterVec) {
	// return direct if it's exist
	pw.counterVecMu.RLock()
	vec = pw.counterVecMap[name]
	pw.counterVecMu.RUnlock()
	if vec != nil {
		return vec
	}

	// create a new metric
	pw.counterVecMu.Lock()
	defer pw.counterVecMu.Unlock()

	// double check
	vec = pw.counterVecMap[name]
	if vec != nil {
		return vec
	}

	vec = prometheus.NewCounterVec(prometheus.CounterOpts{Name: formatName(name), Help: metricHelp(name)}, pw.extractLabelNames(labels))
	registerer.MustRegister(vec)
	pw.counterVecMap[name] = vec
	return vec
}

func (pw *prometheusWrapper) mustGetHistogramVec(name string, labels []metrics.T) (vec *prometheus.HistogramVec) {
	// return direct if it's exist
	pw.histogramVecMu.RLock()
	vec = pw.histogramVecMap[name]
	pw.histogramVecMu.RUnlock()
	if vec != nil {
		return vec
	}

	// create a new metric
	pw.histogramVecMu.Lock()
	defer pw.histogramVecMu.Unlock()

	// double check
	vec = pw.histogramVecMap[name]
	if vec != nil {
		return vec
	}
	opts := prometheus.HistogramOpts{Name: formatName(name), Help: metricHelp(name)}
	if name == "etcd.disk.backend_commit_duration_seconds" {
		// Match server/storage/backend/metrics.go: 1ms through 8.192s.
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 2, 14)
	} else if name == "etcd_debugging.disk.backend_commit_rebalance_duration_seconds" ||
		name == "etcd_debugging.disk.backend_commit_spill_duration_seconds" ||
		name == "etcd_debugging.disk.backend_commit_write_duration_seconds" {
		// Match the bbolt-only commit phase histograms in backend/metrics.go.
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 2, 14)
	} else if name == "etcd.disk.backend_snapshot_duration_seconds" {
		// Match server/storage/backend/metrics.go: 10ms through 655.36s.
		opts.Buckets = prometheus.ExponentialBuckets(0.01, 2, 17)
	} else if name == "etcd.disk.backend_defrag_duration_seconds" {
		// Match server/storage/backend/metrics.go: 100ms through 409.6s.
		opts.Buckets = prometheus.ExponentialBuckets(0.1, 2, 13)
	} else if name == "etcd.server.range_duration_seconds" {
		// Match server/etcdserver/txn/metrics.go: 0.1ms through 52.4288s.
		opts.Buckets = prometheus.ExponentialBuckets(0.0001, 2, 20)
	} else if name == "etcd.server.apply_duration_seconds" {
		// Match server/etcdserver/txn/metrics.go: 0.1ms through 52.4288s.
		opts.Buckets = prometheus.ExponentialBuckets(0.0001, 2, 20)
	} else if name == "etcd.server.request.duration.seconds" {
		// Match server/etcdserver/metrics.go: 1ms through 8.192s.
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 2, 14)
	} else if name == "etcd_debugging.lease.ttl_total" {
		// Match server/lease/metrics.go: 1 second through roughly 3 months.
		opts.Buckets = prometheus.ExponentialBuckets(1, 2, 24)
	} else if name == "etcd.mvcc.hash_duration_seconds" ||
		name == "etcd.mvcc.hash_rev_duration_seconds" {
		// Match server/storage/mvcc/metrics.go: 10ms through 163.84s.
		opts.Buckets = prometheus.ExponentialBuckets(0.01, 2, 15)
	} else if name == "etcd_debugging.server.watch_send_loop.watch_stream.duration.seconds" ||
		name == "etcd_debugging.server.watch_send_loop.watch_stream.duration_per_event.seconds" ||
		name == "etcd_debugging.server.watch_send_loop.control_stream.duration.seconds" ||
		name == "etcd_debugging.server.watch_send_loop.progress.duration.seconds" {
		// Match server/etcdserver/api/v3rpc/metrics.go: 1ms through 8.192s.
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 2, 14)
	} else if name == "etcd.disk.wal_fsync_duration_seconds" ||
		name == "etcd.disk.wal_write_duration_seconds" {
		// Match server/storage/wal/metrics.go: 1ms through 8.192s.
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 2, 14)
	} else if name == "etcd.snap_db.save_total_duration_seconds" {
		// Match server/etcdserver/api/snap/metrics.go: 100ms through 51.2s.
		opts.Buckets = prometheus.ExponentialBuckets(0.1, 2, 10)
	} else if name == "etcd_debugging.snap.save_marshalling_duration_seconds" ||
		name == "etcd_debugging.snap.save_total_duration_seconds" ||
		name == "etcd.snap.fsync_duration_seconds" ||
		name == "etcd.snap_db.fsync_duration_seconds" {
		// Match raft snapshot marshalling/save/fsync: 1ms through 8.192s.
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 2, 14)
	}
	vec = prometheus.NewHistogramVec(opts, pw.extractLabelNames(labels))
	registerer.MustRegister(vec)
	pw.histogramVecMap[name] = vec
	return vec
}

func metricHelp(name string) string {
	if strings.HasPrefix(formatName(name), "write_batch_lock_rpc_") {
		return "Batch phase context lock RPC observations completed before phase closure; unmarked and late calls excluded. Requests and transport_errors are counts per batch; latency is summed RPC wall seconds, including overlaps, not logical lock wait. Zero samples included; success labels batch outcome, not RPC outcome."
	}
	switch formatName(name) {
	case "write_batch_prewrite_region_groups":
		return "SDK prewrite Region groups per observed storage batch, including retries; not distinct Regions, RPC counts, or a commit protocol verdict."
	case "write_batch_primary_rpc_samples":
		return "Batches with a selected slowest successful synchronous primary Commit RPC, by raw detail presence; success labels the batch outcome. Async-enabled attempts and late/background observations are excluded."
	case "write_batch_primary_rpc_successful_requests":
		return "Successful matching primary Commit RPCs per observed synchronous batch before Commit returns, including retries; not logical Put counts."
	case "write_batch_primary_rpc_rpc_latency", "write_batch_primary_rpc_persist_log_latency", "write_batch_primary_rpc_raft_sync_latency", "write_batch_primary_rpc_commit_log_latency":
		return "Seconds for the slowest successful primary Commit RPC selected per synchronous batch, with valid raw WriteDetail only; success labels the batch outcome. RPC and server stages overlap; never add them. Not all RPCs or tail latency of the full operation."
	case "etcd_cluster_version":
		return "Which version is running. 1 for 'cluster_version' label with current cluster version"
	case "etcd_debugging_auth_revision":
		return "The current revision of auth store."
	case "etcd_debugging_disk_backend_commit_rebalance_duration_seconds":
		return "The latency distributions of commit.rebalance called by bboltdb backend."
	case "etcd_debugging_disk_backend_commit_spill_duration_seconds":
		return "The latency distributions of commit.spill called by bboltdb backend."
	case "etcd_debugging_disk_backend_commit_write_duration_seconds":
		return "The latency distributions of commit.write called by bboltdb backend."
	case "etcd_debugging_lease_granted_total":
		return "The total number of granted leases."
	case "etcd_debugging_lease_renewed_total":
		return "The number of renewed leases seen by the leader."
	case "etcd_debugging_lease_revoked_total":
		return "The total number of revoked leases."
	case "etcd_debugging_lease_ttl_total":
		return "Bucketed histogram of lease TTLs."
	case "etcd_debugging_mvcc_compact_revision":
		return "The revision of the last compaction in store."
	case "etcd_debugging_mvcc_current_revision":
		return "The current revision of store."
	case "etcd_debugging_mvcc_db_compaction_keys_total":
		return "Total number of db keys compacted."
	case "etcd_debugging_mvcc_db_compaction_last":
		return "The unix time of the last db compaction. Resets to 0 on start."
	case "etcd_debugging_mvcc_events_total":
		return "Total number of events sent by this member."
	case "etcd_debugging_mvcc_keys_total":
		return "Total number of keys."
	case "etcd_debugging_mvcc_pending_events_total":
		return "Total number of pending events to be sent."
	case "etcd_debugging_mvcc_slow_watcher_total":
		return "Total number of unsynced slow watchers."
	case "etcd_debugging_mvcc_total_put_size_in_bytes":
		return "The total size of put kv pairs seen by this member."
	case "etcd_debugging_mvcc_watch_stream_total":
		return "Total number of watch streams."
	case "etcd_debugging_mvcc_watcher_total":
		return "Total number of watchers."
	case "etcd_debugging_server_lease_expired_total":
		return "The total number of expired leases."
	case "etcd_debugging_snap_save_marshalling_duration_seconds":
		return "The marshalling cost distributions of save called by snapshot."
	case "etcd_debugging_snap_save_total_duration_seconds":
		return "The total latency distributions of save called by snapshot."
	case "etcd_disk_backend_commit_duration_seconds":
		return "The latency distributions of commit called by backend."
	case "etcd_disk_backend_defrag_duration_seconds":
		return "The latency distribution of backend defragmentation."
	case "etcd_disk_backend_snapshot_duration_seconds":
		return "The latency distribution of backend snapshots."
	case "etcd_disk_defrag_inflight":
		return "Whether or not defrag is active on the member. 1 means active, 0 means not."
	case "etcd_disk_wal_fsync_duration_seconds":
		return "The latency distributions of fsync called by WAL."
	case "etcd_disk_wal_write_bytes_total":
		return "Total number of bytes written in WAL."
	case "etcd_disk_wal_write_duration_seconds":
		return "The latency distributions of write called by WAL."
	case "etcd_mvcc_db_open_read_transactions":
		return "The number of currently open read transactions"
	case "etcd_mvcc_db_total_size_in_bytes":
		return "Total size of the underlying database physically allocated in bytes."
	case "etcd_mvcc_db_total_size_in_use_in_bytes":
		return "Total size of the underlying database logically in use in bytes."
	case "etcd_mvcc_delete_total":
		return "Total number of deletes seen by this member."
	case "etcd_mvcc_hash_duration_seconds":
		return "The latency distribution of storage hash operation."
	case "etcd_mvcc_hash_rev_duration_seconds":
		return "The latency distribution of storage hash by revision operation."
	case "etcd_mvcc_put_total":
		return "Total number of puts seen by this member."
	case "etcd_mvcc_range_total":
		return "Total number of ranges seen by this member."
	case "etcd_mvcc_txn_total":
		return "Total number of txns seen by this member."
	case "etcd_network_client_grpc_received_bytes_total":
		return "The total number of bytes received from grpc clients."
	case "etcd_network_client_grpc_sent_bytes_total":
		return "The total number of bytes sent to grpc clients."
	case "etcd_network_known_peers":
		return "The current number of known peers."
	case "etcd_server_go_version":
		return "Which Go version server is running with. 1 for 'server_go_version' label with current version."
	case "etcd_server_has_leader":
		return "Whether or not a leader exists. 1 is existence, 0 is not."
	case "etcd_server_health_failures":
		return "The total number of failed health checks"
	case "etcd_server_health_success":
		return "The total number of successful health checks"
	case "etcd_server_heartbeat_send_failures_total":
		return "The total number of leader heartbeat send failures (likely overloaded from slow disk)."
	case "etcd_server_id":
		return "Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID."
	case "etcd_server_is_leader":
		return "Whether or not this member is a leader. 1 if is, 0 otherwise."
	case "etcd_server_is_learner":
		return "Whether or not this member is a learner. 1 if is, 0 otherwise."
	case "etcd_server_leader_changes_seen_total":
		return "The number of leader changes seen."
	case "etcd_server_learner_promote_successes":
		return "The total number of successful learner promotions while this member is leader."
	case "etcd_server_proposals_applied_total":
		return "The total number of consensus proposals applied."
	case "etcd_server_proposals_committed_total":
		return "The total number of consensus proposals committed."
	case "etcd_server_proposals_failed_total":
		return "The total number of failed proposals seen."
	case "etcd_server_proposals_pending":
		return "The current number of pending proposals to commit."
	case "etcd_server_quota_backend_bytes":
		return "Current backend storage quota size in bytes."
	case "etcd_server_read_indexes_failed_total":
		return "The total number of failed read indexes seen."
	case "etcd_server_request_duration_seconds":
		return "Response latency distribution in seconds for each type."
	case "etcd_server_slow_apply_total":
		return "The total number of slow apply requests (likely overloaded from slow disk)."
	case "etcd_server_slow_read_indexes_total":
		return "The total number of pending read indexes not in sync with leader's or timed out read index requests."
	case "etcd_server_snapshot_apply_in_progress_total":
		return "1 if the server is applying the incoming snapshot. 0 if none."
	case "etcd_server_version":
		return "Which version is running. 1 for 'server_version' label with current version."
	case "etcd_debugging_server_watch_send_loop_watch_stream_duration_seconds":
		return "The total duration in seconds of running through the send loop watch stream response all events."
	case "etcd_debugging_server_watch_send_loop_watch_stream_duration_per_event_seconds":
		return "The average duration in seconds of running through the send loop watch stream response, per event."
	case "etcd_debugging_server_watch_send_loop_control_stream_duration_seconds":
		return "The total duration in seconds of running through the send loop control stream response."
	case "etcd_debugging_server_watch_send_loop_progress_duration_seconds":
		return "The total duration in seconds of running through the progress loop control stream response."
	case "etcd_snap_db_fsync_duration_seconds":
		return "The latency distributions of fsyncing .snap.db file"
	case "etcd_snap_db_save_total_duration_seconds":
		return "The total latency distributions of v3 snapshot save"
	case "etcd_snap_fsync_duration_seconds":
		return "The latency distributions of fsync called by snap."
	case "os_fd_limit":
		return "The file descriptor limit."
	case "os_fd_used":
		return "The number of used file descriptors."
	default:
		return ""
	}
}

func formatName(name string) string {
	return strings.Replace(name, ".", "_", -1)
}
