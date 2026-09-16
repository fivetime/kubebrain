package etcd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestPutAdmissionPhaseUnitsAndPartition(t *testing.T) {
	start := time.Unix(100, 0)
	p := putAdmissionTimings{start: start, routed: start.Add(time.Millisecond),
		quotaChecked: start.Add(3 * time.Millisecond), leaderReady: start.Add(6 * time.Millisecond),
		authAdmitted: start.Add(10 * time.Millisecond), authApplied: start.Add(15 * time.Millisecond),
		corruptChecked: start.Add(21 * time.Millisecond), leaseLocked: start.Add(28 * time.Millisecond),
		prepared: start.Add(36 * time.Millisecond)}
	for _, err := range []error{nil, errors.New("private request data")} {
		rec := &recordingMetrics{}
		emitPutAdmissionPhaseDurations(rec, p, err)
		phases := []string{"route", "quota", "leader_ready", "auth_admission", "auth_apply", "corrupt", "lease_guard", "effective_options"}
		require.Len(t, rec.histograms, len(phases))
		var total float64
		for i, phase := range phases {
			h := rec.histograms[i]
			require.Equal(t, "write.admission."+phase+".latency", h.name)
			require.Equal(t, float64(i+1)/1000, h.value)
			require.Equal(t, []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}, h.tags)
			total += h.value.(float64)
		}
		require.InDelta(t, p.prepared.Sub(p.start).Seconds(), total, 1e-12)
	}
	require.NotPanics(t, func() { emitPutAdmissionPhaseDurations(nil, p, nil) })
}

func TestPutAdmissionPhasePopulation(t *testing.T) {
	for _, mode := range []string{"success", "previous", "backend_failure", "validation_reject", "lease_reject", "follower_reject", "follower_proxy_error"} {
		t.Run(mode, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			s.metricCli = rec
			request := &etcdserverpb.PutRequest{Key: []byte("/admission/private-key"), Value: []byte("private-value")}
			proxied := false
			switch mode {
			case "previous":
				request.PrevKv = true
			case "backend_failure":
				s.backend = &nativePutRouteRecorder{BackendShim: s.backend, txnErr: errors.New("private backend detail")}
			case "validation_reject":
				request.Key = nil
			case "lease_reject":
				request.Lease = 987654
			case "follower_reject":
				s.peers = testPeerService{isLeader: false}
			case "follower_proxy_error":
				s.peers = testPeerService{isLeader: false, proxyEnabled: true,
					putFn: func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
						proxied = true
						return nil, errors.New("private proxy detail")
					}}
			}
			_, err := s.Put(context.Background(), request)
			if mode == "success" || mode == "previous" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, mode == "follower_proxy_error", proxied)
			rec.mu.Lock()
			histograms := append([]recordedHistogram(nil), rec.histograms...)
			rec.mu.Unlock()
			var phases int
			var total, parent float64
			for _, h := range histograms {
				if h.name == "write.pre_backend.latency" {
					parent = h.value.(float64)
				}
				if strings.HasPrefix(h.name, "write.admission.") {
					phases++
					require.GreaterOrEqual(t, h.value.(float64), 0.0)
					total += h.value.(float64)
					require.Equal(t, []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}, h.tags)
				}
			}
			if mode == "success" || mode == "previous" || mode == "backend_failure" {
				require.Equal(t, 8, phases)
				require.InDelta(t, parent, total, 1e-9)
			} else {
				require.Zero(t, phases, "rejected requests must not enter the backend population")
				require.Zero(t, parent)
			}
		})
	}
}

func TestPutBackendPhaseMetricsPairUnitsAndOutcome(t *testing.T) {
	for _, err := range []error{nil, errors.New("private request detail")} {
		rec := &recordingMetrics{}
		emitPutBackendPhaseDurations(rec, 25*time.Millisecond, 75*time.Millisecond, err)
		tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}
		require.Equal(t, []recordedHistogram{
			{name: "write.pre_backend.latency", value: 0.025, tags: tags},
			{name: "write.backend.latency", value: 0.075, tags: tags},
		}, rec.histograms)
	}
	require.NotPanics(t, func() { emitPutBackendPhaseDurations(nil, 0, 0, nil) })
}

func TestBatchPrewriteRPCMetricsUnitsAndPopulation(t *testing.T) {
	for _, attempted := range []bool{false, true} {
		rec := &recordingMetrics{}
		callback := storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))
		callback(storage.BatchCommitObservation{CommitAttempted: attempted, HasPrewriteRPCDetails: true,
			PrewriteRPCs: storage.PrewriteRPCObservation{Requests: 4, RegionErrors: 1, Duration: 25 * time.Millisecond, MaxDuration: 10 * time.Millisecond}})
		values := map[string]interface{}{}
		for _, h := range rec.histograms {
			values[h.name] = h.value
		}
		if !attempted {
			require.NotContains(t, values, "write.batch.prewrite_rpc.requests")
			continue
		}
		require.Equal(t, float64(4), values["write.batch.prewrite_rpc.requests"])
		require.Equal(t, float64(1), values["write.batch.prewrite_rpc.region_errors"])
		require.Equal(t, float64(0), values["write.batch.prewrite_rpc.transport_errors"])
		require.Equal(t, .025, values["write.batch.prewrite_rpc.total_latency"])
		require.Equal(t, .010, values["write.batch.prewrite_rpc.max_latency"])
		require.Len(t, rec.histograms, 10)
	}
}

func TestBatchLockRPCMetricsUnitsAndZeroPopulation(t *testing.T) {
	rec := &recordingMetrics{}
	callback := storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))
	callback(storage.BatchCommitObservation{HasLockRPCDetails: true, CommitAttempted: true,
		PrepareLocks: storage.LockRPCObservation{ResolveLock: storage.LockRPCSample{Requests: 2, TransportErrors: 1, Duration: 25 * time.Millisecond}},
	})
	values := map[string]interface{}{}
	for _, h := range rec.histograms {
		values[h.name] = h.value
	}
	require.Equal(t, float64(2), values["write.batch.lock_rpc.prepare.resolve_lock.requests"])
	require.Equal(t, float64(1), values["write.batch.lock_rpc.prepare.resolve_lock.transport_errors"])
	require.Equal(t, .025, values["write.batch.lock_rpc.prepare.resolve_lock.latency"])
	require.Equal(t, float64(0), values["write.batch.lock_rpc.commit.resolve_lock.requests"])
	require.Len(t, rec.histograms, 15)
	rec.histograms = nil
	callback(storage.BatchCommitObservation{HasLockRPCDetails: true, Err: errors.New("private")})
	require.Len(t, rec.histograms, 8, "early failure has prepare samples but no attempted commit phase")
}

func TestPutBatchMetricsUnitsPopulationAndOutcome(t *testing.T) {
	ctx := context.Background()
	require.True(t, ctx == observePutBatchCommits(ctx, nil))
	for _, result := range []error{nil, errors.New("not a metric label")} {
		rec := &recordingMetrics{}
		observed := observePutBatchCommits(ctx, rec)
		callback := storage.BatchCommitObserverFromContext(observed)
		require.NotNil(t, callback)
		callback(storage.BatchCommitObservation{
			Begin: time.Millisecond, Prepare: 2 * time.Millisecond,
			Commit: 9 * time.Millisecond, CommitAttempted: true,
			HasWriteDetails: true, Prewrite: 3 * time.Millisecond,
			CommitTS: time.Millisecond, PrimaryCommit: 4 * time.Millisecond, Err: result,
			PrewriteRegionGroups: 2,
		})
		tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(result)}
		require.Equal(t, []recordedHistogram{
			{name: "write.batch.begin.latency", value: .001, tags: tags},
			{name: "write.batch.prepare.latency", value: .002, tags: tags},
			{name: "write.batch.commit.latency", value: .009, tags: tags},
			{name: "write.batch.prewrite.latency", value: .003, tags: tags},
			{name: "write.batch.commit_ts.latency", value: .001, tags: tags},
			{name: "write.batch.primary_commit.latency", value: .004, tags: tags},
			{name: "write.batch.prewrite_region_groups", value: float64(2), tags: tags},
		}, rec.histograms)
		rec.histograms = nil
		callback(storage.BatchCommitObservation{Err: result})
		require.Len(t, rec.histograms, 2, "early failure has no commit or SDK sample")
		rec.histograms = nil
		callback(storage.BatchCommitObservation{CommitAttempted: true, Err: result})
		require.Len(t, rec.histograms, 3, "no-write transaction has no SDK write details")
	}
}

func TestPrimaryWriteMetricsPresenceUnitsAndBatchOutcome(t *testing.T) {
	for _, outcome := range []error{nil, errors.New("not exported")} {
		for _, details := range []string{"absent", "exec_only", "write", "invalid_write", "private provider text"} {
			rec := &recordingMetrics{}
			tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(outcome)}
			sample := storage.PrimaryWriteObservation{SuccessfulRPCs: 2, Details: details,
				RPC: 10 * time.Millisecond, PersistLog: 3 * time.Millisecond, RaftSync: time.Millisecond, CommitLog: 5 * time.Millisecond}
			emitPrimaryWriteSample(rec, sample, tags)
			label := details
			if details == "private provider text" {
				label = "invalid_write"
			}
			require.Equal(t, []recordedCounter{{name: "write.batch.primary_rpc.samples", value: 1,
				tags: append(append([]metrics.T(nil), tags...), metrics.Tag("details", label))}}, rec.counters)
			want := []recordedHistogram{{name: "write.batch.primary_rpc.successful_requests", value: float64(2), tags: tags}}
			if details == "write" {
				want = append(want,
					recordedHistogram{name: "write.batch.primary_rpc.rpc.latency", value: .010, tags: tags},
					recordedHistogram{name: "write.batch.primary_rpc.persist_log.latency", value: .003, tags: tags},
					recordedHistogram{name: "write.batch.primary_rpc.raft_sync.latency", value: .001, tags: tags},
					recordedHistogram{name: "write.batch.primary_rpc.commit_log.latency", value: .005, tags: tags})
			}
			require.Equal(t, want, rec.histograms)
			require.Len(t, tags, 2, "detail tags must not mutate the caller tag slice")
		}
	}
}

func TestPrimaryWriteMetricsNoSampleAndNegativeDurations(t *testing.T) {
	rec := &recordingMetrics{}
	emitPrimaryWriteSample(rec, storage.PrimaryWriteObservation{}, nil)
	require.Empty(t, rec.counters)
	require.Empty(t, rec.histograms)
	for _, field := range []string{"rpc", "persist", "raft", "commit"} {
		rec := &recordingMetrics{}
		sample := storage.PrimaryWriteObservation{SuccessfulRPCs: 1, Details: "write"}
		switch field {
		case "rpc":
			sample.RPC = -1
		case "persist":
			sample.PersistLog = -1
		case "raft":
			sample.RaftSync = -1
		case "commit":
			sample.CommitLog = -1
		}
		emitPrimaryWriteSample(rec, sample, nil)
		require.Len(t, rec.histograms, 1, "only request count, no invalid duration samples")
		require.Equal(t, []metrics.T{metrics.Tag("details", "invalid_write")}, rec.counters[0].tags)
	}
	rec = &recordingMetrics{}
	callback := storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))
	callback(storage.BatchCommitObservation{PrimaryWrite: storage.PrimaryWriteObservation{SuccessfulRPCs: 1, Details: "write"}})
	require.Empty(t, rec.counters, "no commit attempt means no primary sample")
}

func TestPrewriteSelectedMetricsPresenceAndBatchOutcome(t *testing.T) {
	for _, outcome := range []error{nil, errors.New("private error")} {
		for _, details := range []string{"absent", "exec_only", "write", "invalid_write", "private provider text"} {
			rec := &recordingMetrics{}
			callback := storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))
			callback(storage.BatchCommitObservation{CommitAttempted: true, HasPrewriteRPCDetails: true, Err: outcome,
				PrewriteRPCs: storage.PrewriteRPCObservation{SlowestSuccessful: storage.WriteRPCObservation{
					SuccessfulRPCs: 2, Details: details, RPC: 10 * time.Millisecond, PersistLog: 3 * time.Millisecond,
					RaftSync: time.Millisecond, CommitLog: 5 * time.Millisecond}}})
			tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(outcome)}
			label := details
			if details == "private provider text" {
				label = "invalid_write"
			}
			require.Equal(t, []recordedCounter{{name: "write.batch.prewrite_slowest_successful_rpc.samples", value: 1,
				tags: append(append([]metrics.T(nil), tags...), metrics.Tag("details", label))}}, rec.counters)
			var selected []recordedHistogram
			for _, h := range rec.histograms {
				if strings.Contains(h.name, "prewrite_slowest_successful_rpc") {
					selected = append(selected, h)
				}
			}
			want := []recordedHistogram{{name: "write.batch.prewrite_slowest_successful_rpc.successful_requests", value: float64(2), tags: tags}}
			if details == "write" {
				for i, phase := range []string{"rpc", "persist_log", "raft_sync", "commit_log"} {
					want = append(want, recordedHistogram{name: "write.batch.prewrite_slowest_successful_rpc." + phase + ".latency", value: []float64{.010, .003, .001, .005}[i], tags: tags})
				}
			}
			require.Equal(t, want, selected)
		}
	}
}

func TestPrewriteSelectedMetricsRequireAttemptDetailsAndSample(t *testing.T) {
	for _, missing := range []string{"attempt", "observation", "sample"} {
		rec := &recordingMetrics{}
		o := storage.BatchCommitObservation{CommitAttempted: true, HasPrewriteRPCDetails: true,
			PrewriteRPCs: storage.PrewriteRPCObservation{SlowestSuccessful: storage.WriteRPCObservation{SuccessfulRPCs: 1, Details: "write"}}}
		switch missing {
		case "attempt":
			o.CommitAttempted = false
		case "observation":
			o.HasPrewriteRPCDetails = false
		case "sample":
			o.PrewriteRPCs.SlowestSuccessful.SuccessfulRPCs = 0
		}
		storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))(o)
		require.Empty(t, rec.counters)
		for _, h := range rec.histograms {
			require.NotContains(t, h.name, "prewrite_slowest_successful_rpc")
		}
	}
}
