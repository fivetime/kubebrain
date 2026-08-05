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

package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// LeaderElection is responsible for run leader election
// and handle state when leader status changed
type LeaderElection interface {
	// Campaign run leader election loop
	Campaign(ctx context.Context)

	// GetLeaderInfo get leader info, return peer address
	GetLeaderInfo() string
	// RefreshLeaderInfo reloads the shared election record into the local lock
	// cache. Callers use this only when the cached holder is unknown after a
	// failover; steady-state reads remain storage-free through GetLeaderInfo.
	RefreshLeaderInfo(ctx context.Context) error

	// LeadershipTerm returns the cluster-wide election term. It is derived from
	// the shared resource-lock transition counter, so leaders and followers
	// report the same monotone value.
	LeadershipTerm(ctx context.Context) (uint64, error)
	// CurrentLeadershipTerm returns the most recently observed shared election
	// term without reading the TiKV-backed resource lock. It is zero before the
	// first valid resource-lock record is observed.
	CurrentLeadershipTerm() uint64

	// IsLeader return true when this instance is leader
	IsLeader() bool

	// EpochAndLeadingFresh returns this node's current leadership epoch together
	// with whether it is still safely leading: leader==1 AND the lease was
	// renewed within a bound strictly tighter than LeaseDuration. Write RPCs
	// capture the epoch at admission and re-test it immediately before commit so
	// a deposed (or partitioned-but-unaware) leader cannot commit a write under a
	// term it has already left — see FINDING #39 write fencing.
	EpochAndLeadingFresh() (uint64, bool)

	// GetElectionInfo get info of election
	GetElectionInfo() (ElectionInfo, error)
}

// ElectionInfo is the response for http election service
type ElectionInfo struct {
	// LeaderAddress is the ip address of leader
	LeaderAddress string

	// IsLeader returns true if current node is leader
	IsLeader bool
}

type leaderElection struct {
	backend interface {
		InitializeLeadershipRevision(context.Context, uint64) error
	}
	// resource lock
	resourceLock resourcelock.Interface
	metricCli    metrics.Metrics
	// onStartedLeading is called when a LeaderElector client starts leading
	onStartedLeading func(context.Context)
	// onPreparingLeading runs before leader ownership is published to RPC
	// goroutines.
	onPreparingLeading func()
	// onStoppedLeading is called when a LeaderElector client stops leading
	onStoppedLeading func()
	// leader indicates whether this instance is leader (1 = leader). It is written
	// by the leader-election callbacks and read by IsLeader() from every RPC
	// goroutine, so it is accessed atomically (#60/#68).
	leader int32
	// epoch is a monotone per-node leadership term counter, bumped in
	// OnStartedLeading BEFORE leader is published so the epoch is always live
	// before any write can be admitted under it. A write is admitted under the
	// epoch read at its RPC gate and re-checked for equality just before commit,
	// fencing a deposed leader's in-flight writes (FINDING #39). Accessed
	// atomically.
	epoch uint64
	// leadershipTerm caches LeaderTransitions+1 from the shared resource lock.
	// It is stamped onto every client response header, so reads must not perform
	// a storage read and TSO request per etcd RPC.
	leadershipTerm uint64
	// lastRenewNanos is the UnixNano of the most recent successful lease
	// Create/Update (leadership renew), stamped by renewStampingLock. It bounds
	// leadership freshness: a partitioned leader whose renews are failing stops
	// admitting/committing writes once this ages past renewDeadline, which is
	// strictly tighter than leaseDuration, so it self-fences before a successor
	// can acquire. Accessed atomically.
	lastRenewNanos int64
	// observationMu guards the raw election record and its local observation
	// deadline. As in client-go, a lease is timed from when this process observes
	// a changed raw record, never from the holder-supplied RenewTime (which is
	// unsafe across clock skew). time.Time retains its monotonic component.
	observationMu     sync.RWMutex
	observedRawRecord []byte
	leaderValidUntil  time.Time

	// leader-election durations (Config.withDefaults applied at construction).
	// renewDeadline doubles as the leadership-validity bound in
	// EpochAndLeadingFresh (FINDING #39).
	leaseDuration time.Duration
	renewDeadline time.Duration
	retryPeriod   time.Duration
}

// Default leader-election durations (client-go leaderelection). Overridable via
// Config so operators can trade failover speed against spurious-failover
// resistance; the defaults preserve the historical 8/5/1s behavior. The
// leadership-validity bound (how long after the last successful renew this node
// still admits/commits writes, FINDING #39) EQUALS RenewDeadline and is strictly
// less than LeaseDuration, so a partitioned-but-unaware leader self-fences before
// any successor can acquire the lease.
const (
	defaultLeaseDuration = 8 * time.Second
	defaultRenewDeadline = 5 * time.Second
	defaultRetryPeriod   = 1 * time.Second
)

// Config carries the tunable leader-election durations. A zero field takes its
// default. Invalid combinations are rejected by Validate.
type Config struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

func (c Config) withDefaults() Config {
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = defaultLeaseDuration
	}
	if c.RenewDeadline <= 0 {
		c.RenewDeadline = defaultRenewDeadline
	}
	if c.RetryPeriod <= 0 {
		c.RetryPeriod = defaultRetryPeriod
	}
	return c
}

// Validate enforces the ordering client-go requires and that the #39 write-fence
// self-fencing stays safe: RetryPeriod < RenewDeadline < LeaseDuration. Called on
// the operator-supplied values (defaults already applied).
func (c Config) Validate() error {
	c = c.withDefaults()
	if !(c.RetryPeriod < c.RenewDeadline && c.RenewDeadline < c.LeaseDuration) {
		return fmt.Errorf("leader election durations must satisfy RetryPeriod(%s) < RenewDeadline(%s) < LeaseDuration(%s)",
			c.RetryPeriod, c.RenewDeadline, c.LeaseDuration)
	}
	return nil
}

// renewStampingLock wraps the resource lock so leader.go observes each successful
// leadership renew (Create/Update) without racing the election goroutine's
// internal state. It records the renew time used to bound leadership freshness.
type renewStampingLock struct {
	resourcelock.Interface
	onRenew    func()
	onRecord   func(resourcelock.LeaderElectionRecord, []byte)
	onMutation func()
}

func (r *renewStampingLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	record, raw, err := r.Interface.Get(ctx)
	if err == nil && record != nil && r.onRecord != nil {
		r.onRecord(*record, raw)
	}
	return record, raw, err
}

func (r *renewStampingLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Create(ctx, ler)
	if err == nil {
		r.onRenew()
		if r.onMutation != nil {
			r.onMutation()
		}
		if r.onRecord != nil {
			r.onRecord(ler, nil)
		}
	}
	return err
}

func (r *renewStampingLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Update(ctx, ler)
	if err == nil {
		r.onRenew()
		if r.onMutation != nil {
			r.onMutation()
		}
		if r.onRecord != nil {
			r.onRecord(ler, nil)
		}
	}
	return err
}

// NewLeaderElection returns a LeaderElection based on resourcelock of
// backend.Backend. cfg's zero fields take their defaults (8/5/1s); callers that
// expose the durations should Validate cfg before this.
func NewLeaderElection(
	backend b.Backend,
	metricCli metrics.Metrics,
	onPreparingLeading func(),
	onStartedLeading func(context.Context),
	onStoppedLeading func(),
	cfg Config,
) LeaderElection {
	cfg = cfg.withDefaults()
	return &leaderElection{
		backend:            backend,
		resourceLock:       backend.GetResourceLock(),
		metricCli:          metricCli,
		onPreparingLeading: onPreparingLeading,
		onStartedLeading:   onStartedLeading,
		onStoppedLeading:   onStoppedLeading,
		leaseDuration:      cfg.LeaseDuration,
		renewDeadline:      cfg.RenewDeadline,
		retryPeriod:        cfg.RetryPeriod,
	}
}

// Campaign implements LeaderElection interface
func (l *leaderElection) Campaign(ctx context.Context) {
	for ctx.Err() == nil {
		runCtx, cancel := context.WithCancel(ctx)
		acquired := make(chan struct{})
		started := make(chan struct{})
		finished := make(chan struct{})
		var acquireOnce sync.Once
		elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock: &renewStampingLock{
				Interface: l.resourceLock, onRenew: l.stampRenew, onRecord: l.observeLeadershipRecordRaw,
				onMutation: func() { acquireOnce.Do(func() { close(acquired) }) },
			},
			ReleaseOnCancel: true,
			LeaseDuration:   l.leaseDuration,
			RenewDeadline:   l.renewDeadline,
			RetryPeriod:     l.retryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leadingCtx context.Context) {
					close(started)
					defer close(finished)
					klog.Info("start leading")
					l.metricCli.EmitCounter("leader.election.success", 1)
					leaderAddr, version, err := l.getLeaderAndVersion()
					if err != nil {
						l.metricCli.EmitCounter("leader.election.initialize.err", 1)
						klog.ErrorS(err, "initialize acquired leadership failed; retrying election")
						cancel()
						return
					}
					l.metricCli.EmitGauge("leader.election.initial.version", version, metrics.Tag("addr", leaderAddr))
					if err := l.backend.InitializeLeadershipRevision(leadingCtx, version); err != nil {
						l.metricCli.EmitCounter("leader.election.initialize.err", 1)
						klog.ErrorS(err, "restore acquired leadership revision failed; retrying election")
						cancel()
						return
					}
					// Publish only after the new epoch and freshness stamp are live,
					// so every admitted write is fenced to this exact term.
					atomic.AddUint64(&l.epoch, 1)
					l.stampRenew()
					if l.onPreparingLeading != nil {
						l.onPreparingLeading()
					}
					atomic.StoreInt32(&l.leader, 1)
					l.onStartedLeading(leadingCtx)
				},
				OnStoppedLeading: func() {
					atomic.StoreInt32(&l.leader, 0)
					l.onStoppedLeading()
					leaderAddr := l.GetLeaderInfo()
					l.metricCli.EmitCounter("leader.election.lost", 1, metrics.Tag("addr", leaderAddr))
					if ctx.Err() != nil {
						klog.Info("leader election stopped by context cancellation")
					} else {
						klog.Warning("leadership lost; retrying election in the same process")
					}
					// client-go starts OnStartedLeading in a goroutine and does
					// not join it. Drain this term before Campaign can construct
					// the next elector, otherwise lease/event/count initialization
					// from adjacent terms can overlap in one process.
					select {
					case <-acquired:
						<-started
						<-finished
					default:
						// Run also calls OnStoppedLeading when acquisition was
						// canceled before success; no callback exists to drain.
					}
				},
			},
		})
		if err != nil {
			cancel()
			klog.ErrorS(err, "invalid leader election configuration")
			return
		}
		elector.Run(runCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(l.retryPeriod)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// IsLeader implements LeaderElection interface
func (l *leaderElection) IsLeader() bool {
	return atomic.LoadInt32(&l.leader) == 1
}

// stampRenew records the current time as the most recent successful leadership
// renew. Called on every successful lease Create/Update and once in
// OnStartedLeading before leader is published.
func (l *leaderElection) stampRenew() {
	atomic.StoreInt64(&l.lastRenewNanos, time.Now().UnixNano())
}

// EpochAndLeadingFresh implements LeaderElection interface.
func (l *leaderElection) EpochAndLeadingFresh() (uint64, bool) {
	leading := atomic.LoadInt32(&l.leader) == 1
	last := atomic.LoadInt64(&l.lastRenewNanos)
	// Load the epoch last so, when we report fresh leadership, the epoch reflects
	// a term at least as new as the one that published leader==1.
	epoch := atomic.LoadUint64(&l.epoch)
	fresh := leading && last != 0 && time.Since(time.Unix(0, last)) < l.renewDeadline
	return epoch, fresh
}

// HasLeader reports whether this node is safely leading or has observed a
// non-expired shared election lease held by another node.
func (l *leaderElection) HasLeader() bool {
	if _, fresh := l.EpochAndLeadingFresh(); fresh {
		return true
	}
	l.observationMu.RLock()
	valid := time.Now().Before(l.leaderValidUntil)
	l.observationMu.RUnlock()
	return valid && election.IsLeaderKnown(l.GetLeaderInfo())
}

// GetLeaderInfo implements LeaderElection interface
func (l *leaderElection) GetLeaderInfo() string {
	leaderAddr, _, _ := l.getLeaderAndVersion()
	return leaderAddr
}

func (l *leaderElection) RefreshLeaderInfo(ctx context.Context) error {
	record, raw, err := l.resourceLock.Get(ctx)
	if err == nil && record != nil {
		l.observeLeadershipRecordRaw(*record, raw)
	}
	return err
}

// LeadershipTerm implements LeaderElection. client-go starts
// LeaderTransitions at zero for the first holder and increments it whenever
// the holder identity changes; expose +1 so the etcd RaftTerm analogue is
// positive from the first election.
func (l *leaderElection) LeadershipTerm(ctx context.Context) (uint64, error) {
	record, raw, err := l.resourceLock.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("read leadership term: %w", err)
	}
	if record == nil {
		return 0, fmt.Errorf("read leadership term: empty election record")
	}
	if record.LeaderTransitions < 0 {
		return 0, fmt.Errorf("read leadership term: negative leader transitions %d", record.LeaderTransitions)
	}
	l.observeLeadershipRecordRaw(*record, raw)
	return uint64(record.LeaderTransitions) + 1, nil
}

func (l *leaderElection) observeLeadershipRecord(record resourcelock.LeaderElectionRecord) {
	l.observeLeadershipRecordRaw(record, nil)
}

func (l *leaderElection) observeLeadershipRecordRaw(record resourcelock.LeaderElectionRecord, raw []byte) {
	if raw == nil {
		raw, _ = json.Marshal(record)
	}
	l.observationMu.Lock()
	if !bytes.Equal(raw, l.observedRawRecord) {
		l.observedRawRecord = append(l.observedRawRecord[:0], raw...)
		duration := l.leaseDuration
		if record.LeaseDurationSeconds > 0 {
			duration = time.Duration(record.LeaseDurationSeconds) * time.Second
		}
		l.leaderValidUntil = time.Now().Add(duration)
	}
	l.observationMu.Unlock()

	if record.LeaderTransitions < 0 {
		return
	}
	term := uint64(record.LeaderTransitions) + 1
	for {
		current := atomic.LoadUint64(&l.leadershipTerm)
		if term <= current || atomic.CompareAndSwapUint64(&l.leadershipTerm, current, term) {
			return
		}
	}
}

func (l *leaderElection) CurrentLeadershipTerm() uint64 {
	return atomic.LoadUint64(&l.leadershipTerm)
}

func (l *leaderElection) GetElectionInfo() (ElectionInfo, error) {
	leaderAddr, _, err := l.getLeaderAndVersion()
	if err != nil {
		return ElectionInfo{}, err
	}
	return ElectionInfo{
		LeaderAddress: leaderAddr,
		IsLeader:      l.IsLeader(),
	}, err
}

func (l *leaderElection) getLeaderAndVersion() (string, uint64, error) {
	lockInfo := l.resourceLock.Describe()
	infos := strings.Split(lockInfo, ",")
	if len(infos) != 2 {
		return "", 0, fmt.Errorf("lock info invalid %s", lockInfo)
	}
	version, err := strconv.ParseUint(infos[1], 10, 64)
	if err != nil {
		return "", 0, err
	}

	return infos[0], version, nil
}
