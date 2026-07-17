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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// LeaderElection is responsible for run leader election
// and handle state when leader status changed
type LeaderElection interface {
	// Campaign run leader election loop
	Campaign(ctx context.Context)

	// GetLeaderInfo get leader info, return peer address
	GetLeaderInfo() string

	// LeadershipTerm returns the cluster-wide election term. It is derived from
	// the shared resource-lock transition counter, so leaders and followers
	// report the same monotone value.
	LeadershipTerm(ctx context.Context) (uint64, error)

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
	backend b.Backend
	// resource lock
	resourceLock resourcelock.Interface
	metricCli    metrics.Metrics
	// onStartedLeading is called when a LeaderElector client starts leading
	onStartedLeading func(context.Context)
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
	// lastRenewNanos is the UnixNano of the most recent successful lease
	// Create/Update (leadership renew), stamped by renewStampingLock. It bounds
	// leadership freshness: a partitioned leader whose renews are failing stops
	// admitting/committing writes once this ages past renewDeadline, which is
	// strictly tighter than leaseDuration, so it self-fences before a successor
	// can acquire. Accessed atomically.
	lastRenewNanos int64

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
	onRenew func()
}

func (r *renewStampingLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Create(ctx, ler)
	if err == nil {
		r.onRenew()
	}
	return err
}

func (r *renewStampingLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Update(ctx, ler)
	if err == nil {
		r.onRenew()
	}
	return err
}

// NewLeaderElection returns a LeaderElection based on resourcelock of
// backend.Backend. cfg's zero fields take their defaults (8/5/1s); callers that
// expose the durations should Validate cfg before this.
func NewLeaderElection(backend b.Backend, metricCli metrics.Metrics, onStartedLeading func(context.Context), onStoppedLeading func(), cfg Config) LeaderElection {
	cfg = cfg.withDefaults()
	return &leaderElection{
		backend:          backend,
		resourceLock:     backend.GetResourceLock(),
		metricCli:        metricCli,
		onStartedLeading: onStartedLeading,
		onStoppedLeading: onStoppedLeading,
		leaseDuration:    cfg.LeaseDuration,
		renewDeadline:    cfg.RenewDeadline,
		retryPeriod:      cfg.RetryPeriod,
	}
}

// Campaign implements LeaderElection interface
func (l *leaderElection) Campaign(ctx context.Context) {
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            &renewStampingLock{Interface: l.resourceLock, onRenew: l.stampRenew},
		ReleaseOnCancel: true,
		// lease timeout deadline
		LeaseDuration: l.leaseDuration,
		// renew deadline
		RenewDeadline: l.renewDeadline,
		// renew lease period
		RetryPeriod: l.retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				klog.Info("start leading")
				l.metricCli.EmitCounter("leader.election.success", 1)
				leaderAddr, version, err := l.getLeaderAndVersion()
				if err != nil {
					klog.Fatal("leader lost")
					panic("invalid leader info")
				}
				l.metricCli.EmitGauge("leader.election.initial.version", version, metrics.Tag("addr", leaderAddr))
				// TODO push this logic to on start leading call back
				l.backend.SetCurrentRevision(version)
				// Bump the leadership epoch and stamp freshness BEFORE publishing
				// leader=1, so no write can be admitted under this term before its
				// epoch is live (FINDING #39). The successor of a previous leader
				// thus always admits writes under a strictly higher epoch.
				atomic.AddUint64(&l.epoch, 1)
				l.stampRenew()
				atomic.StoreInt32(&l.leader, 1)
				l.onStartedLeading(ctx)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here, or after the RunOrDie method
				// returns
				atomic.StoreInt32(&l.leader, 0)
				l.onStoppedLeading()
				leaderAddr := l.GetLeaderInfo()
				l.metricCli.EmitCounter("leader.election.lost", 1, metrics.Tag("addr", leaderAddr))
				if ctx.Err() != nil {
					klog.Info("leader election stopped by context cancellation")
					return
				}
				klog.Fatal("leader lost")
				// panic to avoid watchCache field in backend dirty, simple and rude
				panic("leader lost")
			},
		},
	})
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

// GetLeaderInfo implements LeaderElection interface
func (l *leaderElection) GetLeaderInfo() string {
	leaderAddr, _, _ := l.getLeaderAndVersion()
	return leaderAddr
}

// LeadershipTerm implements LeaderElection. client-go starts
// LeaderTransitions at zero for the first holder and increments it whenever
// the holder identity changes; expose +1 so the etcd RaftTerm analogue is
// positive from the first election.
func (l *leaderElection) LeadershipTerm(ctx context.Context) (uint64, error) {
	record, _, err := l.resourceLock.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("read leadership term: %w", err)
	}
	if record == nil {
		return 0, fmt.Errorf("read leadership term: empty election record")
	}
	if record.LeaderTransitions < 0 {
		return 0, fmt.Errorf("read leadership term: negative leader transitions %d", record.LeaderTransitions)
	}
	return uint64(record.LeaderTransitions) + 1, nil
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
