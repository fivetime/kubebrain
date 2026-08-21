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

package tikv

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Security holds the mTLS material for the KubeBrain->TiKV/PD data plane. When any
// path is set it is applied to the tikv client-go global config so BOTH the PD
// (safepoint etcd) and TiKV RPC connections run over TLS; empty = plaintext,
// preserving existing behavior (#33).
type Security struct {
	CAPath   string
	CertPath string
	KeyPath  string
	VerifyCN []string
}

// TLSConfig builds the same client TLS policy used by every TiKV and PD
// connection. Keeping admission metadata on this path prevents it from
// silently omitting the configured server-CN restriction.
func (s Security) TLSConfig() (*tls.Config, error) {
	security := tikvcfg.NewSecurity(s.CAPath, s.CertPath, s.KeyPath, s.VerifyCN)
	return security.ToTLSConfig()
}

func (s Security) enabled() bool {
	return s.CAPath != "" || s.CertPath != "" || s.KeyPath != ""
}

type clientBalancer struct {
	clients []*txnkv.Client
	idx     uint64
}

var _ storage.SnapshotGetter = (*store)(nil)
var _ storage.SnapshotProtector = (*store)(nil)
var _ storage.SnapshotRegionWarmer = (*store)(nil)
var _ storage.SnapshotReadinessValidator = (*store)(nil)

// defaultClientNum is the fallback number of round-robined txnkv clients when
// the caller passes a non-positive count. Each client carries its own PD
// connections, region cache and TSO dispatcher, so an excessive count
// multiplies PD load and fragments TSO batching rather than adding throughput.
const defaultClientNum = 16

func newDetachableStartupContext(parent context.Context) (context.Context, context.CancelFunc, func() bool) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	return ctx, cancel, context.AfterFunc(parent, cancel)
}

func NewKvStorage(pdAddrs []string, clientNum int, sec Security) (storage.KvStorage, error) {
	return NewKvStorageWithContext(context.Background(), pdAddrs, clientNum, sec)
}

// MaxClientNum bounds the independently connected TiKV clients created by one
// KubeBrain process. Each client owns PD connections, a Region cache and a TSO
// stream, so an unbounded flag value can exhaust memory and control-plane
// connections before the server starts serving.
const MaxClientNum = 128

// NewKvStorageWithContext binds all parallel txn client construction to the
// caller's startup and shutdown lifecycle.
func NewKvStorageWithContext(ctx context.Context, pdAddrs []string, clientNum int, sec Security) (storage.KvStorage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if clientNum <= 0 {
		clientNum = defaultClientNum
	}
	if clientNum > MaxClientNum {
		return nil, errors.Errorf("TiKV client count %d exceeds maximum %d", clientNum, MaxClientNum)
	}
	// txnkv.NewClient reads config.GetGlobalConfig().Security for both the PD
	// safepoint-etcd client and the TiKV RPC client, so applying it here secures
	// the whole KubeBrain->TiKV/PD data plane before any client is created (#33).
	if sec.enabled() {
		tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) {
			c.Security = tikvcfg.NewSecurity(sec.CAPath, sec.CertPath, sec.KeyPath, sec.VerifyCN)
		})
	}
	// PD's NewClientWithContext uses its parent for the client's entire lifetime.
	// Forward cancellation only while the pool is being built; after successful
	// publication, Endpoint owns shutdown ordering and closes storage after gRPC
	// drain. Canceling the process root must not tear PD down ahead of that drain.
	startupCtx, cancelStartup, stopStartupCancellation := newDetachableStartupContext(ctx)
	clients, err := createTxnClients(clientNum, func(index int) (*txnkv.Client, error) {
		return createTxnClientWithEndpointRotation(pdAddrs, index, func(addrs []string) (*txnkv.Client, error) {
			return txnkv.NewClientWithContext(startupCtx, addrs)
		})
	})
	if err != nil {
		stopStartupCancellation()
		cancelStartup()
		return nil, err
	}
	if !stopStartupCancellation() {
		closeErr := closeClient(clients)
		cancelStartup()
		if err := ctx.Err(); err != nil {
			return nil, multierr.Append(err, closeErr)
		}
		return nil, multierr.Append(context.Canceled, closeErr)
	}
	s := NewKvStoreWithClient(clients)
	return s, nil
}

func createTxnClientWithEndpointRotation(pdAddrs []string, start int, factory func([]string) (*txnkv.Client, error)) (*txnkv.Client, error) {
	if len(pdAddrs) == 0 {
		return nil, errors.New("no PD endpoints configured")
	}
	var lastErr error
	for attempt := 0; attempt < len(pdAddrs); attempt++ {
		first := (start + attempt) % len(pdAddrs)
		rotated := make([]string, 0, len(pdAddrs))
		rotated = append(rotated, pdAddrs[first:]...)
		rotated = append(rotated, pdAddrs[:first]...)
		client, err := factory(rotated)
		if err == nil {
			return client, nil
		}
		lastErr = err
	}
	return nil, errors.Wrapf(lastErr, "all %d PD endpoint rotations failed", len(pdAddrs))
}

func createTxnClients(clientNum int, factory func(int) (*txnkv.Client, error)) ([]*txnkv.Client, error) {
	clients := make([]*txnkv.Client, clientNum)
	errs := make([]error, clientNum)
	var wg sync.WaitGroup
	wg.Add(clientNum)
	for i := 0; i < clientNum; i++ {
		go func(index int) {
			defer wg.Done()
			clients[index], errs[index] = factory(index)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			continue
		}
		created := make([]*txnkv.Client, 0, clientNum)
		for _, client := range clients {
			if client != nil {
				created = append(created, client)
			}
		}
		createErr := errors.Wrapf(err, "failed to create txn client %d", i)
		return nil, multierr.Append(createErr, closeClient(created))
	}
	return clients, nil
}

func NewKvStoreWithStorage(sts []*tikv.KVStore) storage.KvStorage {
	clients := make([]*txnkv.Client, 0, len(sts))
	for _, st := range sts {
		clients = append(clients, &txnkv.Client{KVStore: st})
	}
	return NewKvStoreWithClient(clients)
}

func NewKvStoreWithClient(clients []*txnkv.Client) storage.KvStorage {
	s := &store{
		clientBalancer: &clientBalancer{clients: clients},
		closed:         make(chan struct{}),
	}
	return s
}

func closeClient(clients []*txnkv.Client) error {
	var closeErr error
	for _, client := range clients {
		closeErr = multierr.Append(closeErr, client.Close())
	}
	return closeErr
}

// SupportTTL implements storage.KvStorage interface
func (s *store) SupportTTL() bool {
	return false
}

func (c *clientBalancer) getClient() *txnkv.Client {
	idx := atomic.AddUint64(&c.idx, 1)
	return c.clients[int(idx)%len(c.clients)]
}

func (c *clientBalancer) getSnapshotClient(ctx context.Context, timestamp uint64) *txnkv.Client {
	if storage.ProtectedSnapshotFromContext(ctx) {
		// Every operation in one immutable checkpoint must share a Region cache.
		// Round-robin selection would make each sequential metadata point read pay
		// the unavailable-replica timeout independently before a healthy follower
		// can become the protected Region's preferred peer. Timestamp hashing keeps
		// different checkpoints distributed without losing affinity within one.
		return c.clients[int(timestamp%uint64(len(c.clients)))]
	}
	return c.getClient()
}

type store struct {
	*clientBalancer
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (s *store) Del(ctx context.Context, key []byte) (err error) {
	b := s.BeginBatchWrite()
	b.Del(key)
	return b.Commit(ctx)
}

func (s *store) DelCurrent(ctx context.Context, iter storage.Iter) (err error) {
	b := s.BeginBatchWrite()
	b.DelCurrent(iter)
	return b.Commit(ctx)
}

const (
	// gcServiceID is the reserved PD service name of THE GC owner. KubeBrain
	// assumes the gc_worker role on clusters where no TiDB drives GC, so it
	// updates that same record rather than registering a side service: PD
	// special-cases "gc_worker" (its record cannot be deleted and its TTL must
	// be infinite), and a departed TiDB's stale gc_worker record would
	// otherwise pin the service-minimum forever with no way to clear it.
	// Sharing the name is also the correct handoff semantics: whichever GC
	// owner updated last defines the target, and the min across OTHER services
	// (CDC, BR) still clamps both.
	gcServiceID = "gc_worker"
)

// GC implements storage.GarbageCollector: advances the TiKV cluster GC
// safepoint to (PD-now - lifetime) via client-go's KVStore.GC, which resolves
// stale percolator locks below the safepoint before publishing it (the
// correctness step TiDB's gc_worker normally performs). Compaction filters
// then reclaim MVCC versions below the safepoint as RocksDB compacts.
// The physical time comes from PD's TSO, not the local clock.
//
// Shared-cluster safety: before publishing, KubeBrain registers its target as
// its own service safepoint and receives the minimum across ALL services
// (TiDB CDC changefeeds, BR backups, another GC owner...) — the same protocol
// TiDB's gc_worker follows. The published safepoint is clamped to that
// minimum, so co-tenants needing longer MVCC retention are never GC'd out
// from under them. On a KubeBrain-exclusive cluster (the recommended
// deployment) the minimum is simply KubeBrain's own target and the clamp is a
// no-op.
func (s *store) GC(ctx context.Context, lifetime time.Duration) (uint64, error) {
	ts, err := s.GetTimestampOracle(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "gc: fetch tso")
	}
	physical := oracle.ExtractPhysical(ts) - lifetime.Milliseconds()
	if physical <= 0 {
		return 0, nil
	}
	target := oracle.ComposeTS(physical, 0)
	minServiceSP, err := s.getClient().GetPDClient().UpdateServiceGCSafePoint(
		ctx, gcServiceID, math.MaxInt64, target)
	if err != nil {
		return 0, errors.Wrap(err, "gc: update service safepoint")
	}
	return s.getClient().GC(ctx, clampGCTarget(target, minServiceSP))
}

// clampGCTarget lowers the GC target to the minimum service safepoint when
// another service still needs older MVCC history. A zero minimum (no valid
// service records — should not happen since we just registered ours) is
// ignored rather than treated as "keep everything forever".
func clampGCTarget(target, minServiceSafePoint uint64) uint64 {
	if minServiceSafePoint > 0 && minServiceSafePoint < target {
		return minServiceSafePoint
	}
	return target
}

func (s *store) GetTimestampOracle(ctx context.Context) (timestamp uint64, err error) {
	// Go through txnkv's bounded PD backoff instead of calling the oracle once.
	// A PD leader handoff can make an otherwise healthy endpoint briefly answer
	// ErrGenerateTimestamp/not-leader; surfacing that single response made normal
	// reads and checkpoint refreshes fail even though client-go can rediscover the
	// leader within the caller's deadline.
	timestamp, err = s.getClient().GetTimestamp(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, fmt.Errorf("%w: fail to get timestamp: %v", storage.ErrUnavailable, err)
	}
	return timestamp, err
}

func (s *store) ProtectSnapshot(ctx context.Context, serviceID string, ttl time.Duration, timestamp uint64) (uint64, error) {
	if serviceID == "" || ttl <= 0 || timestamp == 0 {
		return 0, errors.New("snapshot protection requires service ID, positive TTL, and timestamp")
	}
	seconds := int64((ttl + time.Second - 1) / time.Second)
	minimum, err := s.getClient().GetPDClient().UpdateServiceGCSafePoint(ctx, serviceID, seconds, timestamp)
	if err != nil {
		return 0, errors.Wrap(err, "protect snapshot with PD service safepoint")
	}
	return minimum, nil
}

func (s *store) ReleaseSnapshot(ctx context.Context, serviceID string) error {
	if serviceID == "" {
		return errors.New("snapshot protection requires service ID")
	}
	_, err := s.getClient().GetPDClient().UpdateServiceGCSafePoint(ctx, serviceID, 0, 0)
	return errors.Wrap(err, "release snapshot PD service safepoint")
}

func maxBytes(a []byte, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func minBytes(a []byte, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return b
	}
	return a
}

// ClusterID implements storage.ClusterIdentifier with the PD cluster ID.
func (s *store) ClusterID() uint64 {
	return s.getClient().GetClusterID()
}

func (s *store) GetPartitions(ctx context.Context, start, end []byte) (partitions []storage.Partition, err error) {
	pdClient := s.getClient().GetPDClient()
	// scan regions without limit
	regs, err := pdClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get regions")
	}
	// ! StartKey of the first region and EndKey of the last region may be empty
	if len(regs) > 1 {
		partitions = make([]storage.Partition, len(regs))
		for idx, reg := range regs {

			if len(reg.Meta.StartKey) != 0 {
				partitions[idx].Start = maxBytes(reg.Meta.StartKey, start)
			} else {
				partitions[idx].Start = start
			}

			if len(reg.Meta.EndKey) != 0 {
				partitions[idx].End = minBytes(reg.Meta.EndKey, end)
			} else {
				partitions[idx].End = end
			}

		}
	} else {
		partitions = []storage.Partition{{Start: start, End: end}}
	}

	return partitions, err
}

type tiKvIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	NextWithContext(context.Context) error
	Close()
}

// A protected snapshot is served under a ten-second etcd unary deadline. Keep
// each replica attempt short enough to retry the other cached TiKV peers when
// one store is blackholed; ordinary latest-revision reads retain client-go's
// command-specific defaults.
const (
	protectedSnapshotRequestTimeout = 2 * time.Second
)

func configureProtectedSnapshot(snapshot *txnsnapshot.KVSnapshot) {
	configureProtectedSnapshotReplica(snapshot, tikvkv.ReplicaReadMixed)
}

func configureProtectedSnapshotReplica(snapshot *txnsnapshot.KVSnapshot, replicaRead tikvkv.ReplicaReadType) {
	snapshot.SetReplicaRead(replicaRead)
	// The checkpoint timestamp is immutable and held above TiKV's GC safe point.
	// Mark it as a stale read so healthy followers may serve it directly when the
	// cached leader is partitioned; replica selection alone does not authorize a
	// follower read in TiKV's request context.
	snapshot.SetIsStalenessReadOnly(true)
	snapshot.SetRequestTimeout(protectedSnapshotRequestTimeout)
	snapshot.SetCacheOnlyRegionRead(true)
}

func (s *store) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	var err error
	reverse := bytes.Compare(start, end) > 0
	pinned := timestamp != 0
	if timestamp == 0 {
		timestamp, err = s.GetTimestampOracle(ctx)
		if err != nil {
			return nil, err
		}
	}
	snapshot := s.getSnapshotClient(ctx, timestamp).GetSnapshot(timestamp)
	if pinned && storage.ProtectedSnapshotFromContext(ctx) {
		// Protected checkpoints are immutable historical snapshots. Mixed replica
		// reads preserve their value semantics and let a warmed Region cache route
		// around one unavailable TiKV store without asking PD for a new leader.
		configureProtectedSnapshot(snapshot)
	}
	var it tiKvIterator

	if !reverse {
		it, err = snapshot.IterWithContext(ctx, start, end)
	} else {
		// iter of tikv failed to scan the start, so append \x00 to start to get it
		next := append(start, '\x00')
		it, err = snapshot.IterReverseWithContext(ctx, next)
	}

	if err != nil {
		return nil, errors.Wrap(err, "failed to create TiKV iter")
	}
	return &iter{iter: it, limit: int(limit), end: end, reverse: reverse}, err
}

func (s *store) BeginBatchWrite() storage.BatchWrite {
	client := s.getClient()
	return &batch{begin: func(ctx context.Context) (*txnkv.KVTxn, error) {
		txn, err := client.BeginWithContext(ctx)
		if err != nil {
			return nil, unavailableBeginError(ctx, "write", err)
		}
		return txn, nil
	}}
}

func (s *store) Get(ctx context.Context, key []byte) (val []byte, err error) {
	txn, err := s.getClient().BeginWithContext(ctx)
	if err != nil {
		return nil, unavailableBeginError(ctx, "read", err)
	}

	val, err = txn.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, storage.ErrKeyNotFound
		}
		return nil, errors.Wrapf(err, "failed to get key %s", string(key))
	}

	err = txn.Commit(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to commit read txn of key %s", string(key))
	}
	return val, nil
}

// Beginning a TiKV transaction obtains a timestamp from PD. A failure at this
// boundary is therefore an availability failure, even when client-go returns
// an untyped (and occasionally empty) error. Mark it explicitly so the gRPC
// boundary returns Unavailable and etcd clients can retry within their request
// deadline instead of treating grpc-go's fallback Unknown as permanent.
func unavailableBeginError(ctx context.Context, kind string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%w: failed to create %s txn: %v", storage.ErrUnavailable, kind, err)
}

// GetAt reads a point key from an explicit TiKV MVCC snapshot. It deliberately
// avoids Begin/GetTimestampOracle. An ordinary pinned snapshot may still load
// Region routes from PD; only WithProtectedSnapshotTimestamp authorizes the
// pre-warmed cache-only failure mode.
func (s *store) GetAt(ctx context.Context, key []byte, timestamp uint64) ([]byte, error) {
	if timestamp == 0 {
		return nil, errors.New("snapshot timestamp must be non-zero")
	}
	snapshot := s.getSnapshotClient(ctx, timestamp).GetSnapshot(timestamp)
	if storage.ProtectedSnapshotFromContext(ctx) {
		configureProtectedSnapshot(snapshot)
	}
	val, err := snapshot.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, storage.ErrKeyNotFound
		}
		return nil, errors.Wrapf(err, "failed to get key %s at snapshot %d", string(key), timestamp)
	}
	return val, nil
}

// WarmSnapshotRegions touches every supplied Region through every independent
// txn client. NewKvStorage deliberately creates multiple clients for request
// distribution, and each owns a separate Region cache; warming through
// getClient would therefore leave most caches dependent on PD.
func (s *store) WarmSnapshotRegions(ctx context.Context, starts [][]byte, timestamp uint64) error {
	if timestamp == 0 {
		return errors.New("snapshot timestamp must be non-zero")
	}
	if _, err := s.refreshSnapshotStores(ctx); err != nil {
		return err
	}
	if err := warmSnapshotRegionReaders(ctx, starts, len(s.clients), func(clientIndex int) snapshotRegionReader {
		return s.clients[clientIndex].GetSnapshot(timestamp)
	}); err != nil {
		return err
	}
	for clientIndex, client := range s.clients {
		if err := client.GetRegionCache().ProtectCachedRegions(starts); err != nil {
			return errors.Wrapf(err, "failed to protect checkpoint regions for client %d", clientIndex)
		}
	}
	return nil
}

func (s *store) refreshSnapshotStores(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := s.clients[0].GetPDClient().GetAllStores(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to discover checkpoint stores")
	}
	for clientIndex, client := range s.clients {
		if err := client.GetRegionCache().SeedStores(stores); err != nil {
			return nil, errors.Wrapf(err, "failed to seed checkpoint stores for client %d", clientIndex)
		}
	}
	return stores, nil
}

// SnapshotReadyTimestamp returns the largest timestamp that every TiKV store
// can already serve over its full key range. With three voters per Region, a
// checkpoint at this common floor remains readable after any one Store loss.
func (s *store) SnapshotReadyTimestamp(ctx context.Context, start, end []byte) (uint64, error) {
	// Store membership changes more frequently than the deliberately throttled
	// full Region scan. Refresh this small directory for every candidate so a
	// newly scheduled peer can be resolved from EpochNotMatch metadata without PD.
	stores, err := s.refreshSnapshotStores(ctx)
	if err != nil {
		return 0, err
	}
	regions, err := s.clients[0].GetPDClient().ScanRegions(ctx, start, end, -1)
	if err != nil {
		return 0, errors.Wrap(err, "discover checkpoint Region voters")
	}
	activeStores, err := checkpointStoresForRange(stores, regions)
	if err != nil {
		return 0, err
	}
	safeTS, err := s.clients[0].GetTiKVStoreSafeTSForRange(ctx, activeStores, start, end)
	if err != nil {
		return 0, errors.Wrap(err, "get checkpoint Store safe timestamps")
	}
	verifiedStores, err := s.refreshSnapshotStores(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "verify checkpoint Store topology")
	}
	verifiedRegions, err := s.clients[0].GetPDClient().ScanRegions(ctx, start, end, -1)
	if err != nil {
		return 0, errors.Wrap(err, "verify checkpoint Region topology")
	}
	verifiedActiveStores, err := checkpointStoresForRange(verifiedStores, verifiedRegions)
	if err != nil {
		return 0, errors.Wrap(err, "verify checkpoint topology")
	}
	if err := validateCheckpointTopology(
		stores, regions, activeStores, verifiedStores, verifiedRegions, verifiedActiveStores,
	); err != nil {
		return 0, err
	}
	return checkpointReadyTimestamp(activeStores, safeTS)
}

func checkpointReadyTimestamp(storeIDs []uint64, safeTS map[uint64]uint64) (uint64, error) {
	minimum := uint64(math.MaxUint64)
	for _, storeID := range storeIDs {
		timestamp, ok := safeTS[storeID]
		if !ok {
			return 0, errors.Errorf("TiKV Store %d safe timestamp is missing", storeID)
		}
		if timestamp == 0 {
			return 0, errors.Errorf("TiKV Store %d safe timestamp is not ready", storeID)
		}
		if timestamp < minimum {
			minimum = timestamp
		}
	}
	if minimum == math.MaxUint64 {
		return 0, errors.New("no TiKV Store safe timestamp discovered")
	}
	return minimum, nil
}

func (s *store) ValidateSnapshotPublication(ctx context.Context, start, end []byte, timestamp uint64) error {
	if timestamp == 0 {
		return errors.New("snapshot timestamp must be non-zero")
	}
	ready, err := s.SnapshotReadyTimestamp(ctx, start, end)
	if err != nil {
		return err
	}
	if ready < timestamp {
		return errors.Errorf("checkpoint timestamp %d exceeds current topology safe timestamp %d", timestamp, ready)
	}
	return nil
}

func validateCheckpointTopology(
	beforeStores []*metapb.Store,
	beforeRegions []*pd.Region,
	beforeSelected []uint64,
	afterStores []*metapb.Store,
	afterRegions []*pd.Region,
	afterSelected []uint64,
) error {
	if len(beforeSelected) != len(afterSelected) {
		return errors.New("checkpoint Store topology changed while reading safe timestamps")
	}
	for index := range beforeSelected {
		if beforeSelected[index] != afterSelected[index] {
			return errors.New("checkpoint Store topology changed while reading safe timestamps")
		}
	}
	beforeStoreByID := make(map[uint64]*metapb.Store, len(beforeStores))
	afterStoreByID := make(map[uint64]*metapb.Store, len(afterStores))
	for _, store := range beforeStores {
		if store != nil {
			beforeStoreByID[store.GetId()] = store
		}
	}
	for _, store := range afterStores {
		if store != nil {
			afterStoreByID[store.GetId()] = store
		}
	}
	for _, storeID := range beforeSelected {
		if !proto.Equal(beforeStoreByID[storeID], afterStoreByID[storeID]) {
			return errors.Errorf("checkpoint TiKV Store %d changed while reading safe timestamps", storeID)
		}
	}
	if len(beforeRegions) != len(afterRegions) {
		return errors.New("checkpoint Region topology changed while reading safe timestamps")
	}
	beforeRegionByID := make(map[uint64]*metapb.Region, len(beforeRegions))
	for _, region := range beforeRegions {
		if region == nil || region.Meta == nil {
			return errors.New("invalid checkpoint Region metadata before safe timestamp query")
		}
		beforeRegionByID[region.Meta.GetId()] = region.Meta
	}
	for _, region := range afterRegions {
		if region == nil || region.Meta == nil {
			return errors.New("invalid checkpoint Region metadata after safe timestamp query")
		}
		if !proto.Equal(beforeRegionByID[region.Meta.GetId()], region.Meta) {
			return errors.Errorf("checkpoint Region %d changed while reading safe timestamps", region.Meta.GetId())
		}
	}
	return nil
}

func checkpointStoresForRange(stores []*metapb.Store, regions []*pd.Region) ([]uint64, error) {
	activeTiKV := make(map[uint64]struct{}, len(stores))
	for _, store := range stores {
		if store != nil && store.GetState() == metapb.StoreState_Up &&
			tikvrpc.GetStoreTypeByMeta(store) == tikvrpc.TiKV {
			activeTiKV[store.GetId()] = struct{}{}
		}
	}
	if len(activeTiKV) == 0 {
		return nil, errors.New("no active TiKV Store discovered")
	}
	if len(regions) == 0 {
		return nil, errors.New("no checkpoint Region discovered")
	}

	selected := make(map[uint64]struct{})
	for _, region := range regions {
		if region == nil || region.Meta == nil {
			return nil, errors.New("invalid checkpoint Region metadata")
		}
		voterStores := make(map[uint64]struct{}, len(region.Meta.Peers))
		for _, peer := range region.Meta.Peers {
			if peer == nil || peer.GetIsWitness() || peer.GetRole() == metapb.PeerRole_Learner {
				continue
			}
			if _, active := activeTiKV[peer.GetStoreId()]; !active {
				continue
			}
			storeID := peer.GetStoreId()
			voterStores[storeID] = struct{}{}
			selected[storeID] = struct{}{}
		}
		if len(voterStores) < 2 {
			return nil, errors.Errorf(
				"checkpoint Region %d has only %d active non-witness TiKV voter Stores",
				region.Meta.GetId(), len(voterStores),
			)
		}
	}
	storeIDs := make([]uint64, 0, len(selected))
	for storeID := range selected {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Slice(storeIDs, func(i, j int) bool { return storeIDs[i] < storeIDs[j] })
	return storeIDs, nil
}

type snapshotRegionReader interface {
	Get(context.Context, []byte) ([]byte, error)
}

func warmSnapshotRegionReaders(ctx context.Context, starts [][]byte, clientCount int, readerFor func(int) snapshotRegionReader) error {
	for clientIndex := 0; clientIndex < clientCount; clientIndex++ {
		snapshot := readerFor(clientIndex)
		for _, start := range starts {
			_, err := snapshot.Get(ctx, start)
			if err != nil && !tikverr.IsErrNotFound(err) {
				return errors.Wrapf(err, "failed to warm snapshot region at %x through client %d", start, clientIndex)
			}
		}
	}
	return nil
}

// BatchGet implements storage.BatchGetter: fetch all keys in one snapshot read.
// Unlike Get (a full begin/commit transaction per key), this takes a single TSO
// and issues one snapshot BatchGet, which the client fans out per-region and runs
// concurrently — replacing N sequential per-key transactions on the event-log
// replay path. The returned map is keyed by string(key) and omits absent keys
// (BatchGet treats a missing key as "not present", not an error), matching the
// storage.BatchGetter contract; callers decide what an absent key means.
func (s *store) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	ts, err := s.GetTimestampOracle(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := s.getClient().GetSnapshot(ts)
	m, err := snapshot.BatchGet(ctx, keys)
	if err != nil {
		return nil, errors.Wrap(err, "failed to batch get from tikv snapshot")
	}
	return m, nil
}

// BatchGetAt is BatchGet at an explicit TiKV MVCC snapshot and does not contact
// the timestamp oracle. Region-cache misses may still require PD; the DBaaS
// isolation gate separately verifies the warmed-cache failure mode.
func (s *store) BatchGetAt(ctx context.Context, keys [][]byte, timestamp uint64) (map[string][]byte, error) {
	if timestamp == 0 {
		return nil, errors.New("snapshot timestamp must be non-zero")
	}
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	snapshot := s.getSnapshotClient(ctx, timestamp).GetSnapshot(timestamp)
	if storage.ProtectedSnapshotFromContext(ctx) {
		configureProtectedSnapshot(snapshot)
	}
	m, err := snapshot.BatchGet(ctx, keys)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to batch get from TiKV snapshot %d", timestamp)
	}
	return m, nil
}

// Close implements storage.KvStorage interface
func (s *store) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.closeErr = closeClient(s.clients)
	})
	return s.closeErr
}
