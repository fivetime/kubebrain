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
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"

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

func (s Security) enabled() bool {
	return s.CAPath != "" || s.CertPath != "" || s.KeyPath != ""
}

type clientBalancer struct {
	clients []*txnkv.Client
	idx     uint64
}

// defaultClientNum is the fallback number of round-robined txnkv clients when
// the caller passes a non-positive count. Each client carries its own PD
// connections, region cache and TSO dispatcher, so an excessive count
// multiplies PD load and fragments TSO batching rather than adding throughput.
const defaultClientNum = 16

func NewKvStorage(pdAddrs []string, clientNum int, sec Security) (storage.KvStorage, error) {
	if clientNum <= 0 {
		clientNum = defaultClientNum
	}
	// txnkv.NewClient reads config.GetGlobalConfig().Security for both the PD
	// safepoint-etcd client and the TiKV RPC client, so applying it here secures
	// the whole KubeBrain->TiKV/PD data plane before any client is created (#33).
	if sec.enabled() {
		tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) {
			c.Security = tikvcfg.NewSecurity(sec.CAPath, sec.CertPath, sec.KeyPath, sec.VerifyCN)
		})
	}
	clients := make([]*txnkv.Client, 0, clientNum)
	for i := 0; i < clientNum; i++ {
		txnClient, err := txnkv.NewClient(pdAddrs)
		if err != nil {
			closeClient(clients)
			return nil, errors.Wrap(err, "failed to create txn client")
		}
		clients = append(clients, txnClient)
	}
	s := NewKvStoreWithClient(clients)
	return s, nil
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

func closeClient(clients []*txnkv.Client) {
	for _, client := range clients {
		_ = client.Close()
	}
}

// SupportTTL implements storage.KvStorage interface
func (s *store) SupportTTL() bool {
	return false
}

func (c *clientBalancer) getClient() *txnkv.Client {
	idx := atomic.AddUint64(&c.idx, 1)
	return c.clients[int(idx)%len(c.clients)]
}

type store struct {
	*clientBalancer
	closed chan struct{}
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
	timestamp, err = s.getClient().GetOracle().GetTimestamp(ctx, oracleOption)
	if err != nil {
		return 0, errors.Wrap(err, "fail to get timestamp")
	}
	return timestamp, err
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
	return s.getClient().GetPDClient().GetClusterID(context.Background())
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
	Next() error
	Close()
}

func (s *store) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	var err error
	reverse := bytes.Compare(start, end) > 0
	if timestamp == 0 {
		timestamp, err = s.GetTimestampOracle(ctx)
		if err != nil {
			return nil, err
		}
	}
	snapshot := s.getClient().GetSnapshot(timestamp)
	var it tiKvIterator

	if !reverse {
		it, err = snapshot.Iter(start, end)
	} else {
		// iter of tikv failed to scan the start, so append \x00 to start to get it
		next := append(start, '\x00')
		it, err = snapshot.IterReverse(next)
	}

	if err != nil {
		return nil, errors.Wrap(err, "failed to create TiKV iter")
	}
	return &iter{iter: it, limit: int(limit), end: end, reverse: reverse}, err
}

func (s *store) BeginBatchWrite() storage.BatchWrite {
	b := &batch{}
	var err error
	f := func(ctx context.Context) error {
		return err
	}
	b.txn, err = s.getClient().Begin()
	b.list = append(b.list, f)
	return b
}

func (s *store) Get(ctx context.Context, key []byte) (val []byte, err error) {
	txn, err := s.getClient().Begin()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create read txn")
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

// Close implements storage.KvStorage interface
func (s *store) Close() error {
	close(s.closed)
	closeClient(s.clients)
	return nil
}

var oracleOption = &oracle.Option{TxnScope: oracle.GlobalTxnScope}
