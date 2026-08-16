package nativepitr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	tikvcfg "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	pd "github.com/tikv/pd/client"
)

const TargetSnapshotEmptyFormat = "kubebrain.native-pitr-target-snapshot-empty.v1"

const WholeTransactionalKeyspace = "whole-transactional-keyspace"

// TargetStore identifies an Up TiKV store observed through the target PD.
type TargetStore struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
}

// TargetSnapshotEmptyReceipt proves that a full transactional keyspace scan at
// SnapshotTS returned no visible committed keys. It intentionally does not
// claim that obsolete MVCC versions or non-transactional/raw data are absent.
type TargetSnapshotEmptyReceipt struct {
	Format                      string        `json:"format"`
	ClusterID                   uint64        `json:"cluster_id"`
	PDAddrs                     []string      `json:"pd_addrs"`
	Stores                      []TargetStore `json:"stores"`
	SnapshotTS                  uint64        `json:"snapshot_ts"`
	ScanScope                   string        `json:"scan_scope"`
	VisibleCommittedKeyCount    uint64        `json:"visible_committed_key_count"`
	HistoricalMVCCAbsenceProven bool          `json:"historical_mvcc_absence_proven"`
	RawKVAbsenceProven          bool          `json:"raw_kv_absence_proven"`
	CheckedAtUnix               int64         `json:"checked_at_unix"`
	ReadOnly                    bool          `json:"read_only"`
}

// TargetProbe is the minimum read-only PD/TiKV surface needed to attest an
// empty transactional snapshot. Implementations own their connections.
type TargetProbe interface {
	PDClusterID(context.Context) uint64
	TxnClusterID() uint64
	GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error)
	SnapshotTSAndFirstKey(context.Context) (uint64, bool, error)
	Close()
}

type liveTargetProbe struct {
	pd  pd.Client
	txn *txnkv.Client
}

func (p *liveTargetProbe) PDClusterID(ctx context.Context) uint64 { return p.pd.GetClusterID(ctx) }
func (p *liveTargetProbe) TxnClusterID() uint64                   { return p.txn.GetClusterID() }
func (p *liveTargetProbe) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return p.pd.GetAllStores(ctx, opts...)
}
func (p *liveTargetProbe) SnapshotTSAndFirstKey(ctx context.Context) (uint64, bool, error) {
	ts, err := p.txn.GetTimestamp(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("obtain target TSO: %w", err)
	}
	snapshot := p.txn.GetSnapshot(ts)
	snapshot.SetKeyOnly(true)
	// RC reads committed versions while ignoring locks, avoiding SI scanner lock
	// resolution that could mutate a cluster advertised as read-only evidence.
	snapshot.SetIsolationLevel(txnkv.RC)
	it, err := snapshot.Iter(nil, nil)
	if err != nil {
		return 0, false, fmt.Errorf("open full transactional snapshot scan: %w", err)
	}
	defer it.Close()
	return ts, it.Valid(), nil
}
func (p *liveTargetProbe) Close() {
	_ = p.txn.Close()
	p.pd.Close()
}

// InspectLiveTargetSnapshotEmpty opens independent PD and txnkv clients so a
// caller cannot substitute a claimed cluster identity for the scanned target.
func InspectLiveTargetSnapshotEmpty(ctx context.Context, addrs []string, ca, cert, key string, checkedAt int64) (TargetSnapshotEmptyReceipt, error) {
	pdc, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{CAPath: ca, CertPath: cert, KeyPath: key})
	if err != nil {
		return TargetSnapshotEmptyReceipt{}, fmt.Errorf("connect target PD: %w", err)
	}
	tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) {
		c.Security = tikvcfg.NewSecurity(ca, cert, key, nil)
	})
	txn, err := txnkv.NewClientWithContext(ctx, addrs)
	if err != nil {
		pdc.Close()
		return TargetSnapshotEmptyReceipt{}, fmt.Errorf("connect target TiKV: %w", err)
	}
	return InspectTargetSnapshotEmpty(ctx, &liveTargetProbe{pd: pdc, txn: txn}, addrs, checkedAt)
}

func InspectTargetSnapshotEmpty(ctx context.Context, probe TargetProbe, addrs []string, checkedAt int64) (TargetSnapshotEmptyReceipt, error) {
	defer probe.Close()
	pdID, txnID := probe.PDClusterID(ctx), probe.TxnClusterID()
	if pdID == 0 || txnID == 0 || pdID != txnID {
		return TargetSnapshotEmptyReceipt{}, fmt.Errorf("target PD/TiKV cluster ID mismatch: pd=%d txn=%d", pdID, txnID)
	}
	stores, err := probe.GetAllStores(ctx)
	if err != nil {
		return TargetSnapshotEmptyReceipt{}, fmt.Errorf("list target TiKV stores: %w", err)
	}
	receipt := TargetSnapshotEmptyReceipt{Format: TargetSnapshotEmptyFormat, ClusterID: pdID, PDAddrs: append([]string(nil), addrs...), ScanScope: WholeTransactionalKeyspace, CheckedAtUnix: checkedAt, ReadOnly: true}
	for _, store := range stores {
		if store.GetState() == metapb.StoreState_Up {
			if store.GetId() == 0 || store.GetAddress() == "" {
				return TargetSnapshotEmptyReceipt{}, errors.New("target PD returned an invalid Up TiKV store")
			}
			receipt.Stores = append(receipt.Stores, TargetStore{ID: store.GetId(), Address: store.GetAddress()})
		}
	}
	if len(receipt.Stores) == 0 {
		return TargetSnapshotEmptyReceipt{}, errors.New("target PD returned no Up TiKV stores")
	}
	sort.Slice(receipt.Stores, func(i, j int) bool { return receipt.Stores[i].ID < receipt.Stores[j].ID })
	ts, found, err := probe.SnapshotTSAndFirstKey(ctx)
	if err != nil {
		return TargetSnapshotEmptyReceipt{}, err
	}
	if found {
		return TargetSnapshotEmptyReceipt{}, errors.New("target transactional keyspace is not empty at the observed snapshot")
	}
	receipt.SnapshotTS = ts
	if err := receipt.Validate(); err != nil {
		return TargetSnapshotEmptyReceipt{}, err
	}
	return receipt, nil
}

func DecodeTargetSnapshotEmpty(r io.Reader) (TargetSnapshotEmptyReceipt, error) {
	var receipt TargetSnapshotEmptyReceipt
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode target snapshot-empty receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("target snapshot-empty receipt contains trailing JSON")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r TargetSnapshotEmptyReceipt) Validate() error {
	if r.Format != TargetSnapshotEmptyFormat || !r.ReadOnly {
		return errors.New("target is not a read-only snapshot-empty v1 receipt")
	}
	if r.ClusterID == 0 || r.SnapshotTS == 0 || r.CheckedAtUnix <= 0 {
		return errors.New("target snapshot-empty receipt has invalid identity or observation time")
	}
	if r.ScanScope != WholeTransactionalKeyspace || r.VisibleCommittedKeyCount != 0 {
		return errors.New("target receipt does not prove an empty full transactional snapshot")
	}
	// These must remain false: this probe cannot establish either property and
	// accepting true would turn an explicitly bounded claim into a false one.
	if r.HistoricalMVCCAbsenceProven || r.RawKVAbsenceProven {
		return errors.New("target snapshot-empty receipt overclaims physical or raw-key emptiness")
	}
	if len(r.PDAddrs) == 0 || len(r.Stores) == 0 {
		return errors.New("target snapshot-empty receipt has no PD addresses or Up TiKV stores")
	}
	for i, addr := range r.PDAddrs {
		if !validTargetEndpoint(addr) || (i > 0 && r.PDAddrs[i-1] >= addr) {
			return errors.New("target snapshot-empty receipt has invalid or unsorted PD addresses")
		}
	}
	for i, store := range r.Stores {
		if store.ID == 0 || !validTargetEndpoint(store.Address) || (i > 0 && r.Stores[i-1].ID >= store.ID) {
			return errors.New("target snapshot-empty receipt has invalid or unsorted TiKV stores")
		}
	}
	return nil
}

func validTargetEndpoint(endpoint string) bool {
	if safeText("target endpoint", endpoint) != nil {
		return false
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n != 0
}
