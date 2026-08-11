package nativepitr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	tikvcfg "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	pd "github.com/tikv/pd/client"
)

const SourceRangeExclusiveFormat = "kubebrain.native-pitr-source-range-exclusive.v1"

type SourceRangeExclusiveReceipt struct {
	Format                      string   `json:"format"`
	ClusterID                   uint64   `json:"cluster_id"`
	PDAddrs                     []string `json:"pd_addrs"`
	Keyspace                    string   `json:"keyspace"`
	StartKeyHex                 string   `json:"start_key_hex"`
	EndKeyHex                   string   `json:"end_key_hex"`
	SnapshotTS                  uint64   `json:"snapshot_ts"`
	FullSnapshotReceiptSHA256   string   `json:"full_snapshot_receipt_sha256"`
	OutsideVisibleKeyCount      uint64   `json:"outside_visible_key_count"`
	HistoricalMVCCAbsenceProven bool     `json:"historical_mvcc_absence_proven"`
	CheckedAtUnix               int64    `json:"checked_at_unix"`
	ReadOnly                    bool     `json:"read_only"`
}

type SourceRangeProbe interface {
	PDClusterID(context.Context) uint64
	TxnClusterID() uint64
	HasVisibleKey(context.Context, uint64, []byte, []byte) (bool, error)
	Close()
}

func InspectSourceRangeExclusive(ctx context.Context, probe SourceRangeProbe, full FullSnapshotReceipt, fullSHA string, addrs []string, checkedAt int64) (SourceRangeExclusiveReceipt, error) {
	defer probe.Close()
	if err := validateFullSnapshotReceipt(full); err != nil {
		return SourceRangeExclusiveReceipt{}, err
	}
	if !sha256RE.MatchString(fullSHA) {
		return SourceRangeExclusiveReceipt{}, errors.New("invalid full-snapshot receipt SHA-256")
	}
	pdID, txnID := probe.PDClusterID(ctx), probe.TxnClusterID()
	if pdID == 0 || pdID != txnID || pdID != full.ClusterID {
		return SourceRangeExclusiveReceipt{}, errors.New("source PD/TiKV/full-snapshot cluster ID mismatch")
	}
	start, _ := hex.DecodeString(full.StartKeyHex)
	end, _ := hex.DecodeString(full.EndKeyHex)
	for _, bounds := range [][2][]byte{{nil, start}, {end, nil}} {
		found, err := probe.HasVisibleKey(ctx, full.BackupTS, bounds[0], bounds[1])
		if err != nil {
			return SourceRangeExclusiveReceipt{}, fmt.Errorf("scan source outside keyspace: %w", err)
		}
		if found {
			return SourceRangeExclusiveReceipt{}, errors.New("source has a visible transactional key outside the KubeBrain range at backup TSO")
		}
	}
	receipt := SourceRangeExclusiveReceipt{Format: SourceRangeExclusiveFormat, ClusterID: full.ClusterID, PDAddrs: append([]string(nil), addrs...), Keyspace: full.Keyspace, StartKeyHex: full.StartKeyHex, EndKeyHex: full.EndKeyHex, SnapshotTS: full.BackupTS, FullSnapshotReceiptSHA256: fullSHA, CheckedAtUnix: checkedAt, ReadOnly: true}
	if err := receipt.Validate(); err != nil {
		return SourceRangeExclusiveReceipt{}, err
	}
	return receipt, nil
}

type liveSourceRangeProbe struct {
	pd  pd.Client
	txn *txnkv.Client
}

func (p *liveSourceRangeProbe) PDClusterID(ctx context.Context) uint64 { return p.pd.GetClusterID(ctx) }
func (p *liveSourceRangeProbe) TxnClusterID() uint64                   { return p.txn.GetClusterID() }
func (p *liveSourceRangeProbe) HasVisibleKey(_ context.Context, ts uint64, start, end []byte) (bool, error) {
	snapshot := p.txn.GetSnapshot(ts)
	snapshot.SetKeyOnly(true)
	snapshot.SetIsolationLevel(txnkv.RC)
	it, err := snapshot.Iter(start, end)
	if err != nil {
		return false, err
	}
	defer it.Close()
	return it.Valid(), nil
}
func (p *liveSourceRangeProbe) Close() { _ = p.txn.Close(); p.pd.Close() }

func (r SourceRangeExclusiveReceipt) Validate() error {
	if r.Format != SourceRangeExclusiveFormat || !r.ReadOnly || r.ClusterID == 0 || r.SnapshotTS == 0 || r.CheckedAtUnix <= 0 {
		return errors.New("source is not a read-only range-exclusive v1 receipt")
	}
	if !sha256RE.MatchString(r.FullSnapshotReceiptSHA256) || r.OutsideVisibleKeyCount != 0 || r.HistoricalMVCCAbsenceProven {
		return errors.New("source receipt does not prove bounded visible transactional keys")
	}
	ks, err := coder.NewKeyspace(r.Keyspace)
	if err != nil || r.StartKeyHex != hex.EncodeToString(ks.ObjectKeyspaceStart()) || r.EndKeyHex != hex.EncodeToString(ks.ObjectKeyspaceEnd()) {
		return errors.New("source range-exclusive receipt range does not match keyspace")
	}
	if len(r.PDAddrs) == 0 {
		return errors.New("source range-exclusive receipt has no PD addresses")
	}
	for i, addr := range r.PDAddrs {
		if !validTargetEndpoint(addr) || (i > 0 && r.PDAddrs[i-1] >= addr) {
			return errors.New("source range-exclusive receipt has invalid or unsorted PD addresses")
		}
	}
	return nil
}

func DecodeSourceRangeExclusive(r io.Reader) (SourceRangeExclusiveReceipt, error) {
	var receipt SourceRangeExclusiveReceipt
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode source range-exclusive receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("source range-exclusive receipt contains trailing JSON")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

// InspectLiveSourceRangeExclusive proves that no committed transactional key
// visible at the full backup TSO lies outside the KubeBrain keyspace range.
func InspectLiveSourceRangeExclusive(ctx context.Context, full FullSnapshotReceipt, fullSHA string, addrs []string, ca, cert, key string, checkedAt int64) (SourceRangeExclusiveReceipt, error) {
	pdc, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{CAPath: ca, CertPath: cert, KeyPath: key})
	if err != nil {
		return SourceRangeExclusiveReceipt{}, fmt.Errorf("connect source PD: %w", err)
	}
	tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) { c.Security = tikvcfg.NewSecurity(ca, cert, key, nil) })
	txn, err := txnkv.NewClient(addrs)
	if err != nil {
		pdc.Close()
		return SourceRangeExclusiveReceipt{}, fmt.Errorf("connect source TiKV: %w", err)
	}
	return InspectSourceRangeExclusive(ctx, &liveSourceRangeProbe{pd: pdc, txn: txn}, full, fullSHA, addrs, checkedAt)
}
