package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
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
