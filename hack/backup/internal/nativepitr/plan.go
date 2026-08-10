// Package nativepitr defines fail-closed artifact contracts for native TiKV
// transactional PITR. A Plan is not a backup receipt: it binds independently
// produced evidence before a restore operation is allowed to mutate a target.
package nativepitr

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
)

const (
	PreflightFormat = "kubebrain.native-pitr-preflight.v1"
	PlanFormat      = "kubebrain.native-pitr-restore-plan.v1"
)

var (
	dnsLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)
	sha256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Preflight struct {
	Format        string   `json:"format"`
	ClusterID     uint64   `json:"cluster_id"`
	PDAddrs       []string `json:"pd_addrs"`
	Keyspace      string   `json:"keyspace"`
	TaskName      string   `json:"task_name"`
	StartKeyHex   string   `json:"start_key_hex"`
	EndKeyHex     string   `json:"end_key_hex"`
	TaskInfoKey   string   `json:"task_info_key"`
	TaskRangesKey string   `json:"task_ranges_prefix"`
	OwnershipKeys []string `json:"ownership_paths_checked"`
	TaskAvailable bool     `json:"task_name_available"`
	TaskCount     int64    `json:"existing_task_count"`
	Stores        []struct {
		ID      uint64 `json:"id"`
		Address string `json:"address"`
		Service string `json:"log_backup_service"`
	} `json:"stores"`
	ReadOnly bool `json:"read_only"`
}

type Source struct {
	ClusterID   uint64 `json:"cluster_id"`
	Keyspace    string `json:"keyspace"`
	StartKeyHex string `json:"start_key_hex"`
	EndKeyHex   string `json:"end_key_hex"`
	TaskName    string `json:"task_name"`
}

type FullSnapshot struct {
	BackupTS         uint64 `json:"backup_ts"`
	BackupMetaSHA256 string `json:"backupmeta_sha256"`
	StoragePrefix    string `json:"storage_prefix"`
	Mode             string `json:"mode"`
}

type LogWindow struct {
	StartTS            uint64 `json:"start_ts"`
	GlobalCheckpointTS uint64 `json:"global_checkpoint_ts"`
	AdvancerOwner      string `json:"advancer_owner"`
}

type Target struct {
	ClusterID          uint64 `json:"cluster_id"`
	EmptyWitnessSHA256 string `json:"empty_witness_sha256"`
}

type Plan struct {
	Format    string       `json:"format"`
	Source    Source       `json:"source"`
	Full      FullSnapshot `json:"full_snapshot"`
	Log       LogWindow    `json:"log_window"`
	Target    Target       `json:"target"`
	RestoreTS uint64       `json:"restore_ts"`
	ReadOnly  bool         `json:"read_only"`
}

type Inputs struct {
	TaskStartTS        uint64
	FullBackupTS       uint64
	BackupMetaSHA256   string
	StoragePrefix      string
	GlobalCheckpointTS uint64
	AdvancerOwner      string
	TargetClusterID    uint64
	EmptyWitnessSHA256 string
	RestoreTS          uint64
}

func DecodePreflight(r io.Reader) (Preflight, error) {
	var p Preflight
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("decode source preflight: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return p, err
	}
	if err := validatePreflight(p); err != nil {
		return p, err
	}
	return p, nil
}

// ValidatePreflight verifies that a receipt proves the exact tenant range and
// every metadata ownership path needed before task creation.
func ValidatePreflight(p Preflight) error { return validatePreflight(p) }

func Build(p Preflight, in Inputs) (Plan, error) {
	if err := validatePreflight(p); err != nil {
		return Plan{}, err
	}
	plan := Plan{
		Format:    PlanFormat,
		Source:    Source{ClusterID: p.ClusterID, Keyspace: p.Keyspace, StartKeyHex: p.StartKeyHex, EndKeyHex: p.EndKeyHex, TaskName: p.TaskName},
		Full:      FullSnapshot{BackupTS: in.FullBackupTS, BackupMetaSHA256: in.BackupMetaSHA256, StoragePrefix: in.StoragePrefix, Mode: "br-txn"},
		Log:       LogWindow{StartTS: in.TaskStartTS, GlobalCheckpointTS: in.GlobalCheckpointTS, AdvancerOwner: in.AdvancerOwner},
		Target:    Target{ClusterID: in.TargetClusterID, EmptyWitnessSHA256: in.EmptyWitnessSHA256},
		RestoreTS: in.RestoreTS,
		ReadOnly:  true,
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (p Plan) Validate() error {
	if p.Format != PlanFormat {
		return fmt.Errorf("unsupported plan format %q", p.Format)
	}
	if p.Source.ClusterID == 0 || p.Target.ClusterID == 0 {
		return errors.New("source and target cluster IDs must be non-zero")
	}
	if p.Source.ClusterID == p.Target.ClusterID {
		return errors.New("target cluster must differ from source cluster")
	}
	if !dnsLabel.MatchString(p.Source.TaskName) {
		return errors.New("invalid task name")
	}
	ks, err := coder.NewKeyspace(p.Source.Keyspace)
	if err != nil {
		return fmt.Errorf("invalid source keyspace: %w", err)
	}
	start, err := hex.DecodeString(p.Source.StartKeyHex)
	if err != nil || len(start) == 0 {
		return errors.New("invalid source start key")
	}
	end, err := hex.DecodeString(p.Source.EndKeyHex)
	if err != nil || len(end) == 0 || bytes.Compare(start, end) >= 0 {
		return errors.New("invalid source key range")
	}
	if !bytes.Equal(start, ks.ObjectKeyspaceStart()) || !bytes.Equal(end, ks.ObjectKeyspaceEnd()) {
		return errors.New("source key range does not match keyspace")
	}
	if p.Log.StartTS == 0 || p.Full.BackupTS == 0 || p.RestoreTS == 0 || p.Log.GlobalCheckpointTS == 0 {
		return errors.New("all PITR timestamps must be non-zero")
	}
	if p.Log.StartTS > p.Full.BackupTS {
		return errors.New("log task must start no later than the full snapshot")
	}
	if p.Full.BackupTS > p.RestoreTS {
		return errors.New("restore timestamp precedes full snapshot")
	}
	if p.RestoreTS > p.Log.GlobalCheckpointTS {
		return errors.New("restore timestamp exceeds durable global checkpoint")
	}
	if p.Full.Mode != "br-txn" {
		return errors.New("full snapshot mode must be br-txn")
	}
	if !sha256RE.MatchString(p.Full.BackupMetaSHA256) {
		return errors.New("invalid backupmeta SHA-256")
	}
	if !sha256RE.MatchString(p.Target.EmptyWitnessSHA256) {
		return errors.New("invalid target empty-witness SHA-256")
	}
	if err := safeText("storage prefix", p.Full.StoragePrefix); err != nil {
		return err
	}
	if err := safeText("advancer owner", p.Log.AdvancerOwner); err != nil {
		return err
	}
	if !p.ReadOnly {
		return errors.New("restore plan must be marked read-only")
	}
	return nil
}

func validatePreflight(p Preflight) error {
	if p.Format != PreflightFormat || !p.ReadOnly {
		return errors.New("source is not a read-only native PITR preflight v1 receipt")
	}
	if p.ClusterID == 0 || !p.TaskAvailable || p.TaskCount != 0 || len(p.Stores) == 0 {
		return errors.New("source preflight did not prove an available task and TiKV stores")
	}
	if !dnsLabel.MatchString(p.TaskName) {
		return errors.New("source preflight has invalid task name")
	}
	ks, err := coder.NewKeyspace(p.Keyspace)
	if err != nil {
		return fmt.Errorf("source preflight keyspace: %w", err)
	}
	wantStart, wantEnd := hex.EncodeToString(ks.ObjectKeyspaceStart()), hex.EncodeToString(ks.ObjectKeyspaceEnd())
	if p.StartKeyHex != wantStart || p.EndKeyHex != wantEnd {
		return errors.New("source preflight range does not match its KubeBrain keyspace")
	}
	wantInfo := "/tidb/br-stream/info/" + p.TaskName
	wantRanges := "/tidb/br-stream/ranges/" + p.TaskName + "/"
	if p.TaskInfoKey != wantInfo || p.TaskRangesKey != wantRanges {
		return errors.New("source preflight task metadata paths do not match task name")
	}
	wantOwnership := []string{wantInfo, wantRanges, "/tidb/br-stream/checkpoint/" + p.TaskName + "/", "/tidb/br-stream/storage-checkpoint/" + p.TaskName + "/", "/tidb/br-stream/pause/" + p.TaskName, "/tidb/br-stream/last-error/" + p.TaskName + "/"}
	if len(p.OwnershipKeys) != len(wantOwnership) {
		return errors.New("source preflight did not check every task ownership path")
	}
	for i := range wantOwnership {
		if p.OwnershipKeys[i] != wantOwnership[i] {
			return errors.New("source preflight ownership paths do not match task name")
		}
	}
	if len(p.PDAddrs) == 0 {
		return errors.New("source preflight contains no PD addresses")
	}
	seenPD := map[string]bool{}
	for _, addr := range p.PDAddrs {
		if addr == "" || seenPD[addr] {
			return errors.New("source preflight contains invalid PD addresses")
		}
		seenPD[addr] = true
	}
	seenStore := map[uint64]bool{}
	for _, s := range p.Stores {
		if s.ID == 0 || seenStore[s.ID] || s.Address == "" || s.Service != "available" {
			return errors.New("source preflight contains an unproven TiKV log-backup service")
		}
		seenStore[s.ID] = true
	}
	start, e1 := hex.DecodeString(p.StartKeyHex)
	end, e2 := hex.DecodeString(p.EndKeyHex)
	if e1 != nil || e2 != nil || len(start) == 0 || bytes.Compare(start, end) >= 0 {
		return errors.New("source preflight contains an invalid key range")
	}
	return nil
}

func safeText(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t") {
		return fmt.Errorf("invalid %s", name)
	}
	return nil
}

func requireEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	}
	return errors.New("source preflight contains trailing JSON")
}
