package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type expectedKV struct {
	key, value                  []byte
	createRevision, modRevision int64
	version, lease              int64
}

type restoreReceipt struct {
	Format           string `json:"format"`
	OperationID      string `json:"operation_id"`
	SourceReceiptSHA string `json:"source_receipt_sha256"`
	Target           struct {
		ClusterID string `json:"cluster_id"`
	} `json:"target"`
}

type snapshotReceipt struct {
	Format      string `json:"format"`
	OperationID string `json:"operation_id"`
	Witness     struct {
		Format     string `json:"format"`
		Prefix     string `json:"prefix"`
		Revision   int64  `json:"revision"`
		Records    int    `json:"records"`
		Leases     int    `json:"leases"`
		SHA256     string `json:"sha256"`
		FileSHA256 string `json:"file_sha256"`
	} `json:"semantic_witness"`
}

type semanticReceipt struct {
	Format                string `json:"format"`
	OperationID           string `json:"operation_id"`
	RestoreReceiptSHA256  string `json:"restore_receipt_sha256"`
	SnapshotReceiptSHA256 string `json:"snapshot_receipt_sha256"`
	WitnessFormat         string `json:"witness_format"`
	WitnessSHA256         string `json:"witness_sha256"`
	WitnessRevision       int64  `json:"witness_revision"`
	WitnessRecords        int    `json:"witness_records"`
	WitnessLeases         int    `json:"witness_leases"`
	RestoredClusterID     string `json:"restored_cluster_id"`
	HistoricalExact       bool   `json:"historical_exact"`
	CurrentExact          bool   `json:"current_exact"`
	LeaseIdentityExact    bool   `json:"lease_identity_exact"`
	WatchProbeSucceeded   bool   `json:"watch_probe_succeeded"`
	ProbePutRevision      int64  `json:"probe_put_revision"`
	ProbeDeleteRevision   int64  `json:"probe_delete_revision"`
	VerifiedAtUnix        int64  `json:"verified_at_unix"`
}

func main() {
	witnessPath := os.Getenv("WITNESS_FILE")
	snapshotReceiptPath := os.Getenv("SNAPSHOT_RECEIPT_FILE")
	restoreReceiptPath := os.Getenv("RESTORE_RECEIPT_FILE")
	output := os.Getenv("SEMANTIC_RECEIPT_FILE")
	probePrefix := os.Getenv("VERIFY_PREFIX")
	if witnessPath == "" || snapshotReceiptPath == "" || restoreReceiptPath == "" || output == "" || probePrefix == "" {
		fatal(errors.New("WITNESS_FILE, SNAPSHOT_RECEIPT_FILE, RESTORE_RECEIPT_FILE, SEMANTIC_RECEIPT_FILE and VERIFY_PREFIX are required"))
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		fatal(errors.New("SEMANTIC_RECEIPT_FILE must not already exist"))
	}

	verified, err := backupfile.OpenVerified(witnessPath)
	if err != nil {
		fatal(fmt.Errorf("verify witness artifact: %w", err))
	}
	defer verified.Close()
	status := verified.Status()
	expected, leaseSpecs, leaseKeys, err := loadWitness(verified)
	if err != nil {
		fatal(err)
	}
	for id, lease := range leaseSpecs {
		if lease.GrantedTTL <= 0 {
			fatal(fmt.Errorf("witness lease %d lacks granted_ttl; re-export with the current logical exporter", id))
		}
	}
	witnessData, err := os.ReadFile(witnessPath)
	if err != nil {
		fatal(err)
	}
	snapshotData, err := os.ReadFile(snapshotReceiptPath)
	if err != nil {
		fatal(err)
	}
	restoreData, err := os.ReadFile(restoreReceiptPath)
	if err != nil {
		fatal(err)
	}
	_, restore, err := validateReceiptChain(status, witnessData, snapshotData, restoreData)
	if err != nil {
		fatal(err)
	}

	cli, err := etcdutil.NewClientFromEnv()
	if err != nil {
		fatal(err)
	}
	defer cli.Close()
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	historical, historicalRevision, err := fetchRange(ctx, cli, status.Prefix, status.Revision)
	if err != nil {
		fatal(fmt.Errorf("read witness revision %d: %w", status.Revision, err))
	}
	if err := compareKVs(expected, historical); err != nil {
		fatal(fmt.Errorf("historical witness mismatch: %w", err))
	}
	current, currentRevision, err := fetchRange(ctx, cli, status.Prefix, 0)
	if err != nil {
		fatal(fmt.Errorf("read current range: %w", err))
	}
	if currentRevision < status.Revision || historicalRevision < status.Revision {
		fatal(fmt.Errorf("restored revision moved backwards: witness=%d historical=%d current=%d", status.Revision, historicalRevision, currentRevision))
	}
	if err := compareKVs(expected, current); err != nil {
		fatal(fmt.Errorf("current witness mismatch: %w", err))
	}
	if err := verifyLeases(ctx, cli, leaseSpecs, leaseKeys); err != nil {
		fatal(err)
	}
	putRevision, deleteRevision, err := runWatchProbe(ctx, cli, probePrefix)
	if err != nil {
		fatal(err)
	}

	receipt := semanticReceipt{
		Format: "kubebrain.cold-physical-semantic-verify.v1", OperationID: restore.OperationID,
		RestoreReceiptSHA256: digest(restoreData), SnapshotReceiptSHA256: digest(snapshotData),
		WitnessFormat: status.Format, WitnessSHA256: status.SHA256,
		WitnessRevision: status.Revision, WitnessRecords: status.Records, WitnessLeases: status.Leases,
		RestoredClusterID: restore.Target.ClusterID, HistoricalExact: true, CurrentExact: true,
		LeaseIdentityExact: true, WatchProbeSucceeded: true, ProbePutRevision: putRevision,
		ProbeDeleteRevision: deleteRevision, VerifiedAtUnix: time.Now().UTC().Unix(),
	}
	if err := writeAtomic(output, receipt); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "verified cold restore witness: records=%d leases=%d revision=%d probe=%d/%d\n",
		status.Records, status.Leases, status.Revision, putRevision, deleteRevision)
}

func validateReceiptChain(status backupfile.Status, witnessData, snapshotData, restoreData []byte) (snapshotReceipt, restoreReceipt, error) {
	var snapshotRecord snapshotReceipt
	if err := json.Unmarshal(snapshotData, &snapshotRecord); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, fmt.Errorf("decode snapshot receipt: %w", err)
	}
	if snapshotRecord.Format != "kubebrain.cold-physical-snapshot.v2" || snapshotRecord.OperationID == "" ||
		snapshotRecord.Witness.Format != status.Format || snapshotRecord.Witness.Prefix != status.Prefix ||
		snapshotRecord.Witness.Revision != status.Revision || snapshotRecord.Witness.Records != status.Records ||
		snapshotRecord.Witness.Leases != status.Leases || snapshotRecord.Witness.SHA256 != status.SHA256 ||
		snapshotRecord.Witness.FileSHA256 != digest(witnessData) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt semantic witness binding mismatch")
	}
	var restore restoreReceipt
	if err := json.Unmarshal(restoreData, &restore); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, fmt.Errorf("decode restore receipt: %w", err)
	}
	if restore.Format != "kubebrain.cold-physical-restore.v1" || restore.OperationID == "" || restore.Target.ClusterID == "" ||
		restore.OperationID != snapshotRecord.OperationID || restore.SourceReceiptSHA != digest(snapshotData) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("cold physical restore receipt does not bind the snapshot receipt")
	}
	return snapshotRecord, restore, nil
}

func loadWitness(verified *backupfile.Verified) (map[string]expectedKV, map[int64]record.Lease, map[int64][]string, error) {
	expected := make(map[string]expectedKV)
	leases := make(map[int64]record.Lease)
	leaseKeys := make(map[int64][]string)
	if err := verified.Leases(func(lease record.Lease) error {
		leases[lease.ID] = lease
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}
	if err := verified.Records(func(rec record.Record) error {
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			return err
		}
		value, err := base64.StdEncoding.DecodeString(rec.Value)
		if err != nil {
			return err
		}
		expected[string(key)] = expectedKV{key: key, value: value, createRevision: rec.CreateRevision, modRevision: rec.ModRevision, version: rec.Version, lease: rec.Lease}
		if rec.Lease != 0 {
			leaseKeys[rec.Lease] = append(leaseKeys[rec.Lease], string(key))
		}
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}
	for id := range leaseKeys {
		sort.Strings(leaseKeys[id])
	}
	return expected, leases, leaseKeys, nil
}

func fetchRange(ctx context.Context, cli *clientv3.Client, prefix string, revision int64) ([]*mvccpb.KeyValue, int64, error) {
	start := []byte(prefix)
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	var result []*mvccpb.KeyValue
	var headerRevision int64
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(string(end)), clientv3.WithLimit(1000)}
		if revision > 0 {
			opts = append(opts, clientv3.WithRev(revision))
		}
		response, err := cli.Get(ctx, string(start), opts...)
		if err != nil {
			return nil, 0, err
		}
		if response.Header != nil && response.Header.Revision > headerRevision {
			headerRevision = response.Header.Revision
		}
		result = append(result, response.Kvs...)
		if !response.More || len(response.Kvs) == 0 {
			return result, headerRevision, nil
		}
		start = append(append([]byte(nil), response.Kvs[len(response.Kvs)-1].Key...), 0)
	}
}

func compareKVs(expected map[string]expectedKV, actual []*mvccpb.KeyValue) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("record count: expected %d, got %d", len(expected), len(actual))
	}
	seen := make(map[string]struct{}, len(actual))
	for _, got := range actual {
		want, exists := expected[string(got.Key)]
		if !exists {
			return fmt.Errorf("unexpected key %q", got.Key)
		}
		if _, duplicate := seen[string(got.Key)]; duplicate {
			return fmt.Errorf("duplicate key %q", got.Key)
		}
		seen[string(got.Key)] = struct{}{}
		if !bytes.Equal(want.value, got.Value) || want.createRevision != got.CreateRevision || want.modRevision != got.ModRevision || want.version != got.Version || want.lease != got.Lease {
			return fmt.Errorf("metadata/value mismatch for key %q", got.Key)
		}
	}
	return nil
}

func verifyLeases(ctx context.Context, cli *clientv3.Client, leases map[int64]record.Lease, expectedKeys map[int64][]string) error {
	for id, expected := range leases {
		response, err := cli.TimeToLive(ctx, clientv3.LeaseID(id), clientv3.WithAttachedKeys())
		if err != nil {
			return fmt.Errorf("read physical lease %d: %w", id, err)
		}
		if response.ID != clientv3.LeaseID(id) || response.TTL <= 0 || response.GrantedTTL != expected.GrantedTTL {
			return fmt.Errorf("physical lease identity/TTL mismatch for %d", id)
		}
		keys := make([]string, len(response.Keys))
		for i := range response.Keys {
			keys[i] = string(response.Keys[i])
		}
		sort.Strings(keys)
		if !equalStrings(expectedKeys[id], keys) {
			return fmt.Errorf("physical lease attached keys mismatch for %d", id)
		}
	}
	return nil
}

func runWatchProbe(ctx context.Context, cli *clientv3.Client, prefix string) (int64, int64, error) {
	key := fmt.Sprintf("%s/%d", bytes.TrimRight([]byte(prefix), "/"), time.Now().UTC().UnixNano())
	watch := cli.Watch(ctx, key, clientv3.WithCreatedNotify())
	created, ok := <-watch
	if !ok || created.Err() != nil || !created.Created {
		return 0, 0, errors.New("watch did not acknowledge creation")
	}
	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		return 0, 0, fmt.Errorf("grant probe lease: %w", err)
	}
	revoked := false
	defer func() {
		if revoked {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
	}()
	put, err := cli.Put(ctx, key, "cold-restore-semantic-probe", clientv3.WithLease(lease.ID))
	if err != nil || put.Header == nil || put.Header.Revision <= 0 {
		return 0, 0, fmt.Errorf("put watch probe: %w", err)
	}
	if err := expectWatchEvent(watch, mvccpb.PUT, []byte(key), put.Header.Revision); err != nil {
		return 0, 0, err
	}
	read, err := cli.Get(ctx, key)
	if err != nil || len(read.Kvs) != 1 || read.Kvs[0].Lease != int64(lease.ID) {
		return 0, 0, errors.New("linearizable probe read mismatch")
	}
	deleted, err := cli.Delete(ctx, key)
	if err != nil || deleted.Header == nil || deleted.Deleted != 1 {
		return 0, 0, fmt.Errorf("delete watch probe: %w", err)
	}
	if err := expectWatchEvent(watch, mvccpb.DELETE, []byte(key), deleted.Header.Revision); err != nil {
		return 0, 0, err
	}
	if _, err := cli.Revoke(ctx, lease.ID); err != nil {
		return 0, 0, err
	}
	revoked = true
	return put.Header.Revision, deleted.Header.Revision, nil
}

func expectWatchEvent(watch clientv3.WatchChan, eventType mvccpb.Event_EventType, key []byte, revision int64) error {
	for response := range watch {
		if err := response.Err(); err != nil {
			return err
		}
		for _, event := range response.Events {
			if event.Type == eventType && bytes.Equal(event.Kv.Key, key) && event.Kv.ModRevision == revision {
				return nil
			}
		}
	}
	return errors.New("watch closed before expected event")
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func writeAtomic(path string, value any) error {
	directory := filepath.Dir(path)
	tmp, err := os.CreateTemp(directory, ".cold-semantic-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := json.NewEncoder(tmp).Encode(value); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
