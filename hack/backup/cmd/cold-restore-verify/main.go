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
	"io"
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

const maxColdRestoreJSONBytes = 4 << 20

type expectedKV struct {
	key, value                  []byte
	createRevision, modRevision int64
	version, lease              int64
}

type restoreManifestSnapshotMapping struct {
	component string
	handle    string
	name      string
}

type restoredVolumeSnapshotContent struct {
	Name           string `json:"name"`
	UID            string `json:"uid"`
	Driver         string `json:"driver"`
	SnapshotHandle string `json:"snapshot_handle"`
}

type restoredPVC struct {
	Name  string `json:"name"`
	UID   string `json:"uid"`
	PV    string `json:"pv"`
	Phase string `json:"phase"`
}

type restoreReceipt struct {
	Format           string `json:"format"`
	OperationID      string `json:"operation_id"`
	SourceReceiptSHA string `json:"source_receipt_sha256"`
	RestoreManifest  struct {
		Format                 string `json:"format"`
		SHA256                 string `json:"sha256"`
		ItemCount              int    `json:"item_count"`
		VolumeSnapshotContents int    `json:"volume_snapshot_contents"`
		VolumeSnapshots        int    `json:"volume_snapshots"`
		PersistentVolumeClaims int    `json:"persistent_volume_claims"`
		TidbClusters           int    `json:"tidbclusters"`
	} `json:"restore_manifest"`
	Target struct {
		KubeSystemUID  string `json:"kube_system_uid"`
		NamespaceUID   string `json:"namespace_uid"`
		Namespace      string `json:"namespace"`
		TidbCluster    string `json:"tidb_cluster"`
		TidbClusterUID string `json:"tidb_cluster_uid"`
		ClusterID      string `json:"cluster_id"`
	} `json:"target"`
	VolumeSnapshotContents []restoredVolumeSnapshotContent `json:"volume_snapshot_contents"`
	PVCs                   []restoredPVC                   `json:"pvcs"`
}

type snapshotReceipt struct {
	Format      string `json:"format"`
	OperationID string `json:"operation_id"`
	CreatedAt   string `json:"created_at"`
	Inventory   struct {
		Format              string `json:"format"`
		VolumeSnapshotClass struct {
			Name           string `json:"name"`
			Driver         string `json:"driver"`
			DeletionPolicy string `json:"deletion_policy"`
		} `json:"volume_snapshot_class"`
		KubeBrain struct {
			Namespace   string `json:"namespace"`
			StatefulSet string `json:"statefulset"`
			UID         string `json:"uid"`
		} `json:"kubebrain"`
		Storage struct {
			Namespace   string `json:"namespace"`
			TidbCluster string `json:"tidb_cluster"`
			UID         string `json:"uid"`
			ClusterID   string `json:"cluster_id"`
		} `json:"storage"`
	} `json:"inventory"`
	Snapshots []struct {
		Name           string `json:"name"`
		UID            string `json:"uid"`
		Content        string `json:"content"`
		ContentUID     string `json:"content_uid"`
		SourcePVC      string `json:"source_pvc"`
		Component      string `json:"component"`
		SnapshotHandle string `json:"snapshot_handle"`
		RestoreSize    string `json:"restore_size"`
	} `json:"snapshots"`
	Witness struct {
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
	RestoreManifestSHA256 string `json:"restore_manifest_sha256"`
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
	restoreManifestPath := os.Getenv("RESTORE_MANIFEST_FILE")
	output := os.Getenv("SEMANTIC_RECEIPT_FILE")
	probePrefix := os.Getenv("VERIFY_PREFIX")
	if witnessPath == "" || snapshotReceiptPath == "" || restoreReceiptPath == "" ||
		restoreManifestPath == "" || output == "" || probePrefix == "" {
		fatal(errors.New("WITNESS_FILE, SNAPSHOT_RECEIPT_FILE, RESTORE_RECEIPT_FILE, RESTORE_MANIFEST_FILE, SEMANTIC_RECEIPT_FILE and VERIFY_PREFIX are required"))
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
	snapshotData, err := readBoundedJSONFile(snapshotReceiptPath, "snapshot receipt")
	if err != nil {
		fatal(err)
	}
	restoreData, err := readBoundedJSONFile(restoreReceiptPath, "restore receipt")
	if err != nil {
		fatal(err)
	}
	restoreManifestData, err := readBoundedJSONFile(restoreManifestPath, "restore manifest")
	if err != nil {
		fatal(err)
	}
	snapshotRecord, restore, err := validateReceiptChain(status, witnessData, snapshotData, restoreData)
	if err != nil {
		fatal(err)
	}
	if err := validateRestoreManifestBinding(restoreManifestData, restore, snapshotRecord); err != nil {
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
		RestoreManifestSHA256: restore.RestoreManifest.SHA256,
		RestoredClusterID:     restore.Target.ClusterID, HistoricalExact: true, CurrentExact: true,
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
	if err := decodeStrictJSON(snapshotData, &snapshotRecord, "snapshot receipt"); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, err
	}
	if _, err := time.Parse(time.RFC3339, snapshotRecord.CreatedAt); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt created_at is invalid")
	}
	if snapshotRecord.Inventory.Format != "kubebrain.cold-physical-snapshot-preflight.v2" {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt inventory format is invalid")
	}
	if snapshotRecord.Format != "kubebrain.cold-physical-snapshot.v2" || snapshotRecord.OperationID == "" ||
		snapshotRecord.Witness.Format != status.Format || snapshotRecord.Witness.Prefix != status.Prefix ||
		snapshotRecord.Witness.Revision != status.Revision || snapshotRecord.Witness.Records != status.Records ||
		snapshotRecord.Witness.Leases != status.Leases || snapshotRecord.Witness.SHA256 != status.SHA256 ||
		snapshotRecord.Witness.FileSHA256 != digest(witnessData) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt semantic witness binding mismatch")
	}
	var restore restoreReceipt
	if err := decodeStrictJSON(restoreData, &restore, "restore receipt"); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, err
	}
	if restore.Format != "kubebrain.cold-physical-restore.v1" || restore.OperationID == "" || restore.Target.ClusterID == "" ||
		restore.OperationID != snapshotRecord.OperationID || restore.SourceReceiptSHA != digest(snapshotData) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("cold physical restore receipt does not bind the snapshot receipt")
	}
	if err := validateRestoreReceiptInventory(restore, snapshotRecord); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, err
	}
	if restore.RestoreManifest.Format != "kubernetes-list.canonical-json.v1" ||
		!validDigest(restore.RestoreManifest.SHA256) ||
		restore.RestoreManifest.ItemCount <= 0 ||
		restore.RestoreManifest.VolumeSnapshotContents <= 0 ||
		restore.RestoreManifest.VolumeSnapshotContents != restore.RestoreManifest.VolumeSnapshots ||
		restore.RestoreManifest.VolumeSnapshotContents != restore.RestoreManifest.PersistentVolumeClaims ||
		restore.RestoreManifest.TidbClusters != 1 ||
		restore.RestoreManifest.ItemCount != restore.RestoreManifest.VolumeSnapshotContents*3+1 ||
		len(restore.VolumeSnapshotContents) != restore.RestoreManifest.VolumeSnapshotContents ||
		len(restore.PVCs) != restore.RestoreManifest.PersistentVolumeClaims {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("cold physical restore receipt does not bind a canonical restore manifest")
	}
	return snapshotRecord, restore, nil
}

func decodeStrictJSON(data []byte, target any, description string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", description, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%s contains trailing JSON", description)
	}
	return nil
}

func readBoundedJSONFile(path, description string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxColdRestoreJSONBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxColdRestoreJSONBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, maxColdRestoreJSONBytes)
	}
	return data, nil
}

func validateRestoreReceiptInventory(restore restoreReceipt, snapshotRecord snapshotReceipt) error {
	if snapshotRecord.Inventory.Storage.Namespace == "" ||
		snapshotRecord.Inventory.Storage.TidbCluster == "" ||
		snapshotRecord.Inventory.Storage.ClusterID == "" ||
		snapshotRecord.Inventory.VolumeSnapshotClass.Driver == "" ||
		restore.Target.KubeSystemUID == "" || restore.Target.NamespaceUID == "" ||
		restore.Target.TidbClusterUID == "" ||
		restore.Target.Namespace != snapshotRecord.Inventory.Storage.Namespace ||
		restore.Target.TidbCluster != snapshotRecord.Inventory.Storage.TidbCluster ||
		restore.Target.ClusterID != snapshotRecord.Inventory.Storage.ClusterID {
		return errors.New("cold physical restore receipt target does not match the snapshot receipt")
	}
	expected := make(map[string]string, len(snapshotRecord.Snapshots))
	expectedPVCs := make(map[string]struct{}, len(snapshotRecord.Snapshots))
	for _, snapshot := range snapshotRecord.Snapshots {
		if snapshot.SourcePVC == "" || snapshot.SnapshotHandle == "" {
			return errors.New("snapshot receipt contains incomplete restored inventory mapping")
		}
		if _, exists := expected[snapshot.SnapshotHandle]; exists {
			return errors.New("snapshot receipt contains duplicate snapshot handle")
		}
		expected[snapshot.SnapshotHandle] = restoreManifestObjectName(
			snapshotRecord.OperationID, snapshot.SourcePVC,
		)
		expectedPVCs[snapshot.SourcePVC] = struct{}{}
	}
	if len(restore.VolumeSnapshotContents) != len(expected) || len(restore.PVCs) != len(expectedPVCs) {
		return errors.New("cold physical restore receipt inventory count does not match snapshot receipt")
	}
	seenContentNames := map[string]struct{}{}
	seenHandles := map[string]struct{}{}
	for _, content := range restore.VolumeSnapshotContents {
		expectedName, exists := expected[content.SnapshotHandle]
		if !exists || content.Name != expectedName || content.UID == "" ||
			content.Driver != snapshotRecord.Inventory.VolumeSnapshotClass.Driver {
			return errors.New("cold physical restore receipt content inventory does not match snapshot receipt")
		}
		if _, duplicate := seenContentNames[content.Name]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate VolumeSnapshotContent")
		}
		if _, duplicate := seenHandles[content.SnapshotHandle]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate snapshot handle")
		}
		seenContentNames[content.Name] = struct{}{}
		seenHandles[content.SnapshotHandle] = struct{}{}
	}
	seenPVCNames := map[string]struct{}{}
	seenPVs := map[string]struct{}{}
	for _, pvc := range restore.PVCs {
		if _, exists := expectedPVCs[pvc.Name]; !exists || pvc.UID == "" || pvc.PV == "" ||
			pvc.Phase != "Bound" {
			return errors.New("cold physical restore receipt PVC inventory does not match snapshot receipt")
		}
		if _, duplicate := seenPVCNames[pvc.Name]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PVC")
		}
		if _, duplicate := seenPVs[pvc.PV]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PV")
		}
		seenPVCNames[pvc.Name] = struct{}{}
		seenPVs[pvc.PV] = struct{}{}
	}
	return nil
}

func validateRestoreManifestBinding(data []byte, restore restoreReceipt, snapshotRecord snapshotReceipt) error {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode restore manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("restore manifest contains trailing JSON")
	}
	manifest, ok := value.(map[string]any)
	if !ok || manifest["apiVersion"] != "v1" || manifest["kind"] != "List" {
		return errors.New("restore manifest is not a Kubernetes List")
	}
	items, ok := manifest["items"].([]any)
	if !ok {
		return errors.New("restore manifest items are invalid")
	}
	counts := map[string]int{}
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			return errors.New("restore manifest contains a non-object item")
		}
		kind, ok := item["kind"].(string)
		if !ok || kind == "" {
			return errors.New("restore manifest item lacks kind")
		}
		counts[kind]++
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if digest(canonical) != restore.RestoreManifest.SHA256 ||
		len(items) != restore.RestoreManifest.ItemCount ||
		counts["VolumeSnapshotContent"] != restore.RestoreManifest.VolumeSnapshotContents ||
		counts["VolumeSnapshot"] != restore.RestoreManifest.VolumeSnapshots ||
		counts["PersistentVolumeClaim"] != restore.RestoreManifest.PersistentVolumeClaims ||
		counts["TidbCluster"] != restore.RestoreManifest.TidbClusters {
		return errors.New("restore manifest does not match restore receipt binding")
	}
	return validateRestoreManifestContent(items, snapshotRecord)
}

func validateRestoreManifestContent(items []any, snapshotRecord snapshotReceipt) error {
	if snapshotRecord.OperationID == "" || snapshotRecord.Inventory.Storage.Namespace == "" ||
		snapshotRecord.Inventory.Storage.TidbCluster == "" ||
		snapshotRecord.Inventory.Storage.UID == "" ||
		snapshotRecord.Inventory.Storage.ClusterID == "" ||
		snapshotRecord.Inventory.VolumeSnapshotClass.Driver == "" ||
		len(snapshotRecord.Snapshots) == 0 {
		return errors.New("snapshot receipt lacks restore manifest identity")
	}
	expected := make(map[string]restoreManifestSnapshotMapping, len(snapshotRecord.Snapshots))
	for _, snapshot := range snapshotRecord.Snapshots {
		if snapshot.SourcePVC == "" || snapshot.Component == "" || snapshot.SnapshotHandle == "" {
			return errors.New("snapshot receipt contains incomplete snapshot mapping")
		}
		if _, exists := expected[snapshot.SourcePVC]; exists {
			return errors.New("snapshot receipt contains duplicate source PVC")
		}
		expected[snapshot.SourcePVC] = restoreManifestSnapshotMapping{
			component: snapshot.Component,
			handle:    snapshot.SnapshotHandle,
			name:      restoreManifestObjectName(snapshotRecord.OperationID, snapshot.SourcePVC),
		}
	}
	seenVSC := map[string]struct{}{}
	seenVS := map[string]struct{}{}
	seenPVC := map[string]struct{}{}
	vscItems := 0
	vsItems := 0
	pvcItems := 0
	tidbClusters := 0
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			return errors.New("restore manifest contains a non-object item")
		}
		kind, _ := item["kind"].(string)
		metadata, _ := item["metadata"].(map[string]any)
		spec, _ := item["spec"].(map[string]any)
		switch kind {
		case "VolumeSnapshotContent":
			vscItems++
			sourcePVC, mapping, err := restoreManifestSourceFromName(metadata, expected)
			if err != nil {
				return err
			}
			if spec["deletionPolicy"] != "Retain" ||
				spec["driver"] != snapshotRecord.Inventory.VolumeSnapshotClass.Driver ||
				nestedString(spec, "source", "snapshotHandle") != mapping.handle ||
				nestedString(spec, "volumeSnapshotRef", "name") != mapping.name ||
				nestedString(spec, "volumeSnapshotRef", "namespace") != snapshotRecord.Inventory.Storage.Namespace {
				return errors.New("restore manifest VolumeSnapshotContent does not match snapshot receipt")
			}
			seenVSC[sourcePVC] = struct{}{}
		case "VolumeSnapshot":
			vsItems++
			sourcePVC, mapping, err := restoreManifestSourceFromName(metadata, expected)
			if err != nil {
				return err
			}
			if metadata["namespace"] != snapshotRecord.Inventory.Storage.Namespace ||
				nestedString(spec, "source", "volumeSnapshotContentName") != mapping.name {
				return errors.New("restore manifest VolumeSnapshot does not match snapshot receipt")
			}
			seenVS[sourcePVC] = struct{}{}
		case "PersistentVolumeClaim":
			pvcItems++
			name, _ := metadata["name"].(string)
			mapping, exists := expected[name]
			if !exists {
				return errors.New("restore manifest PVC does not match snapshot receipt")
			}
			labels, _ := metadata["labels"].(map[string]any)
			if metadata["namespace"] != snapshotRecord.Inventory.Storage.Namespace ||
				labels["app.kubernetes.io/component"] != mapping.component ||
				nestedString(spec, "dataSource", "name") != mapping.name ||
				nestedString(spec, "dataSource", "kind") != "VolumeSnapshot" {
				return errors.New("restore manifest PVC does not match snapshot receipt")
			}
			seenPVC[name] = struct{}{}
		case "TidbCluster":
			tidbClusters++
			annotations, _ := metadata["annotations"].(map[string]any)
			if metadata["name"] != snapshotRecord.Inventory.Storage.TidbCluster ||
				metadata["namespace"] != snapshotRecord.Inventory.Storage.Namespace ||
				annotations["kubebrain.io/source-cluster-id"] != snapshotRecord.Inventory.Storage.ClusterID ||
				annotations["kubebrain.io/source-tidbcluster-uid"] != snapshotRecord.Inventory.Storage.UID ||
				spec["paused"] != true {
				return errors.New("restore manifest TidbCluster does not match snapshot receipt")
			}
		}
	}
	if tidbClusters != 1 || vscItems != len(expected) || vsItems != len(expected) ||
		pvcItems != len(expected) || len(seenVSC) != len(expected) ||
		len(seenVS) != len(expected) || len(seenPVC) != len(expected) {
		return errors.New("restore manifest does not cover every snapshot receipt PVC")
	}
	return nil
}

func restoreManifestSourceFromName(metadata map[string]any, expected map[string]restoreManifestSnapshotMapping) (string, restoreManifestSnapshotMapping, error) {
	name, _ := metadata["name"].(string)
	for sourcePVC, mapping := range expected {
		if name == mapping.name {
			return sourcePVC, mapping, nil
		}
	}
	return "", restoreManifestSnapshotMapping{}, errors.New("restore manifest snapshot object name does not match snapshot receipt")
}

func nestedString(parent map[string]any, first, second string) string {
	child, _ := parent[first].(map[string]any)
	value, _ := child[second].(string)
	return value
}

func restoreManifestObjectName(operation, pvcName string) string {
	sum := sha256.Sum256([]byte(operation + "\x00" + pvcName))
	prefix := bytes.Trim([]byte(operation), "-")
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}
	return "kb-restore-" + string(prefix) + "-" + hex.EncodeToString(sum[:6])
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

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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
