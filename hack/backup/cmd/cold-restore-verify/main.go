package main

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

const maxColdRestoreJSONBytes = 4 << 20

var afterWitnessDigestForTest = func() {}
var afterStableJSONReadForTest = func(string) {}

type expectedKV struct {
	key, value                  []byte
	createRevision, modRevision int64
	version, lease              int64
}

type restoreManifestSnapshotMapping struct {
	component        string
	handle           string
	name             string
	volumeMode       string
	accessModes      []string
	requestedStorage string
}

type restoredVolumeSnapshotContent struct {
	Name           string              `json:"name"`
	UID            string              `json:"uid"`
	Driver         string              `json:"driver"`
	SnapshotClass  string              `json:"snapshot_class"`
	DeletionPolicy string              `json:"deletion_policy"`
	SnapshotHandle string              `json:"snapshot_handle"`
	SnapshotRef    restoredSnapshotRef `json:"snapshot_ref"`
}

type restoredSnapshotRef struct {
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
}

type restoredVolumeSnapshot struct {
	Name          string `json:"name"`
	UID           string `json:"uid"`
	SnapshotClass string `json:"snapshot_class"`
	SourceContent string `json:"source_content"`
	BoundContent  string `json:"bound_content"`
	Ready         bool   `json:"ready"`
}

type restoredDataSource struct {
	APIGroup string `json:"api_group"`
	Kind     string `json:"kind"`
	Name     string `json:"name"`
}

type restoredPVC struct {
	Name             string             `json:"name"`
	UID              string             `json:"uid"`
	PV               string             `json:"pv"`
	Phase            string             `json:"phase"`
	StorageClass     string             `json:"storage_class"`
	VolumeMode       string             `json:"volume_mode"`
	AccessModes      []string           `json:"access_modes"`
	RequestedStorage string             `json:"requested_storage"`
	DataSource       restoredDataSource `json:"data_source"`
}

type restoredClaimRef struct {
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

type restoredPV struct {
	Name         string           `json:"name"`
	UID          string           `json:"uid"`
	Phase        string           `json:"phase"`
	StorageClass string           `json:"storage_class"`
	VolumeMode   string           `json:"volume_mode"`
	Capacity     string           `json:"capacity"`
	ClaimRef     restoredClaimRef `json:"claim_ref"`
	CSIDriver    string           `json:"csi_driver"`
	VolumeHandle string           `json:"volume_handle"`
}

type sourcePVC struct {
	Name             string            `json:"name"`
	UID              string            `json:"uid"`
	PV               string            `json:"pv"`
	PVUID            string            `json:"pv_uid"`
	CSIDriver        string            `json:"csi_driver"`
	VolumeHandle     string            `json:"volume_handle"`
	Labels           map[string]string `json:"labels"`
	VolumeMode       string            `json:"volume_mode"`
	AccessModes      []string          `json:"access_modes"`
	StorageClass     string            `json:"storage_class"`
	RequestedStorage string            `json:"requested_storage"`
	Phase            string            `json:"phase"`
}

type recoveryTidbCluster struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   recoveryMetadata `json:"metadata"`
	Spec       json.RawMessage  `json:"spec"`
}

type recoveryMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type snapshotInventory struct {
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
	RecoveryBlueprint struct {
		TidbCluster recoveryTidbCluster `json:"tidbcluster"`
	} `json:"recovery_blueprint"`
	PDPVCs   []sourcePVC `json:"pd_pvcs"`
	TiKVPVCs []sourcePVC `json:"tikv_pvcs"`
}

type sourceSnapshot struct {
	Name           string `json:"name"`
	UID            string `json:"uid"`
	Content        string `json:"content"`
	ContentUID     string `json:"content_uid"`
	SourcePVC      string `json:"source_pvc"`
	Component      string `json:"component"`
	SnapshotHandle string `json:"snapshot_handle"`
	RestoreSize    string `json:"restore_size"`
}

type restoreReceipt struct {
	Format           string `json:"format"`
	OperationID      string `json:"operation_id"`
	SourceReceiptSHA string `json:"source_receipt_sha256"`
	CompletedAt      string `json:"completed_at"`
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
	VolumeSnapshots        []restoredVolumeSnapshot        `json:"volume_snapshots"`
	VolumeSnapshotContents []restoredVolumeSnapshotContent `json:"volume_snapshot_contents"`
	PVs                    []restoredPV                    `json:"pvs"`
	PVCs                   []restoredPVC                   `json:"pvcs"`
}

type snapshotReceipt struct {
	Format      string            `json:"format"`
	OperationID string            `json:"operation_id"`
	CreatedAt   string            `json:"created_at"`
	Inventory   snapshotInventory `json:"inventory"`
	Snapshots   []sourceSnapshot  `json:"snapshots"`
	Witness     struct {
		Format        string `json:"format"`
		Prefix        string `json:"prefix"`
		Revision      int64  `json:"revision"`
		CreatedAtUnix int64  `json:"created_at_unix"`
		Records       int    `json:"records"`
		Leases        int    `json:"leases"`
		SHA256        string `json:"sha256"`
		FileSHA256    string `json:"file_sha256"`
	} `json:"semantic_witness"`
}

type semanticReceipt struct {
	Format                 string `json:"format"`
	OperationID            string `json:"operation_id"`
	RestoreReceiptSHA256   string `json:"restore_receipt_sha256"`
	SnapshotReceiptSHA256  string `json:"snapshot_receipt_sha256"`
	RestoreCompletedAt     string `json:"restore_completed_at"`
	WitnessFormat          string `json:"witness_format"`
	WitnessSHA256          string `json:"witness_sha256"`
	WitnessRevision        int64  `json:"witness_revision"`
	WitnessRecords         int    `json:"witness_records"`
	WitnessLeases          int    `json:"witness_leases"`
	RestoreManifestSHA256  string `json:"restore_manifest_sha256"`
	TargetKubeSystemUID    string `json:"target_kube_system_uid"`
	TargetNamespaceUID     string `json:"target_namespace_uid"`
	SourceTidbClusterUID   string `json:"source_tidbcluster_uid"`
	RestoredTidbClusterUID string `json:"restored_tidbcluster_uid"`
	RestoredClusterID      string `json:"restored_cluster_id"`
	HistoricalExact        bool   `json:"historical_exact"`
	CurrentExact           bool   `json:"current_exact"`
	LeaseIdentityExact     bool   `json:"lease_identity_exact"`
	WatchProbeSucceeded    bool   `json:"watch_probe_succeeded"`
	ProbePutRevision       int64  `json:"probe_put_revision"`
	ProbeDeleteRevision    int64  `json:"probe_delete_revision"`
	VerifiedAtUnix         int64  `json:"verified_at_unix"`
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
	if err := validateProbePrefix(probePrefix); err != nil {
		fatal(err)
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		fatal(errors.New("SEMANTIC_RECEIPT_FILE must not already exist"))
	}

	verified, witnessFileSHA, err := openStableWitness(witnessPath)
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
	snapshotData, snapshotReceiptSHA, err := readStableBoundedJSONFile(snapshotReceiptPath, "snapshot receipt")
	if err != nil {
		fatal(err)
	}
	restoreData, restoreReceiptSHA, err := readStableBoundedJSONFile(restoreReceiptPath, "restore receipt")
	if err != nil {
		fatal(err)
	}
	restoreManifestData, restoreManifestSHA, err := readStableBoundedJSONFile(restoreManifestPath, "restore manifest")
	if err != nil {
		fatal(err)
	}
	snapshotRecord, restore, err := validateReceiptChain(status, witnessFileSHA, snapshotData, restoreData)
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
	if err := verifyBoundedJSONFileDigest(snapshotReceiptPath, snapshotReceiptSHA, "snapshot receipt"); err != nil {
		fatal(err)
	}
	if err := verifyBoundedJSONFileDigest(restoreReceiptPath, restoreReceiptSHA, "restore receipt"); err != nil {
		fatal(err)
	}
	if err := verifyBoundedJSONFileDigest(restoreManifestPath, restoreManifestSHA, "restore manifest"); err != nil {
		fatal(err)
	}
	if err := verifyFileDigest(witnessPath, witnessFileSHA, "witness file"); err != nil {
		fatal(err)
	}

	receipt := semanticReceipt{
		Format: "kubebrain.cold-physical-semantic-verify.v1", OperationID: restore.OperationID,
		RestoreReceiptSHA256: digest(restoreData), SnapshotReceiptSHA256: digest(snapshotData),
		RestoreCompletedAt: restore.CompletedAt,
		WitnessFormat:      status.Format, WitnessSHA256: status.SHA256,
		WitnessRevision: status.Revision, WitnessRecords: status.Records, WitnessLeases: status.Leases,
		RestoreManifestSHA256:  restore.RestoreManifest.SHA256,
		TargetKubeSystemUID:    restore.Target.KubeSystemUID,
		TargetNamespaceUID:     restore.Target.NamespaceUID,
		SourceTidbClusterUID:   snapshotRecord.Inventory.Storage.UID,
		RestoredTidbClusterUID: restore.Target.TidbClusterUID,
		RestoredClusterID:      restore.Target.ClusterID, HistoricalExact: true, CurrentExact: true,
		LeaseIdentityExact: true, WatchProbeSucceeded: true, ProbePutRevision: putRevision,
		ProbeDeleteRevision: deleteRevision, VerifiedAtUnix: time.Now().UTC().Unix(),
	}
	if err := validateSemanticReceipt(receipt); err != nil {
		fatal(err)
	}
	if err := writeAtomic(output, receipt); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "verified cold restore witness: records=%d leases=%d revision=%d probe=%d/%d\n",
		status.Records, status.Leases, status.Revision, putRevision, deleteRevision)
}

func validateReceiptChain(status backupfile.Status, witnessFileSHA string, snapshotData, restoreData []byte) (snapshotReceipt, restoreReceipt, error) {
	var snapshotRecord snapshotReceipt
	if err := decodeStrictJSON(snapshotData, &snapshotRecord, "snapshot receipt"); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, err
	}
	snapshotCreatedAt, err := time.Parse(time.RFC3339, snapshotRecord.CreatedAt)
	if err != nil {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt created_at is invalid")
	}
	if snapshotRecord.Inventory.Format != "kubebrain.cold-physical-snapshot-preflight.v2" {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt inventory format is invalid")
	}
	if err := validateSnapshotReceiptInventory(snapshotRecord); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, err
	}
	if snapshotRecord.Format != "kubebrain.cold-physical-snapshot.v2" || snapshotRecord.OperationID == "" ||
		snapshotRecord.Witness.Format != status.Format || snapshotRecord.Witness.Prefix != status.Prefix ||
		snapshotRecord.Witness.Revision != status.Revision || snapshotRecord.Witness.CreatedAtUnix <= 0 ||
		snapshotRecord.Witness.CreatedAtUnix != status.CreatedAtUnix || snapshotRecord.Witness.Records != status.Records ||
		snapshotRecord.Witness.Leases != status.Leases || snapshotRecord.Witness.SHA256 != status.SHA256 ||
		snapshotRecord.Witness.FileSHA256 != witnessFileSHA {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt semantic witness binding mismatch")
	}
	if snapshotCreatedAt.Before(time.Unix(status.CreatedAtUnix, 0)) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("snapshot receipt predates semantic witness")
	}
	var restore restoreReceipt
	if err := decodeStrictJSON(restoreData, &restore, "restore receipt"); err != nil {
		return snapshotReceipt{}, restoreReceipt{}, err
	}
	restoreCompletedAt, err := time.Parse(time.RFC3339, restore.CompletedAt)
	if err != nil {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("cold physical restore receipt completed_at is invalid")
	}
	if restore.Format != "kubebrain.cold-physical-restore.v1" || restore.OperationID == "" || restore.Target.ClusterID == "" ||
		restore.OperationID != snapshotRecord.OperationID || restore.SourceReceiptSHA != digest(snapshotData) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("cold physical restore receipt does not bind the snapshot receipt")
	}
	if restoreCompletedAt.Before(snapshotCreatedAt) {
		return snapshotReceipt{}, restoreReceipt{}, errors.New("restore receipt predates snapshot receipt")
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

func validateSnapshotReceiptInventory(snapshotRecord snapshotReceipt) error {
	inventory := snapshotRecord.Inventory
	if inventory.VolumeSnapshotClass.Name == "" || inventory.VolumeSnapshotClass.Driver == "" ||
		inventory.VolumeSnapshotClass.DeletionPolicy != "Retain" ||
		inventory.KubeBrain.Namespace == "" || inventory.KubeBrain.StatefulSet == "" || inventory.KubeBrain.UID == "" ||
		inventory.Storage.Namespace == "" || inventory.Storage.TidbCluster == "" ||
		inventory.Storage.UID == "" || inventory.Storage.ClusterID == "" {
		return errors.New("snapshot receipt inventory identity is incomplete")
	}
	blueprint := inventory.RecoveryBlueprint.TidbCluster
	if blueprint.APIVersion != "pingcap.com/v1alpha1" || blueprint.Kind != "TidbCluster" ||
		blueprint.Metadata.Name != inventory.Storage.TidbCluster ||
		blueprint.Metadata.Namespace != inventory.Storage.Namespace || len(blueprint.Spec) == 0 {
		return errors.New("snapshot receipt TidbCluster blueprint identity is invalid")
	}
	var replicas struct {
		PD struct {
			Replicas int `json:"replicas"`
		} `json:"pd"`
		TiKV struct {
			Replicas int `json:"replicas"`
		} `json:"tikv"`
	}
	if err := json.Unmarshal(blueprint.Spec, &replicas); err != nil ||
		replicas.PD.Replicas != len(inventory.PDPVCs) || replicas.TiKV.Replicas != len(inventory.TiKVPVCs) {
		return errors.New("snapshot receipt PVC inventory does not match TidbCluster replicas")
	}

	allPVCs := append(append([]sourcePVC(nil), inventory.PDPVCs...), inventory.TiKVPVCs...)
	if len(allPVCs) == 0 || len(allPVCs) != len(snapshotRecord.Snapshots) {
		return errors.New("snapshot receipt PVC and snapshot inventory counts do not match")
	}
	claims := make(map[string]sourcePVC, len(allPVCs))
	components := make(map[string]string, len(allPVCs))
	seenUIDs := map[string]struct{}{}
	seenPVs := map[string]struct{}{}
	seenPVUIDs := map[string]struct{}{}
	seenVolumeHandles := map[string]struct{}{}
	for _, group := range []struct {
		component string
		claims    []sourcePVC
	}{{component: "pd", claims: inventory.PDPVCs}, {component: "tikv", claims: inventory.TiKVPVCs}} {
		for _, claim := range group.claims {
			if claim.Name == "" || claim.UID == "" || claim.PV == "" || claim.PVUID == "" || claim.CSIDriver == "" || claim.VolumeHandle == "" || claim.Phase != "Bound" ||
				claim.StorageClass == "" || claim.VolumeMode == "" || len(claim.AccessModes) == 0 ||
				claim.RequestedStorage == "" || claim.Labels["app.kubernetes.io/instance"] != inventory.Storage.TidbCluster ||
				claim.Labels["app.kubernetes.io/component"] != group.component {
				return fmt.Errorf("snapshot receipt source PVC %q inventory is incomplete", claim.Name)
			}
			requested, err := resource.ParseQuantity(claim.RequestedStorage)
			if err != nil || requested.Sign() < 0 {
				return fmt.Errorf("snapshot receipt source PVC %q has invalid storage request", claim.Name)
			}
			if _, duplicate := claims[claim.Name]; duplicate {
				return errors.New("snapshot receipt contains duplicate source PVC")
			}
			if _, duplicate := seenUIDs[claim.UID]; duplicate {
				return errors.New("snapshot receipt contains duplicate source PVC UID")
			}
			if _, duplicate := seenPVs[claim.PV]; duplicate {
				return errors.New("snapshot receipt contains duplicate source PV")
			}
			if claim.CSIDriver != inventory.VolumeSnapshotClass.Driver {
				return errors.New("snapshot receipt source PV driver does not match snapshot class")
			}
			if _, duplicate := seenPVUIDs[claim.PVUID]; duplicate {
				return errors.New("snapshot receipt contains duplicate source PV UID")
			}
			if _, duplicate := seenVolumeHandles[claim.VolumeHandle]; duplicate {
				return errors.New("snapshot receipt contains duplicate source PV volume handle")
			}
			claims[claim.Name] = claim
			components[claim.Name] = group.component
			seenUIDs[claim.UID] = struct{}{}
			seenPVs[claim.PV] = struct{}{}
			seenPVUIDs[claim.PVUID] = struct{}{}
			seenVolumeHandles[claim.VolumeHandle] = struct{}{}
		}
	}
	seenSnapshots := map[string]struct{}{}
	seenSnapshotUIDs := map[string]struct{}{}
	seenContents := map[string]struct{}{}
	seenContentUIDs := map[string]struct{}{}
	seenHandles := map[string]struct{}{}
	for _, snapshot := range snapshotRecord.Snapshots {
		claim, exists := claims[snapshot.SourcePVC]
		if !exists || snapshot.Component != components[snapshot.SourcePVC] || snapshot.Name == "" ||
			snapshot.UID == "" || snapshot.Content == "" || snapshot.ContentUID == "" ||
			snapshot.SnapshotHandle == "" || snapshot.RestoreSize == "" {
			return errors.New("snapshot receipt snapshot inventory does not match source PVCs")
		}
		requested, requestedErr := resource.ParseQuantity(claim.RequestedStorage)
		restoreSize, restoreErr := resource.ParseQuantity(snapshot.RestoreSize)
		if requestedErr != nil || restoreErr != nil || restoreSize.Sign() < 0 || requested.Cmp(restoreSize) < 0 {
			return fmt.Errorf("snapshot receipt restore size for PVC %q is invalid", snapshot.SourcePVC)
		}
		for _, identity := range []struct {
			value string
			seen  map[string]struct{}
		}{
			{snapshot.Name, seenSnapshots}, {snapshot.UID, seenSnapshotUIDs}, {snapshot.Content, seenContents},
			{snapshot.ContentUID, seenContentUIDs}, {snapshot.SnapshotHandle, seenHandles},
		} {
			if _, duplicate := identity.seen[identity.value]; duplicate {
				return errors.New("snapshot receipt contains duplicate snapshot identity")
			}
			identity.seen[identity.value] = struct{}{}
		}
	}
	return nil
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

func readStableBoundedJSONFile(path, description string) ([]byte, string, error) {
	data, err := readBoundedJSONFile(path, description)
	if err != nil {
		return nil, "", err
	}
	sha := digest(data)
	afterStableJSONReadForTest(description)
	confirm, err := readBoundedJSONFile(path, description)
	if err != nil {
		return nil, "", err
	}
	if !bytes.Equal(confirm, data) || digest(confirm) != sha {
		return nil, "", fmt.Errorf("%s changed during capture", description)
	}
	return data, sha, nil
}

func verifyBoundedJSONFileDigest(path, expected, description string) error {
	data, err := readBoundedJSONFile(path, description)
	if err != nil {
		return err
	}
	if digest(data) != expected {
		return fmt.Errorf("%s changed after validation", description)
	}
	return nil
}

func openStableWitness(path string) (*backupfile.Verified, string, error) {
	witnessFileSHA, err := fileDigest(path)
	if err != nil {
		return nil, "", err
	}
	afterWitnessDigestForTest()
	verified, err := backupfile.OpenVerified(path)
	if err != nil {
		return nil, "", err
	}
	if err := verifyFileDigest(path, witnessFileSHA, "witness file"); err != nil {
		verified.Close()
		return nil, "", err
	}
	return verified, witnessFileSHA, nil
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func verifyFileDigest(path, expected, description string) error {
	actual, err := fileDigest(path)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%s changed after validation", description)
	}
	return nil
}

func validateRestoreReceiptInventory(restore restoreReceipt, snapshotRecord snapshotReceipt) error {
	if snapshotRecord.Inventory.Storage.Namespace == "" ||
		snapshotRecord.Inventory.Storage.TidbCluster == "" ||
		snapshotRecord.Inventory.Storage.ClusterID == "" ||
		snapshotRecord.Inventory.VolumeSnapshotClass.Driver == "" ||
		restore.Target.KubeSystemUID == "" || restore.Target.NamespaceUID == "" ||
		restore.Target.TidbClusterUID == "" ||
		restore.Target.TidbClusterUID == snapshotRecord.Inventory.Storage.UID ||
		restore.Target.Namespace != snapshotRecord.Inventory.Storage.Namespace ||
		restore.Target.TidbCluster != snapshotRecord.Inventory.Storage.TidbCluster ||
		restore.Target.ClusterID != snapshotRecord.Inventory.Storage.ClusterID {
		return errors.New("cold physical restore receipt target does not match the snapshot receipt")
	}
	expected := make(map[string]string, len(snapshotRecord.Snapshots))
	expectedPVCs := make(map[string]string, len(snapshotRecord.Snapshots))
	for _, snapshot := range snapshotRecord.Snapshots {
		if snapshot.SourcePVC == "" || snapshot.SnapshotHandle == "" {
			return errors.New("snapshot receipt contains incomplete restored inventory mapping")
		}
		if _, exists := expected[snapshot.SnapshotHandle]; exists {
			return errors.New("snapshot receipt contains duplicate snapshot handle")
		}
		objectName := restoreManifestObjectName(snapshotRecord.OperationID, snapshot.SourcePVC)
		expected[snapshot.SnapshotHandle] = objectName
		if _, duplicate := expectedPVCs[snapshot.SourcePVC]; duplicate {
			return errors.New("snapshot receipt contains duplicate source PVC")
		}
		expectedPVCs[snapshot.SourcePVC] = objectName
	}
	if len(restore.VolumeSnapshotContents) != len(expected) || len(restore.VolumeSnapshots) != len(expected) ||
		len(restore.PVCs) != len(expectedPVCs) || len(restore.PVs) != len(expectedPVCs) {
		return errors.New("cold physical restore receipt inventory count does not match snapshot receipt")
	}
	seenContentNames := map[string]struct{}{}
	seenContentUIDs := map[string]struct{}{}
	seenHandles := map[string]struct{}{}
	contents := make(map[string]restoredVolumeSnapshotContent, len(restore.VolumeSnapshotContents))
	for _, content := range restore.VolumeSnapshotContents {
		expectedName, exists := expected[content.SnapshotHandle]
		if !exists || content.Name != expectedName || content.UID == "" ||
			content.Driver != snapshotRecord.Inventory.VolumeSnapshotClass.Driver ||
			content.SnapshotClass == "" || content.DeletionPolicy != "Retain" ||
			content.SnapshotRef.APIVersion != "snapshot.storage.k8s.io/v1" ||
			content.SnapshotRef.Kind != "VolumeSnapshot" ||
			content.SnapshotRef.Namespace != snapshotRecord.Inventory.Storage.Namespace ||
			content.SnapshotRef.Name != content.Name {
			return errors.New("cold physical restore receipt content inventory does not match snapshot receipt")
		}
		if _, duplicate := seenContentNames[content.Name]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate VolumeSnapshotContent")
		}
		if _, duplicate := seenHandles[content.SnapshotHandle]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate snapshot handle")
		}
		if _, duplicate := seenContentUIDs[content.UID]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate VolumeSnapshotContent UID")
		}
		seenContentNames[content.Name] = struct{}{}
		seenContentUIDs[content.UID] = struct{}{}
		seenHandles[content.SnapshotHandle] = struct{}{}
		contents[content.Name] = content
	}
	seenSnapshotUIDs := map[string]struct{}{}
	seenSnapshots := map[string]struct{}{}
	for _, snapshot := range restore.VolumeSnapshots {
		content, exists := contents[snapshot.Name]
		if !exists || snapshot.UID == "" || !snapshot.Ready || snapshot.SnapshotClass != content.SnapshotClass ||
			snapshot.SourceContent != content.Name || snapshot.BoundContent != content.Name {
			return errors.New("cold physical restore receipt VolumeSnapshot inventory does not match content inventory")
		}
		if _, duplicate := seenSnapshots[snapshot.Name]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate VolumeSnapshot")
		}
		if _, duplicate := seenSnapshotUIDs[snapshot.UID]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate VolumeSnapshot UID")
		}
		seenSnapshots[snapshot.Name] = struct{}{}
		seenSnapshotUIDs[snapshot.UID] = struct{}{}
	}
	seenPVCNames := map[string]struct{}{}
	seenPVCUIDs := map[string]struct{}{}
	seenPVs := map[string]struct{}{}
	claimsByPV := make(map[string]restoredPVC, len(restore.PVCs))
	for _, pvc := range restore.PVCs {
		expectedSnapshot, exists := expectedPVCs[pvc.Name]
		if !exists || pvc.UID == "" || pvc.PV == "" || pvc.Phase != "Bound" ||
			pvc.StorageClass == "" || pvc.VolumeMode == "" || len(pvc.AccessModes) == 0 ||
			pvc.RequestedStorage == "" || pvc.DataSource.APIGroup != "snapshot.storage.k8s.io" ||
			pvc.DataSource.Kind != "VolumeSnapshot" || pvc.DataSource.Name != expectedSnapshot {
			return errors.New("cold physical restore receipt PVC inventory does not match snapshot receipt")
		}
		requested, err := resource.ParseQuantity(pvc.RequestedStorage)
		if err != nil || requested.Sign() < 0 {
			return fmt.Errorf("cold physical restore receipt PVC %q has invalid storage request", pvc.Name)
		}
		if _, duplicate := seenPVCNames[pvc.Name]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PVC")
		}
		if _, duplicate := seenPVs[pvc.PV]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PV")
		}
		if _, duplicate := seenPVCUIDs[pvc.UID]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PVC UID")
		}
		seenPVCNames[pvc.Name] = struct{}{}
		seenPVCUIDs[pvc.UID] = struct{}{}
		seenPVs[pvc.PV] = struct{}{}
		claimsByPV[pvc.PV] = pvc
	}
	seenPVUIDs := map[string]struct{}{}
	seenPVNames := map[string]struct{}{}
	seenVolumeHandles := map[string]struct{}{}
	for _, volume := range restore.PVs {
		claim, exists := claimsByPV[volume.Name]
		if !exists || volume.UID == "" || volume.Phase != "Bound" ||
			volume.StorageClass != claim.StorageClass || volume.VolumeMode != claim.VolumeMode ||
			volume.ClaimRef.APIVersion != "v1" || volume.ClaimRef.Kind != "PersistentVolumeClaim" ||
			volume.ClaimRef.Namespace != snapshotRecord.Inventory.Storage.Namespace ||
			volume.ClaimRef.Name != claim.Name || volume.ClaimRef.UID != claim.UID ||
			volume.CSIDriver != snapshotRecord.Inventory.VolumeSnapshotClass.Driver || volume.VolumeHandle == "" {
			return errors.New("cold physical restore receipt PV inventory does not match PVC inventory")
		}
		capacity, capacityErr := resource.ParseQuantity(volume.Capacity)
		requested, requestedErr := resource.ParseQuantity(claim.RequestedStorage)
		if capacityErr != nil || requestedErr != nil || capacity.Sign() < 0 || capacity.Cmp(requested) < 0 {
			return fmt.Errorf("cold physical restore receipt PV %q capacity does not satisfy PVC %q request", volume.Name, claim.Name)
		}
		if _, duplicate := seenPVUIDs[volume.UID]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PV UID")
		}
		if _, duplicate := seenPVNames[volume.Name]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate PV")
		}
		if _, duplicate := seenVolumeHandles[volume.VolumeHandle]; duplicate {
			return errors.New("cold physical restore receipt contains duplicate CSI volume handle")
		}
		seenPVUIDs[volume.UID] = struct{}{}
		seenPVNames[volume.Name] = struct{}{}
		seenVolumeHandles[volume.VolumeHandle] = struct{}{}
	}
	if len(seenPVNames) != len(claimsByPV) {
		return errors.New("cold physical restore receipt PV inventory does not cover every PVC")
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
	if err := validateRestoreManifestContent(items, snapshotRecord); err != nil {
		return err
	}
	return validateRestoreManifestReceiptInventory(items, restore)
}

func validateRestoreManifestReceiptInventory(items []any, restore restoreReceipt) error {
	contents := make(map[string]restoredVolumeSnapshotContent, len(restore.VolumeSnapshotContents))
	for _, content := range restore.VolumeSnapshotContents {
		contents[content.Name] = content
	}
	snapshots := make(map[string]restoredVolumeSnapshot, len(restore.VolumeSnapshots))
	for _, snapshot := range restore.VolumeSnapshots {
		snapshots[snapshot.Name] = snapshot
	}
	claims := make(map[string]restoredPVC, len(restore.PVCs))
	for _, claim := range restore.PVCs {
		claims[claim.Name] = claim
	}
	seenContents := map[string]struct{}{}
	seenSnapshots := map[string]struct{}{}
	seenClaims := map[string]struct{}{}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		metadata, _ := item["metadata"].(map[string]any)
		spec, _ := item["spec"].(map[string]any)
		name, _ := metadata["name"].(string)
		switch item["kind"] {
		case "VolumeSnapshotContent":
			content, exists := contents[name]
			if !exists || content.Driver != stringValue(spec["driver"]) ||
				content.SnapshotClass != stringValue(spec["volumeSnapshotClassName"]) ||
				content.DeletionPolicy != stringValue(spec["deletionPolicy"]) ||
				content.SnapshotHandle != nestedString(spec, "source", "snapshotHandle") ||
				content.SnapshotRef.APIVersion != nestedString(spec, "volumeSnapshotRef", "apiVersion") ||
				content.SnapshotRef.Kind != nestedString(spec, "volumeSnapshotRef", "kind") ||
				content.SnapshotRef.Namespace != nestedString(spec, "volumeSnapshotRef", "namespace") ||
				content.SnapshotRef.Name != nestedString(spec, "volumeSnapshotRef", "name") {
				return errors.New("restore receipt VolumeSnapshotContent inventory does not match restore manifest")
			}
			seenContents[name] = struct{}{}
		case "VolumeSnapshot":
			snapshot, exists := snapshots[name]
			if !exists || snapshot.SnapshotClass != stringValue(spec["volumeSnapshotClassName"]) ||
				snapshot.SourceContent != nestedString(spec, "source", "volumeSnapshotContentName") {
				return errors.New("restore receipt VolumeSnapshot inventory does not match restore manifest")
			}
			seenSnapshots[name] = struct{}{}
		case "PersistentVolumeClaim":
			claim, exists := claims[name]
			manifestModes := stringSlice(spec["accessModes"])
			claimModes := append([]string(nil), claim.AccessModes...)
			sort.Strings(manifestModes)
			sort.Strings(claimModes)
			if !exists || claim.StorageClass != stringValue(spec["storageClassName"]) ||
				claim.VolumeMode != stringValue(spec["volumeMode"]) || !slicesEqual(claimModes, manifestModes) ||
				claim.RequestedStorage != nestedString3(spec, "resources", "requests", "storage") ||
				claim.DataSource.APIGroup != nestedString(spec, "dataSource", "apiGroup") ||
				claim.DataSource.Kind != nestedString(spec, "dataSource", "kind") ||
				claim.DataSource.Name != nestedString(spec, "dataSource", "name") {
				return errors.New("restore receipt PVC inventory does not match restore manifest")
			}
			seenClaims[name] = struct{}{}
		}
	}
	if len(seenContents) != len(contents) || len(seenSnapshots) != len(snapshots) || len(seenClaims) != len(claims) {
		return errors.New("restore receipt storage inventory is not fully represented by restore manifest")
	}
	return nil
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
	sourceClaims := make(map[string]sourcePVC, len(snapshotRecord.Inventory.PDPVCs)+len(snapshotRecord.Inventory.TiKVPVCs))
	for _, claim := range append(append([]sourcePVC(nil), snapshotRecord.Inventory.PDPVCs...), snapshotRecord.Inventory.TiKVPVCs...) {
		sourceClaims[claim.Name] = claim
	}
	for _, snapshot := range snapshotRecord.Snapshots {
		if snapshot.SourcePVC == "" || snapshot.Component == "" || snapshot.SnapshotHandle == "" {
			return errors.New("snapshot receipt contains incomplete snapshot mapping")
		}
		if _, exists := expected[snapshot.SourcePVC]; exists {
			return errors.New("snapshot receipt contains duplicate source PVC")
		}
		claim, exists := sourceClaims[snapshot.SourcePVC]
		if !exists {
			return errors.New("snapshot receipt snapshot has no source PVC inventory")
		}
		expected[snapshot.SourcePVC] = restoreManifestSnapshotMapping{
			component:        snapshot.Component,
			handle:           snapshot.SnapshotHandle,
			name:             restoreManifestObjectName(snapshotRecord.OperationID, snapshot.SourcePVC),
			volumeMode:       claim.VolumeMode,
			accessModes:      append([]string(nil), claim.AccessModes...),
			requestedStorage: claim.RequestedStorage,
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
			labels, _ := metadata["labels"].(map[string]any)
			if spec["deletionPolicy"] != "Retain" ||
				item["apiVersion"] != "snapshot.storage.k8s.io/v1" ||
				labels["kubebrain.io/operation-id"] != snapshotRecord.OperationID ||
				spec["driver"] != snapshotRecord.Inventory.VolumeSnapshotClass.Driver ||
				spec["volumeSnapshotClassName"] == "" ||
				nestedString(spec, "source", "snapshotHandle") != mapping.handle ||
				nestedString(spec, "volumeSnapshotRef", "apiVersion") != "snapshot.storage.k8s.io/v1" ||
				nestedString(spec, "volumeSnapshotRef", "kind") != "VolumeSnapshot" ||
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
			labels, _ := metadata["labels"].(map[string]any)
			if item["apiVersion"] != "snapshot.storage.k8s.io/v1" ||
				metadata["namespace"] != snapshotRecord.Inventory.Storage.Namespace ||
				labels["kubebrain.io/operation-id"] != snapshotRecord.OperationID ||
				spec["volumeSnapshotClassName"] == "" ||
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
			manifestModes := stringSlice(spec["accessModes"])
			expectedModes := append([]string(nil), mapping.accessModes...)
			sort.Strings(manifestModes)
			sort.Strings(expectedModes)
			if item["apiVersion"] != "v1" ||
				metadata["namespace"] != snapshotRecord.Inventory.Storage.Namespace ||
				labels["app.kubernetes.io/component"] != mapping.component ||
				labels["kubebrain.io/operation-id"] != snapshotRecord.OperationID ||
				spec["storageClassName"] == "" ||
				spec["volumeMode"] != mapping.volumeMode || !slicesEqual(manifestModes, expectedModes) ||
				nestedString3(spec, "resources", "requests", "storage") != mapping.requestedStorage ||
				nestedString(spec, "dataSource", "apiGroup") != "snapshot.storage.k8s.io" ||
				nestedString(spec, "dataSource", "name") != mapping.name ||
				nestedString(spec, "dataSource", "kind") != "VolumeSnapshot" {
				return errors.New("restore manifest PVC does not match snapshot receipt")
			}
			seenPVC[name] = struct{}{}
		case "TidbCluster":
			tidbClusters++
			annotations, _ := metadata["annotations"].(map[string]any)
			if item["apiVersion"] != "pingcap.com/v1alpha1" ||
				metadata["name"] != snapshotRecord.Inventory.Storage.TidbCluster ||
				metadata["namespace"] != snapshotRecord.Inventory.Storage.Namespace ||
				annotations["kubebrain.io/cold-restore-operation"] != snapshotRecord.OperationID ||
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

func nestedString3(parent map[string]any, first, second, third string) string {
	child, _ := parent[first].(map[string]any)
	return nestedString(child, second, third)
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func stringSlice(value any) []string {
	raw, _ := value.([]any)
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		text, ok := item.(string)
		if !ok {
			return nil
		}
		result = append(result, text)
	}
	return result
}

func slicesEqual(left, right []string) bool {
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
	token, err := randomHex(24)
	if err != nil {
		return 0, 0, fmt.Errorf("generate watch probe nonce: %w", err)
	}
	key := strings.TrimRight(prefix, "/") + "/" + token
	value := "cold-restore-semantic-probe:" + token
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
	put, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		return 0, 0, fmt.Errorf("conditionally create watch probe: %w", err)
	}
	if !put.Succeeded || put.Header == nil || put.Header.Revision <= 0 {
		return 0, 0, errors.New("watch probe key unexpectedly existed or put returned no revision")
	}
	if err := expectWatchEvent(watch, mvccpb.PUT, []byte(key), put.Header.Revision); err != nil {
		return 0, 0, err
	}
	read, err := cli.Get(ctx, key)
	if err != nil || len(read.Kvs) != 1 || string(read.Kvs[0].Value) != value || read.Kvs[0].Lease != int64(lease.ID) {
		return 0, 0, errors.New("linearizable probe read mismatch")
	}
	deleted, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", value)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return 0, 0, fmt.Errorf("conditionally delete watch probe: %w", err)
	}
	if !deleted.Succeeded || deleted.Header == nil || len(deleted.Responses) != 1 ||
		deleted.Responses[0].GetResponseDeleteRange().Deleted != 1 {
		return 0, 0, errors.New("watch probe delete compare failed or deleted the wrong key count")
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

func validateProbePrefix(prefix string) error {
	if prefix == "" || !strings.HasPrefix(prefix, "/") || containsControlCharacter(prefix) {
		return errors.New("VERIFY_PREFIX must be an absolute key prefix without control characters")
	}
	trimmed := strings.TrimRight(prefix, "/")
	if trimmed == "" || trimmed == "/registry" || strings.HasPrefix(trimmed+"/", "/registry/") {
		return errors.New("VERIFY_PREFIX must not target Kubernetes /registry data")
	}
	return nil
}

func containsControlCharacter(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) >= 0
}

func randomHex(bytesCount int) (string, error) {
	token := make([]byte, bytesCount)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return hex.EncodeToString(token), nil
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
	for i := 0; i < len(value); i++ {
		switch {
		case value[i] >= '0' && value[i] <= '9':
		case value[i] >= 'a' && value[i] <= 'f':
		default:
			return false
		}
	}
	return true
}

func validateSemanticReceipt(receipt semanticReceipt) error {
	if receipt.Format != "kubebrain.cold-physical-semantic-verify.v1" ||
		receipt.OperationID == "" ||
		!validDigest(receipt.RestoreReceiptSHA256) ||
		!validDigest(receipt.SnapshotReceiptSHA256) ||
		!validDigest(receipt.WitnessSHA256) ||
		!validDigest(receipt.RestoreManifestSHA256) ||
		receipt.WitnessFormat != backupfile.Format ||
		receipt.WitnessRevision <= 0 ||
		receipt.WitnessRecords <= 0 ||
		receipt.WitnessLeases < 0 ||
		receipt.RestoredClusterID == "" ||
		receipt.TargetKubeSystemUID == "" ||
		receipt.TargetNamespaceUID == "" ||
		receipt.SourceTidbClusterUID == "" ||
		receipt.RestoredTidbClusterUID == "" ||
		receipt.SourceTidbClusterUID == receipt.RestoredTidbClusterUID ||
		receipt.ProbePutRevision <= 0 ||
		receipt.ProbeDeleteRevision <= receipt.ProbePutRevision ||
		receipt.VerifiedAtUnix <= 0 ||
		!receipt.HistoricalExact ||
		!receipt.CurrentExact ||
		!receipt.LeaseIdentityExact ||
		!receipt.WatchProbeSucceeded {
		return errors.New("cold physical semantic receipt is incomplete")
	}
	restoreCompletedAt, err := time.Parse(time.RFC3339, receipt.RestoreCompletedAt)
	if err != nil {
		return errors.New("cold physical semantic receipt restore_completed_at is invalid")
	}
	if time.Unix(receipt.VerifiedAtUnix, 0).Before(restoreCompletedAt) {
		return errors.New("cold physical semantic receipt verification predates restore completion")
	}
	return nil
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
	if err := os.Link(tmpName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("semantic receipt already exists")
		}
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
