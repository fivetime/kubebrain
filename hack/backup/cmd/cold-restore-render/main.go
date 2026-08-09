package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

const maxColdRestoreJSONBytes = 4 << 20

type receipt struct {
	Format          string     `json:"format"`
	OperationID     string     `json:"operation_id"`
	CreatedAt       string     `json:"created_at"`
	Inventory       inventory  `json:"inventory"`
	Snapshots       []snapshot `json:"snapshots"`
	SemanticWitness struct {
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

type inventory struct {
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
		TidbCluster map[string]any `json:"tidbcluster"`
	} `json:"recovery_blueprint"`
	PDPVCs   []pvc `json:"pd_pvcs"`
	TiKVPVCs []pvc `json:"tikv_pvcs"`
}

type pvc struct {
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

type snapshot struct {
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Content            string `json:"content"`
	ContentUID         string `json:"content_uid"`
	SourcePVC          string `json:"source_pvc"`
	Component          string `json:"component"`
	SourceVolumeHandle string `json:"source_volume_handle"`
	SnapshotHandle     string `json:"snapshot_handle"`
	RestoreSize        string `json:"restore_size"`
}

func main() {
	receiptPath := flag.String("receipt", "", "cold physical snapshot v2 receipt")
	snapshotClass := flag.String("target-snapshot-class", "", "target VolumeSnapshotClass using the receipt CSI driver")
	storageClass := flag.String("target-storage-class", "", "target StorageClass used for restored PVCs")
	outputPath := flag.String("output", "", "new output JSON manifest path")
	isolated := flag.Bool("confirm-isolated-target", false, "confirm restore uses an isolated cluster with the source namespace and names")
	flag.Parse()
	if *receiptPath == "" || *snapshotClass == "" || *storageClass == "" || *outputPath == "" || !*isolated {
		fmt.Fprintln(os.Stderr, "--receipt, --target-snapshot-class, --target-storage-class, --output and --confirm-isolated-target are required")
		os.Exit(2)
	}
	if _, err := os.Stat(*outputPath); !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "output must not already exist")
		os.Exit(2)
	}
	data, err := readBoundedJSONFile(*receiptPath, "cold snapshot receipt")
	if err != nil {
		fatal(err)
	}
	value, err := decodeReceipt(data)
	if err != nil {
		fatal(err)
	}
	manifest, err := render(value, *snapshotClass, *storageClass)
	if err != nil {
		fatal(err)
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := writeAtomic(*outputPath, encoded); err != nil {
		fatal(err)
	}
}

func writeAtomic(path string, data []byte) (returnErr error) {
	directory := filepath.Dir(path)
	tmp, err := os.CreateTemp(directory, ".cold-restore-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err := os.Remove(tmpName); err != nil && !errors.Is(err, os.ErrNotExist) && returnErr == nil {
			returnErr = err
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("restore manifest output already exists %q: %w", path, os.ErrExist)
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

func decodeReceipt(data []byte) (receipt, error) {
	var value receipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return receipt{}, fmt.Errorf("decode receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt{}, errors.New("receipt contains trailing JSON")
	}
	return value, nil
}

func render(r receipt, snapshotClass, storageClass string) (map[string]any, error) {
	if r.Format != "kubebrain.cold-physical-snapshot.v2" {
		return nil, fmt.Errorf("unsupported receipt format %q", r.Format)
	}
	if r.OperationID == "" || r.Inventory.VolumeSnapshotClass.Driver == "" || snapshotClass == "" || storageClass == "" {
		return nil, errors.New("receipt identity, CSI driver and target classes must be non-empty")
	}
	if r.Inventory.Format != "kubebrain.cold-physical-snapshot-preflight.v2" {
		return nil, errors.New("cold snapshot receipt inventory format is invalid")
	}
	createdAt, err := time.Parse(time.RFC3339, r.CreatedAt)
	if err != nil {
		return nil, errors.New("cold snapshot receipt created_at is invalid")
	}
	if createdAt.After(time.Now()) {
		return nil, errors.New("cold snapshot receipt created_at is in the future")
	}
	if r.SemanticWitness.CreatedAtUnix <= 0 || createdAt.Before(time.Unix(r.SemanticWitness.CreatedAtUnix, 0)) {
		return nil, errors.New("cold snapshot receipt predates semantic witness")
	}
	if r.SemanticWitness.Format != "kubebrain.logical.v2" || r.SemanticWitness.Prefix == "" ||
		r.SemanticWitness.Revision <= 0 || r.SemanticWitness.Records <= 0 ||
		!validDigest(r.SemanticWitness.SHA256) || !validDigest(r.SemanticWitness.FileSHA256) {
		return nil, errors.New("cold snapshot receipt has no complete semantic witness binding")
	}
	namespace := r.Inventory.Storage.Namespace
	cluster := r.Inventory.Storage.TidbCluster
	if problems := validation.IsDNS1123Label(r.OperationID); len(problems) > 0 || len(r.OperationID) > 40 {
		return nil, errors.New("operation ID must be a DNS label of at most 40 characters")
	}
	for label, name := range map[string]string{"namespace": namespace, "TidbCluster": cluster, "snapshot class": snapshotClass, "storage class": storageClass} {
		if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
			return nil, fmt.Errorf("invalid %s name %q", label, name)
		}
	}
	if namespace == "" || cluster == "" || r.Inventory.Storage.UID == "" || r.Inventory.Storage.ClusterID == "" {
		return nil, errors.New("source storage identity is incomplete")
	}
	blueprint, err := cloneMap(r.Inventory.RecoveryBlueprint.TidbCluster)
	if err != nil {
		return nil, fmt.Errorf("clone TidbCluster blueprint: %w", err)
	}
	metadata, ok := blueprint["metadata"].(map[string]any)
	if !ok || metadata["name"] != cluster || metadata["namespace"] != namespace || blueprint["apiVersion"] != "pingcap.com/v1alpha1" || blueprint["kind"] != "TidbCluster" {
		return nil, errors.New("TidbCluster blueprint identity does not match source storage identity")
	}
	spec, ok := blueprint["spec"].(map[string]any)
	if !ok {
		return nil, errors.New("TidbCluster blueprint spec is missing")
	}
	if spec["paused"] == true {
		return nil, errors.New("source TidbCluster blueprint was already paused")
	}
	spec["paused"] = true
	metadata["annotations"] = map[string]any{
		"kubebrain.io/cold-restore-operation": r.OperationID,
		"kubebrain.io/source-cluster-id":      r.Inventory.Storage.ClusterID,
		"kubebrain.io/source-tidbcluster-uid": r.Inventory.Storage.UID,
	}

	allPVCs := append(append([]pvc(nil), r.Inventory.PDPVCs...), r.Inventory.TiKVPVCs...)
	if len(allPVCs) == 0 || len(allPVCs) != len(r.Snapshots) {
		return nil, errors.New("PVC and snapshot counts must be equal and non-zero")
	}
	if err := requireReplicaCount(spec, "pd", len(r.Inventory.PDPVCs)); err != nil {
		return nil, err
	}
	if err := requireReplicaCount(spec, "tikv", len(r.Inventory.TiKVPVCs)); err != nil {
		return nil, err
	}
	pvcByName := make(map[string]pvc, len(allPVCs))
	componentByPVC := make(map[string]string, len(allPVCs))
	seenPVUIDs := make(map[string]struct{}, len(allPVCs))
	seenVolumeHandles := make(map[string]struct{}, len(allPVCs))
	for _, componentPVCs := range []struct {
		component string
		values    []pvc
	}{{"pd", r.Inventory.PDPVCs}, {"tikv", r.Inventory.TiKVPVCs}} {
		for _, volume := range componentPVCs.values {
			if problems := validation.IsDNS1123Subdomain(volume.Name); len(problems) > 0 {
				return nil, fmt.Errorf("invalid PVC name %q", volume.Name)
			}
			if volume.Name == "" || volume.UID == "" || volume.PV == "" || volume.PVUID == "" || volume.CSIDriver == "" || volume.VolumeHandle == "" ||
				volume.RequestedStorage == "" || len(volume.AccessModes) == 0 || volume.VolumeMode == "" {
				return nil, fmt.Errorf("PVC blueprint for %q is incomplete", volume.Name)
			}
			if volume.Labels["app.kubernetes.io/instance"] != cluster || volume.Labels["app.kubernetes.io/component"] != componentPVCs.component {
				return nil, fmt.Errorf("PVC %q operator labels do not match the recovery blueprint", volume.Name)
			}
			if volume.CSIDriver != r.Inventory.VolumeSnapshotClass.Driver {
				return nil, fmt.Errorf("PVC %q source PV driver does not match snapshot class", volume.Name)
			}
			if _, exists := pvcByName[volume.Name]; exists {
				return nil, fmt.Errorf("duplicate PVC %q", volume.Name)
			}
			if _, exists := seenPVUIDs[volume.PVUID]; exists {
				return nil, errors.New("source PV UIDs must be unique")
			}
			if _, exists := seenVolumeHandles[volume.VolumeHandle]; exists {
				return nil, errors.New("source PV volume handles must be unique")
			}
			pvcByName[volume.Name] = volume
			componentByPVC[volume.Name] = componentPVCs.component
			seenPVUIDs[volume.PVUID] = struct{}{}
			seenVolumeHandles[volume.VolumeHandle] = struct{}{}
		}
	}
	snapshotByPVC := make(map[string]snapshot, len(r.Snapshots))
	handles := make(map[string]struct{}, len(r.Snapshots))
	snapshotUIDs := make(map[string]struct{}, len(r.Snapshots))
	contents := make(map[string]struct{}, len(r.Snapshots))
	contentUIDs := make(map[string]struct{}, len(r.Snapshots))
	for _, snap := range r.Snapshots {
		volume, exists := pvcByName[snap.SourcePVC]
		if !exists || snap.Component != componentByPVC[snap.SourcePVC] {
			return nil, fmt.Errorf("snapshot source PVC/component mismatch for %q", snap.SourcePVC)
		}
		if snap.Name != r.OperationID+"-"+snap.SourcePVC || snap.UID == "" || snap.Content == "" || snap.ContentUID == "" ||
			snap.SourceVolumeHandle == "" || snap.SnapshotHandle == "" || snap.RestoreSize == "" {
			return nil, fmt.Errorf("snapshot identity for %q is incomplete", snap.SourcePVC)
		}
		if snap.SourceVolumeHandle != volume.VolumeHandle {
			return nil, fmt.Errorf("snapshot source volume handle does not match PVC %q", snap.SourcePVC)
		}
		if _, exists := snapshotByPVC[snap.SourcePVC]; exists {
			return nil, fmt.Errorf("duplicate snapshot for PVC %q", snap.SourcePVC)
		}
		if _, exists := handles[snap.SnapshotHandle]; exists {
			return nil, errors.New("snapshot handles must be unique")
		}
		for _, identity := range []struct {
			value string
			seen  map[string]struct{}
		}{{snap.UID, snapshotUIDs}, {snap.Content, contents}, {snap.ContentUID, contentUIDs}} {
			if _, exists := identity.seen[identity.value]; exists {
				return nil, errors.New("snapshot object identities must be unique")
			}
			identity.seen[identity.value] = struct{}{}
		}
		requested, err := resource.ParseQuantity(volume.RequestedStorage)
		if err != nil {
			return nil, fmt.Errorf("parse requested storage for %q: %w", volume.Name, err)
		}
		restoreSize, err := resource.ParseQuantity(snap.RestoreSize)
		if err != nil || requested.Cmp(restoreSize) < 0 {
			return nil, fmt.Errorf("PVC %q requested storage is smaller than snapshot restore size", volume.Name)
		}
		snapshotByPVC[snap.SourcePVC] = snap
		handles[snap.SnapshotHandle] = struct{}{}
	}

	sort.Slice(allPVCs, func(i, j int) bool { return allPVCs[i].Name < allPVCs[j].Name })
	items := make([]any, 0, len(allPVCs)*3+1)
	for _, volume := range allPVCs {
		snap := snapshotByPVC[volume.Name]
		name := restoreObjectName(r.OperationID, volume.Name)
		labels := map[string]any{
			"app.kubernetes.io/managed-by": "kubebrain-cold-restore",
			"kubebrain.io/operation-id":    r.OperationID,
		}
		pvcLabels := make(map[string]any, len(volume.Labels)+1)
		for key, value := range volume.Labels {
			pvcLabels[key] = value
		}
		pvcLabels["kubebrain.io/operation-id"] = r.OperationID
		items = append(items,
			map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "metadata": map[string]any{"name": name, "labels": labels}, "spec": map[string]any{
				"deletionPolicy": "Retain", "driver": r.Inventory.VolumeSnapshotClass.Driver, "volumeSnapshotClassName": snapshotClass,
				"source":            map[string]any{"snapshotHandle": snap.SnapshotHandle},
				"volumeSnapshotRef": map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "name": name, "namespace": namespace},
			}},
			map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"name": name, "namespace": namespace, "labels": labels}, "spec": map[string]any{
				"volumeSnapshotClassName": snapshotClass, "source": map[string]any{"volumeSnapshotContentName": name},
			}},
			map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": volume.Name, "namespace": namespace, "labels": pvcLabels}, "spec": map[string]any{
				"accessModes": volume.AccessModes, "volumeMode": volume.VolumeMode, "storageClassName": storageClass,
				"resources":  map[string]any{"requests": map[string]any{"storage": volume.RequestedStorage}},
				"dataSource": map[string]any{"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": name},
			}},
		)
	}
	items = append(items, blueprint)
	return map[string]any{"apiVersion": "v1", "kind": "List", "items": items}, nil
}

func requireReplicaCount(spec map[string]any, component string, count int) error {
	value, ok := spec[component].(map[string]any)
	if !ok {
		return fmt.Errorf("TidbCluster blueprint has no %s spec", component)
	}
	replicas, ok := value["replicas"].(float64)
	if !ok || int(replicas) != count || replicas != float64(count) {
		return fmt.Errorf("%s PVC count does not match blueprint replicas", component)
	}
	return nil
}

func restoreObjectName(operation, pvcName string) string {
	digest := sha256.Sum256([]byte(operation + "\x00" + pvcName))
	prefix := strings.Trim(operation, "-")
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}
	return "kb-restore-" + prefix + "-" + hex.EncodeToString(digest[:6])
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

func cloneMap(value map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(encoded, &result)
	return result, err
}
