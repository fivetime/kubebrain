package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
)

const TargetQualificationReceiptFormat = "kubebrain.native-pitr-target-qualification.v1"

type TargetQualificationReceipt struct {
	Format                    string   `json:"format"`
	TargetProvisioningSHA256  string   `json:"target_provisioning_sha256"`
	TargetSnapshotEmptySHA256 string   `json:"target_snapshot_empty_sha256"`
	WriterExclusionSHA256     string   `json:"writer_exclusion_sha256"`
	Namespace                 string   `json:"namespace"`
	TidbCluster               string   `json:"tidb_cluster"`
	TidbClusterUID            string   `json:"tidb_cluster_uid"`
	ClusterID                 uint64   `json:"cluster_id"`
	PDAddrs                   []string `json:"pd_addrs"`
	WholeTransactionalScan    bool     `json:"whole_transactional_scan"`
	VisibleCommittedKeyCount  uint64   `json:"visible_committed_key_count"`
	TargetEmpty               bool     `json:"target_empty"`
	WriterStatefulSetUID      string   `json:"writer_statefulset_uid"`
	WriterResourceVersion     string   `json:"writer_resource_version"`
	WriterObservedAtUnix      int64    `json:"writer_observed_at_unix"`
	KubeBrainWritersExcluded  bool     `json:"kubebrain_writers_excluded"`
	QualifiedAtUnix           int64    `json:"qualified_at_unix"`
	ReadOnlyQualification     bool     `json:"read_only_qualification"`
}

type TargetWriterExclusionEvidence struct {
	Namespace          string `json:"namespace"`
	StatefulSet        string `json:"statefulset"`
	StatefulSetUID     string `json:"statefulset_uid"`
	ResourceVersion    string `json:"resource_version"`
	DesiredReplicas    int64  `json:"desired_replicas"`
	CurrentReplicas    int64  `json:"current_replicas"`
	ReadyReplicas      int64  `json:"ready_replicas"`
	ObservedPodCount   int64  `json:"observed_pod_count"`
	ObservedAtUnix     int64  `json:"observed_at_unix"`
	ReadOnlyInspection bool   `json:"read_only_inspection"`
}

func (e TargetWriterExclusionEvidence) Validate() error {
	if e.Namespace != "kubebrain-system" || e.StatefulSet != "kubebrain" || !kubernetesUIDRE.MatchString(e.StatefulSetUID) || e.ResourceVersion == "" || e.DesiredReplicas != 0 || e.CurrentReplicas != 0 || e.ReadyReplicas != 0 || e.ObservedPodCount != 0 || e.ObservedAtUnix <= 0 || !e.ReadOnlyInspection {
		return errors.New("invalid replacement target KubeBrain writer exclusion evidence")
	}
	return nil
}

func VerifyTargetWriterExclusionContinuity(recorded, current TargetWriterExclusionEvidence) error {
	if err := recorded.Validate(); err != nil {
		return err
	}
	if err := current.Validate(); err != nil {
		return err
	}
	if recorded.Namespace != current.Namespace || recorded.StatefulSet != current.StatefulSet || recorded.StatefulSetUID != current.StatefulSetUID || recorded.ResourceVersion != current.ResourceVersion || current.ObservedAtUnix < recorded.ObservedAtUnix {
		return errors.New("replacement target KubeBrain writer exclusion changed after qualification")
	}
	return nil
}

func BuildTargetQualificationReceipt(provisioning TargetProvisioningReceipt, target TargetSnapshotEmptyReceipt, writers TargetWriterExclusionEvidence, provisioningSHA, targetSHA, writerSHA string, expectedPDAddrs []string, at int64) (TargetQualificationReceipt, error) {
	if err := VerifyTargetProvisioningBinding(provisioning, target); err != nil {
		return TargetQualificationReceipt{}, err
	}
	if err := writers.Validate(); err != nil {
		return TargetQualificationReceipt{}, err
	}
	expected := append([]string(nil), expectedPDAddrs...)
	slices.Sort(expected)
	if !sha256RE.MatchString(provisioningSHA) || !sha256RE.MatchString(targetSHA) || !sha256RE.MatchString(writerSHA) || len(expected) != provisioning.PDReplicas || !slices.Equal(expected, target.PDAddrs) || writers.ObservedAtUnix > target.CheckedAtUnix || at < target.CheckedAtUnix {
		return TargetQualificationReceipt{}, errors.New("target qualification does not bind the exact provisioning and PD endpoints")
	}
	for i, value := range expected {
		if value == "" || (i > 0 && expected[i-1] == value) {
			return TargetQualificationReceipt{}, errors.New("target qualification PD endpoints are invalid or duplicated")
		}
	}
	r := TargetQualificationReceipt{
		Format: TargetQualificationReceiptFormat, TargetProvisioningSHA256: provisioningSHA,
		TargetSnapshotEmptySHA256: targetSHA, WriterExclusionSHA256: writerSHA, Namespace: provisioning.Namespace, TidbCluster: provisioning.TidbCluster,
		TidbClusterUID: provisioning.TidbClusterUID, ClusterID: provisioning.ClusterID, PDAddrs: expected,
		WholeTransactionalScan: target.ScanScope == "whole-transactional-keyspace", VisibleCommittedKeyCount: target.VisibleCommittedKeyCount,
		TargetEmpty: target.VisibleCommittedKeyCount == 0, WriterStatefulSetUID: writers.StatefulSetUID, WriterResourceVersion: writers.ResourceVersion, WriterObservedAtUnix: writers.ObservedAtUnix,
		KubeBrainWritersExcluded: true, QualifiedAtUnix: at, ReadOnlyQualification: true,
	}
	return r, r.Validate()
}

func (r TargetQualificationReceipt) Validate() error {
	if r.Format != TargetQualificationReceiptFormat || !sha256RE.MatchString(r.TargetProvisioningSHA256) || !sha256RE.MatchString(r.TargetSnapshotEmptySHA256) || !sha256RE.MatchString(r.WriterExclusionSHA256) || !dnsLabel.MatchString(r.Namespace) || !dnsLabel.MatchString(r.TidbCluster) || !kubernetesUIDRE.MatchString(r.TidbClusterUID) || r.ClusterID == 0 || len(r.PDAddrs) == 0 || !slices.IsSorted(r.PDAddrs) || !r.WholeTransactionalScan || r.VisibleCommittedKeyCount != 0 || !r.TargetEmpty || !kubernetesUIDRE.MatchString(r.WriterStatefulSetUID) || r.WriterResourceVersion == "" || r.WriterObservedAtUnix <= 0 || !r.KubeBrainWritersExcluded || r.QualifiedAtUnix <= 0 || !r.ReadOnlyQualification {
		return errors.New("invalid native PITR target qualification receipt")
	}
	for i, value := range r.PDAddrs {
		if value == "" || (i > 0 && r.PDAddrs[i-1] == value) {
			return errors.New("target qualification contains invalid or duplicate PD endpoints")
		}
	}
	return nil
}

func VerifyTargetQualificationBinding(r TargetQualificationReceipt, provisioning TargetProvisioningReceipt, target TargetSnapshotEmptyReceipt, writers TargetWriterExclusionEvidence, provisioningSHA, targetSHA, writerSHA string, expectedPDAddrs []string) error {
	if err := r.Validate(); err != nil {
		return err
	}
	expected, err := BuildTargetQualificationReceipt(provisioning, target, writers, provisioningSHA, targetSHA, writerSHA, expectedPDAddrs, r.QualifiedAtUnix)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("target qualification receipt does not match its exact source evidence")
	}
	return nil
}

func DecodeTargetQualificationReceipt(reader io.Reader) (TargetQualificationReceipt, error) {
	var r TargetQualificationReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("decode target qualification receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return r, errors.New("target qualification receipt contains trailing JSON")
	}
	return r, r.Validate()
}

func DecodeTargetWriterExclusionEvidence(reader io.Reader) (TargetWriterExclusionEvidence, error) {
	var e TargetWriterExclusionEvidence
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return e, fmt.Errorf("decode target writer exclusion evidence: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return e, errors.New("target writer exclusion evidence contains trailing JSON")
	}
	return e, e.Validate()
}
