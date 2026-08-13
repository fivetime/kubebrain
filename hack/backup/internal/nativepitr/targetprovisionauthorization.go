package nativepitr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const TargetProvisionAuthorizationFormat = "kubebrain.native-pitr-target-provision-authorization.v1"
const TargetProvisionCreationFormat = "kubebrain.native-pitr-target-provision-creation.v1"

type ReplacementTidbClusterManifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name        string            `json:"name"`
		Namespace   string            `json:"namespace"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec json.RawMessage `json:"spec"`
}

type TargetProvisionAuthorization struct {
	Format                  string `json:"format"`
	RetirementReceiptSHA256 string `json:"retirement_receipt_sha256"`
	OldProvisioningSHA256   string `json:"old_target_provisioning_sha256"`
	ManifestSHA256          string `json:"manifest_sha256"`
	Namespace               string `json:"namespace"`
	TidbCluster             string `json:"tidb_cluster"`
	OldTidbClusterUID       string `json:"old_tidb_cluster_uid"`
	OldClusterID            uint64 `json:"old_cluster_id"`
	PDReplicas              int    `json:"pd_replicas"`
	TiKVReplicas            int    `json:"tikv_replicas"`
	AuthorizationID         string `json:"authorization_id"`
	ManifestAuthorizationID string `json:"manifest_authorization_id"`
	AuthorizedAtUnix        int64  `json:"authorized_at_unix"`
}

type TargetProvisionCreationReceipt struct {
	Format                 string `json:"format"`
	AuthorizationSHA256    string `json:"authorization_sha256"`
	ManifestSHA256         string `json:"manifest_sha256"`
	Namespace              string `json:"namespace"`
	TidbCluster            string `json:"tidb_cluster"`
	TidbClusterUID         string `json:"tidb_cluster_uid"`
	ResourceVersion        string `json:"resource_version"`
	AdmittedIdentitySHA256 string `json:"admitted_identity_sha256"`
	CreatedAtUnix          int64  `json:"created_at_unix"`
	ServerDryRunPassed     bool   `json:"server_dry_run_passed"`
}

type AdmittedTidbCluster struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		UID             string            `json:"uid"`
		ResourceVersion string            `json:"resourceVersion"`
		Labels          map[string]string `json:"labels"`
		Annotations     map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec any `json:"spec"`
}

func DecodeReplacementTidbClusterManifest(r io.Reader) (ReplacementTidbClusterManifest, error) {
	var m ReplacementTidbClusterManifest
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, fmt.Errorf("decode replacement TidbCluster manifest: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return m, errors.New("replacement TidbCluster manifest contains trailing JSON")
	}
	return m, m.Validate()
}

func (m ReplacementTidbClusterManifest) Validate() error {
	if m.APIVersion != "pingcap.com/v1alpha1" || m.Kind != "TidbCluster" || !dnsLabel.MatchString(m.Metadata.Namespace) || !dnsLabel.MatchString(m.Metadata.Name) || len(m.Spec) == 0 {
		return errors.New("invalid replacement TidbCluster manifest identity")
	}
	if m.Metadata.Labels["app.kubernetes.io/name"] != "tidb-cluster" || m.Metadata.Labels["app.kubernetes.io/instance"] != m.Metadata.Name {
		return errors.New("replacement TidbCluster ownership labels are invalid")
	}
	if len(m.Metadata.Annotations) != 1 || m.Metadata.Annotations["dbaas.kubebrain.io/native-pitr-provision-authorization"] == "" {
		return errors.New("replacement TidbCluster authorization annotation is invalid")
	}
	var spec map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(m.Spec))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return errors.New("replacement TidbCluster spec is invalid")
	}
	for _, forbidden := range []string{"paused", "initializer", "bootstrapSQLConfigMapName", "tidb", "tiflash", "ticdc", "pump", "binlog", "dm"} {
		if _, ok := spec[forbidden]; ok {
			return fmt.Errorf("replacement TidbCluster spec contains forbidden field %s", forbidden)
		}
	}
	var reclaim string
	if json.Unmarshal(spec["pvReclaimPolicy"], &reclaim) != nil || reclaim != "Retain" {
		return errors.New("replacement TidbCluster must retain provisioned PVs")
	}
	for _, component := range []string{"pd", "tikv"} {
		var value struct {
			Replicas int `json:"replicas"`
			Requests struct {
				Storage string `json:"storage"`
			} `json:"requests"`
		}
		if json.Unmarshal(spec[component], &value) != nil || value.Replicas <= 0 || value.Requests.Storage == "" {
			return fmt.Errorf("replacement TidbCluster %s topology/storage is invalid", component)
		}
	}
	return nil
}

func BuildTargetProvisionAuthorization(retirement TargetRetirementReceipt, old TargetProvisioningReceipt, retirementSHA, oldSHA, manifestSHA, authorizationID string, manifest ReplacementTidbClusterManifest, at int64) (TargetProvisionAuthorization, error) {
	if err := retirement.Validate(); err != nil {
		return TargetProvisionAuthorization{}, err
	}
	if err := old.Validate(); err != nil {
		return TargetProvisionAuthorization{}, err
	}
	if err := manifest.Validate(); err != nil {
		return TargetProvisionAuthorization{}, err
	}
	if retirement.OldTargetProvisioningSHA256 != oldSHA || retirement.Namespace != old.Namespace || retirement.TidbCluster != old.TidbCluster || retirement.OldTidbClusterUID != old.TidbClusterUID || retirement.OldClusterID != old.ClusterID || retirement.CompletedAtUnix < old.ObservedAtUnix || manifest.Metadata.Namespace != old.Namespace || manifest.Metadata.Name != old.TidbCluster {
		return TargetProvisionAuthorization{}, errors.New("target provision authorization does not bind the retired target identity")
	}
	manifestAuthorizationID := manifest.Metadata.Annotations["dbaas.kubebrain.io/native-pitr-provision-authorization"]
	if !sha256RE.MatchString(retirementSHA) || !sha256RE.MatchString(oldSHA) || !sha256RE.MatchString(manifestSHA) || !operationIDRE.MatchString(authorizationID) || !operationIDRE.MatchString(manifestAuthorizationID) || at <= 0 {
		return TargetProvisionAuthorization{}, errors.New("invalid target provision authorization inputs")
	}
	var pd, tikv struct {
		Replicas int `json:"replicas"`
	}
	var spec map[string]json.RawMessage
	_ = json.Unmarshal(manifest.Spec, &spec)
	_ = json.Unmarshal(spec["pd"], &pd)
	_ = json.Unmarshal(spec["tikv"], &tikv)
	a := TargetProvisionAuthorization{Format: TargetProvisionAuthorizationFormat, RetirementReceiptSHA256: retirementSHA, OldProvisioningSHA256: oldSHA, ManifestSHA256: manifestSHA, Namespace: old.Namespace, TidbCluster: old.TidbCluster, OldTidbClusterUID: old.TidbClusterUID, OldClusterID: old.ClusterID, PDReplicas: pd.Replicas, TiKVReplicas: tikv.Replicas, AuthorizationID: authorizationID, ManifestAuthorizationID: manifestAuthorizationID, AuthorizedAtUnix: at}
	return a, a.Validate()
}

func (a TargetProvisionAuthorization) Validate() error {
	if a.Format != TargetProvisionAuthorizationFormat || !sha256RE.MatchString(a.RetirementReceiptSHA256) || !sha256RE.MatchString(a.OldProvisioningSHA256) || !sha256RE.MatchString(a.ManifestSHA256) || !dnsLabel.MatchString(a.Namespace) || !dnsLabel.MatchString(a.TidbCluster) || !kubernetesUIDRE.MatchString(a.OldTidbClusterUID) || a.OldClusterID == 0 || a.PDReplicas <= 0 || a.TiKVReplicas <= 0 || !operationIDRE.MatchString(a.AuthorizationID) || !operationIDRE.MatchString(a.ManifestAuthorizationID) || a.AuthorizedAtUnix <= 0 {
		return errors.New("invalid target provision authorization")
	}
	return nil
}
func (r TargetProvisionCreationReceipt) Validate() error {
	if r.Format != TargetProvisionCreationFormat || !sha256RE.MatchString(r.AuthorizationSHA256) || !sha256RE.MatchString(r.ManifestSHA256) || !sha256RE.MatchString(r.AdmittedIdentitySHA256) || !dnsLabel.MatchString(r.Namespace) || !dnsLabel.MatchString(r.TidbCluster) || !kubernetesUIDRE.MatchString(r.TidbClusterUID) || r.ResourceVersion == "" || r.CreatedAtUnix <= 0 || !r.ServerDryRunPassed {
		return errors.New("invalid target provision creation receipt")
	}
	return nil
}

func DecodeAdmittedTidbCluster(reader io.Reader) (AdmittedTidbCluster, error) {
	var object AdmittedTidbCluster
	dec := json.NewDecoder(reader)
	if err := dec.Decode(&object); err != nil {
		return object, fmt.Errorf("decode admitted TidbCluster: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return object, errors.New("admitted TidbCluster contains trailing JSON")
	}
	if object.APIVersion != "pingcap.com/v1alpha1" || object.Kind != "TidbCluster" || !dnsLabel.MatchString(object.Metadata.Namespace) || !dnsLabel.MatchString(object.Metadata.Name) || object.Spec == nil {
		return object, errors.New("invalid admitted TidbCluster identity")
	}
	return object, nil
}

func AdmittedTidbClusterIdentitySHA256(object AdmittedTidbCluster) (string, error) {
	identity := struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name        string            `json:"name"`
			Namespace   string            `json:"namespace"`
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
		Spec any `json:"spec"`
	}{APIVersion: object.APIVersion, Kind: object.Kind, Spec: object.Spec}
	identity.Metadata.Name = object.Metadata.Name
	identity.Metadata.Namespace = object.Metadata.Namespace
	identity.Metadata.Labels = object.Metadata.Labels
	identity.Metadata.Annotations = object.Metadata.Annotations
	digest, _, err := DigestCanonicalJSON(identity)
	return digest, err
}

func VerifyCurrentTargetProvisioningObject(a TargetProvisionAuthorization, creation TargetProvisionCreationReceipt, object AdmittedTidbCluster, authorizationSHA string) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if err := creation.Validate(); err != nil {
		return err
	}
	digest, err := AdmittedTidbClusterIdentitySHA256(object)
	if err != nil {
		return err
	}
	if creation.AuthorizationSHA256 != authorizationSHA || creation.ManifestSHA256 != a.ManifestSHA256 || object.Metadata.Namespace != creation.Namespace || object.Metadata.Name != creation.TidbCluster || object.Metadata.UID != creation.TidbClusterUID || object.Metadata.Annotations["dbaas.kubebrain.io/native-pitr-provision-authorization"] != a.ManifestAuthorizationID || digest != creation.AdmittedIdentitySHA256 {
		return errors.New("current replacement TidbCluster does not match the admitted creation identity")
	}
	return nil
}
func BuildTargetProvisionCreationReceipt(a TargetProvisionAuthorization, authorizationSHA string, r TargetProvisionCreationReceipt) (TargetProvisionCreationReceipt, error) {
	if err := a.Validate(); err != nil {
		return r, err
	}
	if r.AuthorizationSHA256 != authorizationSHA || r.ManifestSHA256 != a.ManifestSHA256 || r.Namespace != a.Namespace || r.TidbCluster != a.TidbCluster || r.TidbClusterUID == a.OldTidbClusterUID || r.CreatedAtUnix < a.AuthorizedAtUnix {
		return r, errors.New("target provision creation does not bind the exact authorization or distinct UID")
	}
	return r, r.Validate()
}
func VerifyAuthorizedReplacementProvisioning(a TargetProvisionAuthorization, creation TargetProvisionCreationReceipt, old, newReceipt TargetProvisioningReceipt, authorizationSHA string) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if err := creation.Validate(); err != nil {
		return err
	}
	if err := old.Validate(); err != nil {
		return err
	}
	if err := newReceipt.Validate(); err != nil {
		return err
	}
	if creation.AuthorizationSHA256 != authorizationSHA || a.Namespace != old.Namespace || a.TidbCluster != old.TidbCluster || a.OldTidbClusterUID != old.TidbClusterUID || a.OldClusterID != old.ClusterID || newReceipt.Namespace != a.Namespace || newReceipt.TidbCluster != a.TidbCluster || newReceipt.TidbClusterUID != creation.TidbClusterUID || newReceipt.ClusterID == old.ClusterID || newReceipt.PDReplicas != a.PDReplicas || newReceipt.TiKVReplicas != a.TiKVReplicas || newReceipt.ObservedAtUnix < creation.CreatedAtUnix {
		return errors.New("completed provisioning does not bind the authorized distinct target")
	}
	oldPVC, oldPV, oldStorage := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, v := range old.Volumes {
		oldPVC[v.PVCUID] = true
		oldPV[v.PVUID] = true
		oldStorage[v.CSIDriver+"\x00"+v.VolumeHandle] = true
	}
	for _, v := range newReceipt.Volumes {
		if oldPVC[v.PVCUID] || oldPV[v.PVUID] || oldStorage[v.CSIDriver+"\x00"+v.VolumeHandle] {
			return errors.New("completed provisioning reuses retired storage identity")
		}
	}
	return nil
}
func DecodeTargetProvisionAuthorization(reader io.Reader) (TargetProvisionAuthorization, error) {
	var a TargetProvisionAuthorization
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&a); err != nil {
		return a, err
	}
	if err := requireEOF(dec); err != nil {
		return a, errors.New("target provision authorization contains trailing JSON")
	}
	return a, a.Validate()
}
func DecodeTargetProvisionCreationReceipt(reader io.Reader) (TargetProvisionCreationReceipt, error) {
	var r TargetProvisionCreationReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, err
	}
	if err := requireEOF(dec); err != nil {
		return r, errors.New("target provision creation receipt contains trailing JSON")
	}
	return r, r.Validate()
}
func DigestCanonicalJSON(v any) (string, []byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", nil, err
	}
	b = append(b, '\n')
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), b, nil
}
