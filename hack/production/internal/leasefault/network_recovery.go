package leasefault

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"

	strictjson "sigs.k8s.io/json"
)

const networkRecoveryFile = "network-recovery.json"
const networkRecoveryLimit = 1 << 20

// NetworkRecovery is the parent's pre-mutation intent, not an API receipt.
// Independent admission must supply every identity and the two original JSON
// objects. Never infer this record from already-mutated resources. Reservation
// creation must additionally persist its real UID receipt before activation.
type NetworkRecovery struct {
	Owner          string          `json:"owner"`
	Namespace      string          `json:"namespace"`
	NamespaceUID   string          `json:"namespace_uid"`
	StatefulSetUID string          `json:"statefulset_uid"`
	PodName        string          `json:"pod_name"`
	PodUID         string          `json:"pod_uid"`
	PolicyName     string          `json:"policy_name"`
	Nonce          string          `json:"nonce"`
	ReservedNonce  string          `json:"reserved_nonce"`
	PodBefore      json.RawMessage `json:"pod_before"`
	ApprovedPolicy json.RawMessage `json:"approved_policy"`
}

func (p NetworkRecovery) encoded() ([]byte, error) {
	bad := errors.New("invalid network recovery intent")
	for _, id := range []string{p.Owner, p.Namespace, p.NamespaceUID, p.StatefulSetUID, p.PodName, p.PodUID, p.PolicyName, p.Nonce, p.ReservedNonce} {
		if !recoveryIdentity.MatchString(id) {
			return nil, bad
		}
	}
	if p.Nonce == p.ReservedNonce {
		return nil, bad
	}
	// RawMessage preserves full-width numbers and all original object fields.
	// Validate duplicate fields recursively before extracting identity fields.
	type object struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name              string            `json:"name"`
			Namespace         string            `json:"namespace"`
			UID               string            `json:"uid"`
			ResourceVersion   string            `json:"resourceVersion"`
			DeletionTimestamp *string           `json:"deletionTimestamp"`
			Labels            map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			EndpointSelector struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"endpointSelector"`
		} `json:"spec"`
		Specs json.RawMessage `json:"specs"`
	}
	objects := make([]object, 2)
	for i, raw := range []json.RawMessage{p.PodBefore, p.ApprovedPolicy} {
		if len(raw) == 0 || len(raw) > networkRecoveryLimit {
			return nil, bad
		}
		var fields map[string]interface{}
		strict, err := strictjson.UnmarshalStrict(raw, &fields, strictjson.DisallowDuplicateFields)
		if err != nil || len(strict) != 0 || fields == nil || strictjson.UnmarshalCaseSensitivePreserveInts(raw, &objects[i]) != nil {
			return nil, bad
		}
	}
	pod, policy := objects[0], objects[1]
	_, labeled := pod.Metadata.Labels["kubebrain.io/fault-owner"]
	if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Namespace != p.Namespace ||
		pod.Metadata.Name != p.PodName || pod.Metadata.UID != p.PodUID || pod.Metadata.ResourceVersion == "" ||
		pod.Metadata.DeletionTimestamp != nil || labeled || policy.APIVersion != "cilium.io/v2" ||
		policy.Kind != "CiliumNetworkPolicy" || policy.Metadata.Namespace != p.Namespace ||
		policy.Metadata.Name != p.PolicyName || policy.Metadata.DeletionTimestamp != nil ||
		policy.Spec.EndpointSelector.MatchLabels["kubebrain.io/fault-owner"] != p.Nonce ||
		(len(policy.Specs) != 0 && !bytes.Equal(bytes.TrimSpace(policy.Specs), []byte("null"))) {
		return nil, bad
	}
	data, err := json.Marshal(p)
	if err != nil || len(data) > networkRecoveryLimit {
		return nil, bad
	}
	return data, nil
}

// ArmNetworkRecovery must succeed BEFORE labeling the Pod or reserving a policy.
// Sync both file and directory; preserve any partial record on error and refuse
// overwrite. This is neither a lifecycle lock nor live Kubernetes admission.
func ArmNetworkRecovery(dir string, plan NetworkRecovery) error {
	data, err := plan.encoded()
	if err != nil {
		return err
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := r.OpenFile(networkRecoveryFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	d, err := r.Open(".")
	if err != nil {
		return err
	}
	err = d.Sync()
	return errors.Join(err, d.Close())
}

// LoadNetworkRecovery binds the saved record to independently retained admission.
// Missing or malformed records require reconciliation, never skipping cleanup.
// Returned original objects still need fresh namespace/Pod/policy observations
// and the actual reservation receipt before constructing any mutation plan.
func LoadNetworkRecovery(dir string, expected NetworkRecovery) (NetworkRecovery, error) {
	bad := errors.New("invalid network recovery record")
	want, err := expected.encoded()
	if err != nil {
		return NetworkRecovery{}, err
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return NetworkRecovery{}, err
	}
	defer r.Close()
	f, err := openRecoveryRecord(r, networkRecoveryFile)
	if err != nil {
		return NetworkRecovery{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > networkRecoveryLimit {
		return NetworkRecovery{}, bad
	}
	data, err := io.ReadAll(io.LimitReader(f, networkRecoveryLimit+1))
	if err != nil || len(data) > networkRecoveryLimit {
		return NetworkRecovery{}, bad
	}
	var plan NetworkRecovery
	strict, err := strictjson.UnmarshalStrict(data, &plan, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return NetworkRecovery{}, bad
	}
	got, err := plan.encoded()
	if err != nil || !bytes.Equal(got, want) {
		return NetworkRecovery{}, bad
	}
	return plan, nil
}
