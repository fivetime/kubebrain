package jwtrotationpublisher

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
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	ReceiptFormat = "kubebrain.jwt-key-rotation.publish-receipt.v1"
	maxKeyBytes   = 1 << 20
)

type Kubernetes interface {
	Get(context.Context, string, string, string) ([]byte, bool, error)
	Create(context.Context, string, []byte) error
	Patch(context.Context, string, string, string, []byte) error
}

type Options struct {
	Phase, OperationID, Instance, Namespace, StatefulSet, Secret string
	OldKeyField, NewKeyField, OldKeySource, NewKeySource         string
	OldKeySHA256, NewKeySHA256                                   string
	KeyVolume, KeyMountDir, SignMethod                           string
	ExpectedReplicas, JWTTokenTTL                                int64
	PreviousReceipt, ReceiptOutput                               string
	PollInterval, RolloutTimeout                                 time.Duration
}

type Receipt struct {
	Format                string `json:"format"`
	Phase                 string `json:"phase"`
	OperationID           string `json:"operation_id"`
	Instance              string `json:"instance"`
	Namespace             string `json:"namespace"`
	StatefulSet           string `json:"statefulset"`
	StatefulSetUID        string `json:"statefulset_uid"`
	Secret                string `json:"secret"`
	SecretUID             string `json:"secret_uid"`
	SecretResourceVersion string `json:"secret_resource_version"`
	SecretDataSHA256      string `json:"secret_data_sha256"`
	OldKeySHA256          string `json:"old_key_sha256"`
	NewKeySHA256          string `json:"new_key_sha256"`
	TemplateBaselineSHA   string `json:"template_baseline_sha256"`
	BeforeRevision        string `json:"before_revision"`
	AfterRevision         string `json:"after_revision"`
	AuthTokenArgSHA256    string `json:"auth_token_arg_sha256"`
	PreviousReceiptSHA256 string `json:"previous_receipt_sha256"`
	Replicas              int64  `json:"replicas"`
	ObservedAtUnix        int64  `json:"observed_at_unix"`
	RolledOut             bool   `json:"rolled_out"`
	ReconciledExisting    bool   `json:"reconciled_existing"`
}

type objectMeta struct {
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resourceVersion"`
	Generation      int64             `json:"generation"`
	Annotations     map[string]string `json:"annotations"`
}

type secretObject struct {
	Metadata  objectMeta        `json:"metadata"`
	Immutable *bool             `json:"immutable"`
	Type      string            `json:"type"`
	Data      map[string]string `json:"data"`
}

type statefulSetObject struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int64 `json:"replicas"`
		Template struct {
			Metadata objectMeta `json:"metadata"`
			Spec     struct {
				Containers []struct {
					Name         string   `json:"name"`
					Args         []string `json:"args"`
					VolumeMounts []struct {
						Name, MountPath string
						ReadOnly        bool   `json:"readOnly"`
						SubPath         string `json:"subPath"`
					} `json:"volumeMounts"`
				} `json:"containers"`
				Volumes []struct {
					Name   string `json:"name"`
					Secret *struct {
						SecretName string `json:"secretName"`
					} `json:"secret"`
				} `json:"volumes"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64  `json:"observedGeneration"`
		ReadyReplicas      int64  `json:"readyReplicas"`
		UpdatedReplicas    int64  `json:"updatedReplicas"`
		CurrentRevision    string `json:"currentRevision"`
		UpdateRevision     string `json:"updateRevision"`
	} `json:"status"`
}

type stateEvidence struct {
	object                       statefulSetObject
	raw                          map[string]any
	containerIndex, authArgIndex int
	authArg, revision, baseline  string
}

func Run(ctx context.Context, kube Kubernetes, o Options, now func() time.Time) (Receipt, error) {
	if err := validateOptions(o); err != nil {
		return Receipt{}, err
	}
	oldKey, err := readPrivateFile(o.OldKeySource)
	if err != nil {
		return Receipt{}, fmt.Errorf("old key: %w", err)
	}
	newKey, err := readPrivateFile(o.NewKeySource)
	if err != nil {
		return Receipt{}, fmt.Errorf("new key: %w", err)
	}
	if digestBytes(oldKey) != o.OldKeySHA256 || digestBytes(newKey) != o.NewKeySHA256 {
		return Receipt{}, errors.New("key source digest does not match approved parameters")
	}
	secret, err := ensureSecret(ctx, kube, o, oldKey, newKey)
	if err != nil {
		return Receipt{}, err
	}
	secretDigest := digestJSONLine(secret.Data)

	desired, predecessor := desiredAuthArgs(o)
	previousDigest := ""
	var previous Receipt
	if o.Phase != "phase-a" {
		previous, previousDigest, err = readReceipt(o.PreviousReceipt)
		if err != nil {
			return Receipt{}, fmt.Errorf("previous receipt: %w", err)
		}
		if err := validatePrevious(previous, o, secret, secretDigest); err != nil {
			return Receipt{}, err
		}
	}

	current, err := getStatefulSet(ctx, kube, o)
	if err != nil {
		return Receipt{}, err
	}
	if err := validateStateBinding(current, o); err != nil {
		return Receipt{}, err
	}
	managedDesired := current.authArg == desired && phaseMetadataMatches(current.object, o, secretDigest)
	if !rolloutComplete(current.object, o.ExpectedReplicas) && !managedDesired {
		return Receipt{}, errors.New("StatefulSet must be fully rolled out before publishing a JWT phase")
	}
	if o.Phase != "phase-a" && current.object.Metadata.UID != previous.StatefulSetUID {
		return Receipt{}, errors.New("StatefulSet UID changed between phases")
	}
	if current.baseline != previousBaseline(o.Phase, previous, current.baseline) {
		return Receipt{}, errors.New("StatefulSet template baseline changed between phases")
	}
	if current.authArg != predecessor && current.authArg != desired {
		return Receipt{}, fmt.Errorf("phase %s cannot follow current auth-token configuration", o.Phase)
	}

	if _, err := os.Lstat(o.ReceiptOutput); err == nil {
		existing, _, err := readReceipt(o.ReceiptOutput)
		if err != nil {
			return Receipt{}, fmt.Errorf("existing receipt: %w", err)
		}
		if err := validateExisting(existing, o, secret, secretDigest, current, desired, previousDigest); err != nil {
			return Receipt{}, err
		}
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Receipt{}, err
	}

	beforeRevision := current.revision
	if o.Phase != "phase-a" {
		beforeRevision = previous.AfterRevision
	}
	reconciledExisting := managedDesired
	if !reconciledExisting {
		patch, err := buildPatch(current, desired, secretDigest, o)
		if err != nil {
			return Receipt{}, err
		}
		if err := kube.Patch(ctx, o.Namespace, "statefulset", o.StatefulSet, patch); err != nil {
			return Receipt{}, fmt.Errorf("patch StatefulSet: %w", err)
		}
	}

	deadline := time.Now().Add(o.RolloutTimeout)
	for {
		current, err = getStatefulSet(ctx, kube, o)
		if err == nil && current.authArg == desired && phaseMetadataMatches(current.object, o, secretDigest) && rolloutComplete(current.object, o.ExpectedReplicas) {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				return Receipt{}, fmt.Errorf("wait for rollout: %w", err)
			}
			return Receipt{}, errors.New("timed out waiting for StatefulSet rollout")
		}
		select {
		case <-ctx.Done():
			return Receipt{}, ctx.Err()
		case <-time.After(o.PollInterval):
		}
	}
	if err := validateStateBinding(current, o); err != nil {
		return Receipt{}, fmt.Errorf("StatefulSet binding changed during rollout: %w", err)
	}
	if current.object.Metadata.UID == "" || (o.Phase != "phase-a" && current.object.Metadata.UID != previous.StatefulSetUID) {
		return Receipt{}, errors.New("StatefulSet UID changed during rollout")
	}
	if current.baseline != previousBaseline(o.Phase, previous, current.baseline) {
		return Receipt{}, errors.New("StatefulSet template baseline changed during rollout")
	}
	if o.Phase != "phase-a" && current.revision == previous.AfterRevision {
		return Receipt{}, errors.New("adjacent phases must use different StatefulSet revisions")
	}
	receipt := Receipt{
		Format: ReceiptFormat, Phase: o.Phase, OperationID: o.OperationID, Instance: o.Instance,
		Namespace: o.Namespace, StatefulSet: o.StatefulSet, StatefulSetUID: current.object.Metadata.UID,
		Secret: o.Secret, SecretUID: secret.Metadata.UID, SecretResourceVersion: secret.Metadata.ResourceVersion,
		SecretDataSHA256: secretDigest, OldKeySHA256: o.OldKeySHA256, NewKeySHA256: o.NewKeySHA256,
		TemplateBaselineSHA: current.baseline, BeforeRevision: beforeRevision,
		AfterRevision: current.revision, AuthTokenArgSHA256: digestString(desired), PreviousReceiptSHA256: previousDigest,
		Replicas: o.ExpectedReplicas, ObservedAtUnix: now().Unix(), RolledOut: true, ReconciledExisting: reconciledExisting,
	}
	if receipt.ObservedAtUnix <= 0 {
		return Receipt{}, errors.New("observed time must be positive")
	}
	if err := publishReceipt(o.ReceiptOutput, receipt); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// RollbackPhaseCToB conservatively restores the overlap configuration after an
// unreceipted phase C rollout failure. It never rewrites phase receipts.
func RollbackPhaseCToB(ctx context.Context, kube Kubernetes, o Options) error {
	if err := validateOptions(o); err != nil {
		return err
	}
	if o.Phase != "phase-c" {
		return errors.New("rollback requires phase-c options")
	}
	if _, err := os.Lstat(o.ReceiptOutput); err == nil {
		return errors.New("refusing to roll back a receipted phase C")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	oldKey, err := readPrivateFile(o.OldKeySource)
	if err != nil {
		return fmt.Errorf("old key: %w", err)
	}
	newKey, err := readPrivateFile(o.NewKeySource)
	if err != nil {
		return fmt.Errorf("new key: %w", err)
	}
	if digestBytes(oldKey) != o.OldKeySHA256 || digestBytes(newKey) != o.NewKeySHA256 {
		return errors.New("key source digest does not match approved parameters")
	}
	secret, err := ensureSecret(ctx, kube, o, oldKey, newKey)
	if err != nil {
		return err
	}
	secretDigest := digestJSONLine(secret.Data)
	previous, _, err := readReceipt(o.PreviousReceipt)
	if err != nil {
		return fmt.Errorf("previous receipt: %w", err)
	}
	if err := validatePrevious(previous, o, secret, secretDigest); err != nil {
		return err
	}
	current, err := getStatefulSet(ctx, kube, o)
	if err != nil {
		return err
	}
	if err := validateStateBinding(current, o); err != nil {
		return err
	}
	if current.object.Metadata.UID != previous.StatefulSetUID || current.baseline != previous.TemplateBaselineSHA {
		return errors.New("StatefulSet identity or template baseline changed before rollback")
	}
	phaseC, phaseB := desiredAuthArgs(o)
	if current.authArg != phaseC && current.authArg != phaseB {
		return errors.New("phase C rollback found an unsupported auth-token configuration")
	}
	rollbackOptions := o
	rollbackOptions.Phase = "phase-b"
	if current.authArg != phaseB || !phaseMetadataMatches(current.object, rollbackOptions, secretDigest) {
		patch, err := buildPatch(current, phaseB, secretDigest, rollbackOptions)
		if err != nil {
			return err
		}
		if err := kube.Patch(ctx, o.Namespace, "statefulset", o.StatefulSet, patch); err != nil {
			return fmt.Errorf("patch StatefulSet rollback: %w", err)
		}
	}
	deadline := time.Now().Add(o.RolloutTimeout)
	for {
		current, err = getStatefulSet(ctx, kube, o)
		if err == nil && current.authArg == phaseB && phaseMetadataMatches(current.object, rollbackOptions, secretDigest) && rolloutComplete(current.object, o.ExpectedReplicas) {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("wait for rollback: %w", err)
			}
			return errors.New("timed out waiting for phase B rollback")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.PollInterval):
		}
	}
	if err := validateStateBinding(current, o); err != nil {
		return fmt.Errorf("StatefulSet binding changed during rollback: %w", err)
	}
	if current.object.Metadata.UID != previous.StatefulSetUID || current.baseline != previous.TemplateBaselineSHA {
		return errors.New("StatefulSet identity or template baseline changed during rollback")
	}
	return nil
}

func validateOptions(o Options) error {
	if !slices.Contains([]string{"phase-a", "phase-b", "phase-c"}, o.Phase) {
		return errors.New("phase must be phase-a, phase-b, or phase-c")
	}
	identifier := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	for name, value := range map[string]string{"operation ID": o.OperationID, "instance": o.Instance, "namespace": o.Namespace, "StatefulSet": o.StatefulSet, "Secret": o.Secret, "old key field": o.OldKeyField, "new key field": o.NewKeyField, "key volume": o.KeyVolume} {
		if !identifier.MatchString(value) {
			return fmt.Errorf("%s is invalid", name)
		}
	}
	if o.OldKeyField == o.NewKeyField || !filepath.IsAbs(o.KeyMountDir) || filepath.Clean(o.KeyMountDir) != o.KeyMountDir || o.KeyMountDir == "/" || o.ExpectedReplicas <= 0 || o.ExpectedReplicas > 2147483647 || o.JWTTokenTTL <= 0 || o.JWTTokenTTL > 2147483647 {
		return errors.New("key fields, mount directory, replicas, or JWT TTL are invalid")
	}
	if !slices.Contains([]string{"HS256", "RS256", "PS256", "ES256", "EdDSA"}, o.SignMethod) {
		return errors.New("sign method is unsupported")
	}
	if !validDigest(o.OldKeySHA256) || !validDigest(o.NewKeySHA256) {
		return errors.New("approved key SHA-256 values are required")
	}
	if o.ReceiptOutput == "" || !filepath.IsAbs(o.ReceiptOutput) || o.PollInterval <= 0 || o.RolloutTimeout <= 0 {
		return errors.New("absolute receipt output and positive rollout timing are required")
	}
	if o.Phase == "phase-a" && o.PreviousReceipt != "" || o.Phase != "phase-a" && (o.PreviousReceipt == "" || !filepath.IsAbs(o.PreviousReceipt)) {
		return errors.New("previous receipt is forbidden for phase A and required for later phases")
	}
	if o.PreviousReceipt != "" && filepath.Clean(o.PreviousReceipt) == filepath.Clean(o.ReceiptOutput) {
		return errors.New("previous and output receipt paths must differ")
	}
	return nil
}

func readPrivateFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&0o077 != 0 || info.Size() <= 0 || info.Size() > maxKeyBytes {
		return nil, errors.New("must be a 1..1 MiB regular non-symlink file inaccessible to group/other")
	}
	return os.ReadFile(path)
}

func ensureSecret(ctx context.Context, kube Kubernetes, o Options, oldKey, newKey []byte) (secretObject, error) {
	raw, found, err := kube.Get(ctx, o.Namespace, "secret", o.Secret)
	if err != nil {
		return secretObject{}, err
	}
	if !found {
		if o.Phase != "phase-a" {
			return secretObject{}, errors.New("immutable key Secret is missing after phase A")
		}
		object := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": o.Secret, "namespace": o.Namespace, "annotations": map[string]any{"dbaas.kubebrain.io/jwt-key-rotation-operation": o.OperationID}}, "immutable": true, "type": "Opaque", "data": map[string]string{o.OldKeyField: base64.StdEncoding.EncodeToString(oldKey), o.NewKeyField: base64.StdEncoding.EncodeToString(newKey)}}
		encoded, _ := json.Marshal(object)
		if createErr := kube.Create(ctx, o.Namespace, encoded); createErr != nil {
			raw, found, err = kube.Get(ctx, o.Namespace, "secret", o.Secret)
			if err != nil || !found {
				return secretObject{}, fmt.Errorf("create immutable key Secret: %w", createErr)
			}
		} else {
			raw, found, err = kube.Get(ctx, o.Namespace, "secret", o.Secret)
		}
		if err != nil || !found {
			return secretObject{}, errors.New("created immutable key Secret cannot be read")
		}
	}
	var secret secretObject
	if err := json.Unmarshal(raw, &secret); err != nil {
		return secretObject{}, fmt.Errorf("decode Secret: %w", err)
	}
	if secret.Immutable == nil || !*secret.Immutable || secret.Type != "Opaque" || secret.Metadata.UID == "" || secret.Metadata.ResourceVersion == "" || len(secret.Data) != 2 {
		return secretObject{}, errors.New("key Secret identity, immutability, type, or field count is invalid")
	}
	if secret.Metadata.Annotations["dbaas.kubebrain.io/jwt-key-rotation-operation"] != o.OperationID {
		return secretObject{}, errors.New("key Secret is not bound to this rotation operation")
	}
	for field, expected := range map[string][]byte{o.OldKeyField: oldKey, o.NewKeyField: newKey} {
		value, ok := secret.Data[field]
		decoded, decodeErr := base64.StdEncoding.DecodeString(value)
		if !ok || decodeErr != nil || !bytes.Equal(decoded, expected) {
			return secretObject{}, fmt.Errorf("key Secret field %s does not match approved source", field)
		}
	}
	return secret, nil
}

func getStatefulSet(ctx context.Context, kube Kubernetes, o Options) (stateEvidence, error) {
	raw, found, err := kube.Get(ctx, o.Namespace, "statefulset", o.StatefulSet)
	if err != nil || !found {
		return stateEvidence{}, errors.New("StatefulSet is missing or unreadable")
	}
	var object statefulSetObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return stateEvidence{}, err
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return stateEvidence{}, err
	}
	e := stateEvidence{object: object, raw: generic, containerIndex: -1, authArgIndex: -1, revision: object.Status.UpdateRevision}
	for i, container := range object.Spec.Template.Spec.Containers {
		if container.Name != "kubebrain" {
			continue
		}
		if e.containerIndex >= 0 {
			return stateEvidence{}, errors.New("StatefulSet has duplicate kubebrain containers")
		}
		e.containerIndex = i
		for j, arg := range container.Args {
			if strings.HasPrefix(arg, "--auth-token=") {
				if e.authArgIndex >= 0 {
					return stateEvidence{}, errors.New("StatefulSet has duplicate auth-token arguments")
				}
				e.authArgIndex, e.authArg = j, arg
			}
		}
	}
	if e.containerIndex < 0 || e.authArgIndex < 0 {
		return stateEvidence{}, errors.New("StatefulSet kubebrain auth-token argument is missing")
	}
	baseline, err := templateBaseline(generic, e.containerIndex, e.authArgIndex)
	if err != nil {
		return stateEvidence{}, err
	}
	e.baseline = baseline
	return e, nil
}

func validateStateBinding(e stateEvidence, o Options) error {
	if e.object.Metadata.UID == "" || e.object.Metadata.ResourceVersion == "" || e.object.Metadata.Generation <= 0 || e.object.Spec.Replicas == nil || *e.object.Spec.Replicas != o.ExpectedReplicas {
		return errors.New("StatefulSet identity or replica count is invalid")
	}
	container := e.object.Spec.Template.Spec.Containers[e.containerIndex]
	mountOK := false
	for _, mount := range container.VolumeMounts {
		if mount.Name == o.KeyVolume && mount.MountPath == o.KeyMountDir && mount.ReadOnly && mount.SubPath == "" {
			mountOK = true
		}
	}
	volumeOK := false
	for _, volume := range e.object.Spec.Template.Spec.Volumes {
		if volume.Name == o.KeyVolume && volume.Secret != nil && volume.Secret.SecretName == o.Secret {
			volumeOK = true
		}
	}
	if !mountOK || !volumeOK {
		return errors.New("StatefulSet key Secret volume binding is invalid")
	}
	ttl := fmt.Sprintf("--auth-token-ttl=%d", o.JWTTokenTTL)
	ttlCount := 0
	for _, arg := range container.Args {
		if strings.HasPrefix(arg, "--auth-token-ttl=") {
			ttlCount++
			if arg != ttl {
				return errors.New("StatefulSet JWT TTL argument does not match the approved value")
			}
		}
	}
	if ttlCount != 1 {
		return errors.New("StatefulSet JWT TTL argument does not match the approved value")
	}
	return nil
}

func desiredAuthArgs(o Options) (string, string) {
	oldPath := strings.TrimRight(o.KeyMountDir, "/") + "/" + o.OldKeyField
	newPath := strings.TrimRight(o.KeyMountDir, "/") + "/" + o.NewKeyField
	oldOnly := "--auth-token=jwt,sign-method=" + o.SignMethod + ",priv-key=" + oldPath
	a := oldOnly + ",verify-key=" + newPath
	b := "--auth-token=jwt,sign-method=" + o.SignMethod + ",priv-key=" + newPath + ",verify-key=" + oldPath
	c := "--auth-token=jwt,sign-method=" + o.SignMethod + ",priv-key=" + newPath
	switch o.Phase {
	case "phase-a":
		return a, oldOnly
	case "phase-b":
		return b, a
	default:
		return c, b
	}
}

func rolloutComplete(s statefulSetObject, replicas int64) bool {
	return s.Status.ObservedGeneration == s.Metadata.Generation && s.Status.ReadyReplicas == replicas && s.Status.UpdatedReplicas == replicas && s.Status.CurrentRevision != "" && s.Status.CurrentRevision == s.Status.UpdateRevision
}

func phaseMetadataMatches(s statefulSetObject, o Options, secretDigest string) bool {
	annotations := s.Spec.Template.Metadata.Annotations
	return annotations["dbaas.kubebrain.io/jwt-key-rotation-phase"] == o.Phase &&
		annotations["dbaas.kubebrain.io/jwt-key-rotation-operation"] == o.OperationID &&
		annotations["dbaas.kubebrain.io/jwt-key-secret-sha256"] == secretDigest
}

func templateBaseline(object map[string]any, containerIndex, authArgIndex int) (string, error) {
	encoded, _ := json.Marshal(object)
	var copyObject map[string]any
	_ = json.Unmarshal(encoded, &copyObject)
	template, ok := nestedMap(copyObject, "spec", "template")
	if !ok {
		return "", errors.New("StatefulSet template is missing")
	}
	metadata, ok := nestedMap(template, "metadata")
	if !ok {
		return "", errors.New("StatefulSet template metadata is missing")
	}
	if annotations, ok := metadata["annotations"].(map[string]any); ok {
		delete(annotations, "dbaas.kubebrain.io/jwt-key-rotation-phase")
		delete(annotations, "dbaas.kubebrain.io/jwt-key-rotation-operation")
		delete(annotations, "dbaas.kubebrain.io/jwt-key-secret-sha256")
	}
	spec, ok := nestedMap(template, "spec")
	if !ok {
		return "", errors.New("StatefulSet template spec is missing")
	}
	containers, ok := spec["containers"].([]any)
	if !ok || containerIndex >= len(containers) {
		return "", errors.New("StatefulSet containers are invalid")
	}
	container, ok := containers[containerIndex].(map[string]any)
	if !ok {
		return "", errors.New("StatefulSet container is invalid")
	}
	args, ok := container["args"].([]any)
	if !ok || authArgIndex >= len(args) {
		return "", errors.New("StatefulSet arguments are invalid")
	}
	args[authArgIndex] = "--auth-token=<managed>"
	return digestJSON(template), nil
}

func buildPatch(current stateEvidence, desired, secretDigest string, o Options) ([]byte, error) {
	annotations := current.object.Spec.Template.Metadata.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	} else {
		annotations = mapsClone(annotations)
	}
	annotations["dbaas.kubebrain.io/jwt-key-rotation-phase"] = o.Phase
	annotations["dbaas.kubebrain.io/jwt-key-rotation-operation"] = o.OperationID
	annotations["dbaas.kubebrain.io/jwt-key-secret-sha256"] = secretDigest
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/resourceVersion", "value": current.object.Metadata.ResourceVersion},
		{"op": "test", "path": fmt.Sprintf("/spec/template/spec/containers/%d/args/%d", current.containerIndex, current.authArgIndex), "value": current.authArg},
		{"op": "replace", "path": fmt.Sprintf("/spec/template/spec/containers/%d/args/%d", current.containerIndex, current.authArgIndex), "value": desired},
		{"op": "add", "path": "/spec/template/metadata/annotations", "value": annotations},
	}
	return json.Marshal(patch)
}

func validatePrevious(r Receipt, o Options, secret secretObject, secretDigest string) error {
	expectedPhase := map[string]string{"phase-b": "phase-a", "phase-c": "phase-b"}[o.Phase]
	_, expectedAuthArg := desiredAuthArgs(o)
	if r.Format != ReceiptFormat || r.Phase != expectedPhase || r.OperationID != o.OperationID || r.Instance != o.Instance || r.Namespace != o.Namespace || r.StatefulSet != o.StatefulSet || r.StatefulSetUID == "" || r.Secret != o.Secret || r.SecretUID != secret.Metadata.UID || r.SecretResourceVersion != secret.Metadata.ResourceVersion || r.SecretDataSHA256 != secretDigest || r.OldKeySHA256 != o.OldKeySHA256 || r.NewKeySHA256 != o.NewKeySHA256 || r.Replicas != o.ExpectedReplicas || !r.RolledOut || r.AfterRevision == "" || r.BeforeRevision == "" || !validDigest(r.TemplateBaselineSHA) || r.AuthTokenArgSHA256 != digestString(expectedAuthArg) || r.ObservedAtUnix <= 0 {
		return errors.New("previous publish receipt does not match this rotation")
	}
	if expectedPhase == "phase-a" && r.PreviousReceiptSHA256 != "" {
		return errors.New("phase A publish receipt must start the receipt chain")
	}
	if expectedPhase == "phase-b" && !validDigest(r.PreviousReceiptSHA256) {
		return errors.New("phase B publish receipt does not bind phase A")
	}
	return nil
}

func validateExisting(r Receipt, o Options, secret secretObject, secretDigest string, current stateEvidence, desired, previousDigest string) error {
	if r.Format != ReceiptFormat || r.Phase != o.Phase || r.OperationID != o.OperationID || r.Instance != o.Instance || r.Namespace != o.Namespace || r.StatefulSet != o.StatefulSet || r.StatefulSetUID != current.object.Metadata.UID || r.Secret != o.Secret || r.SecretUID != secret.Metadata.UID || r.SecretResourceVersion != secret.Metadata.ResourceVersion || r.SecretDataSHA256 != secretDigest || r.OldKeySHA256 != o.OldKeySHA256 || r.NewKeySHA256 != o.NewKeySHA256 || r.TemplateBaselineSHA != current.baseline || r.BeforeRevision == "" || r.AfterRevision != current.revision || r.AuthTokenArgSHA256 != digestString(desired) || r.PreviousReceiptSHA256 != previousDigest || r.Replicas != o.ExpectedReplicas || r.ObservedAtUnix <= 0 || !r.RolledOut || current.authArg != desired || !phaseMetadataMatches(current.object, o, secretDigest) || !rolloutComplete(current.object, o.ExpectedReplicas) {
		return errors.New("existing publish receipt does not match current state")
	}
	return nil
}

func previousBaseline(phase string, previous Receipt, current string) string {
	if phase == "phase-a" {
		return current
	}
	return previous.TemplateBaselineSHA
}

func readReceipt(path string) (Receipt, string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return Receipt{}, "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > 1<<20 {
		return Receipt{}, "", errors.New("receipt must be a 0600 regular non-symlink file containing 1..1 MiB")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Receipt{}, "", err
	}
	if len(data) == 0 || len(data) > 1<<20 {
		return Receipt{}, "", errors.New("receipt must contain 1..1 MiB")
	}
	var receipt Receipt
	if err := decodeStrict(data, &receipt); err != nil {
		return Receipt{}, "", err
	}
	return receipt, digestBytes(data), nil
}

func publishReceipt(path string, receipt Receipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".jwt-publish-receipt-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
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
	if err := os.Link(tmpPath, path); err != nil {
		return fmt.Errorf("refusing to overwrite publish receipt: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func decodeStrict(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func nestedMap(value map[string]any, keys ...string) (map[string]any, bool) {
	current := value
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func digestJSON(value any) string { encoded, _ := json.Marshal(value); return digestBytes(encoded) }
func digestJSONLine(value any) string {
	encoded, _ := json.Marshal(value)
	return digestBytes(append(encoded, '\n'))
}
func digestString(value string) string { return digestBytes([]byte(value)) }
func digestBytes(value []byte) string  { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:]) }
func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
func mapsClone(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}
