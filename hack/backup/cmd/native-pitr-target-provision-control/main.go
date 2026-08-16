package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

type options struct {
	mode, retirement, oldProvisioning, newProvisioning, manifest, targetEmpty                         string
	authorization, dryRunObject, dryRunReceipt, createdObject, currentObject, output, authorizationID string
	targetEmptyOutput, expectedPDAddrs                                                                string
	writerExclusion                                                                                   string
}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "", "authorize, verify-authorization, record-dry-run, record-creation, verify-current, or verify-completion")
	flag.StringVar(&o.retirement, "retirement", "", "old target retirement receipt")
	flag.StringVar(&o.oldProvisioning, "old-target-provisioning", "", "old target provisioning receipt")
	flag.StringVar(&o.newProvisioning, "new-target-provisioning", "", "new target provisioning receipt")
	flag.StringVar(&o.targetEmpty, "target-empty", "", "live target snapshot-empty receipt or candidate")
	flag.StringVar(&o.targetEmptyOutput, "target-empty-output", "", "optional exclusive durable target-empty output")
	flag.StringVar(&o.expectedPDAddrs, "expected-pd-addrs", "", "comma-separated exact replacement PD endpoints")
	flag.StringVar(&o.writerExclusion, "writer-exclusion", "", "live KubeBrain writer exclusion evidence")
	flag.StringVar(&o.manifest, "manifest", "", "exact replacement TidbCluster JSON manifest")
	flag.StringVar(&o.authorization, "authorization", "", "target provision authorization")
	flag.StringVar(&o.createdObject, "created-object", "", "creation receipt or Kubernetes create response")
	flag.StringVar(&o.dryRunObject, "dry-run-object", "", "Kubernetes server-side dry-run response")
	flag.StringVar(&o.dryRunReceipt, "dry-run-receipt", "", "durable server-side dry-run receipt")
	flag.StringVar(&o.currentObject, "current-object", "", "current Kubernetes object")
	flag.StringVar(&o.authorizationID, "authorization-id", "", "operation-scoped authorization ID")
	flag.StringVar(&o.output, "output", "", "exclusive durable output")
	flag.Parse()
	if err := run(o, time.Now().Unix()); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target provision control:", err)
		os.Exit(1)
	}
}

func run(o options, now int64) error {
	switch o.mode {
	case "authorize":
		return authorize(o, now)
	case "verify-authorization":
		return verifyAuthorization(o)
	case "record-creation":
		return recordCreation(o, now)
	case "record-dry-run":
		return recordDryRun(o, now)
	case "verify-dry-run":
		return verifyDryRun(o)
	case "verify-current":
		return verifyCurrent(o)
	case "verify-completion":
		return verifyCompletion(o)
	case "qualify-target":
		return qualifyTarget(o, now)
	case "verify-qualification":
		return verifyQualification(o)
	case "record-writer-exclusion":
		return recordWriterExclusion(o)
	case "verify-writer-exclusion":
		return verifyWriterExclusion(o)
	default:
		return errors.New("invalid mode")
	}
}

func recordWriterExclusion(o options) error {
	if o.writerExclusion == "" || o.output == "" {
		return errors.New("writer exclusion candidate and output are required")
	}
	data, err := read(o.writerExclusion)
	if err != nil {
		return err
	}
	e, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(data))
	if err != nil {
		return err
	}
	_, canonical, err := nativepitr.DigestCanonicalJSON(e)
	if err != nil {
		return err
	}
	return writeExclusive(o.output, canonical)
}

func verifyWriterExclusion(o options) error {
	if o.writerExclusion == "" || o.currentObject == "" {
		return errors.New("recorded and current writer exclusion evidence are required")
	}
	recordedBytes, err := read(o.writerExclusion)
	if err != nil {
		return err
	}
	recorded, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(recordedBytes))
	if err != nil {
		return err
	}
	currentBytes, err := read(o.currentObject)
	if err != nil {
		return err
	}
	current, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(currentBytes))
	if err != nil {
		return err
	}
	return nativepitr.VerifyTargetWriterExclusionContinuity(recorded, current)
}

func qualifyTarget(o options, now int64) error {
	if o.newProvisioning == "" || o.targetEmpty == "" || o.writerExclusion == "" || o.expectedPDAddrs == "" || o.output == "" {
		return errors.New("new provisioning, target-empty, exact PD endpoints, and qualification output are required")
	}
	provisioningBytes, err := read(o.newProvisioning)
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningBytes))
	if err != nil {
		return err
	}
	targetBytes, err := read(o.targetEmpty)
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	writerBytes, err := read(o.writerExclusion)
	if err != nil {
		return err
	}
	writers, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(writerBytes))
	if err != nil {
		return err
	}
	if o.targetEmptyOutput != "" {
		_, canonical, err := nativepitr.DigestCanonicalJSON(target)
		if err != nil {
			return err
		}
		if err := writeExclusive(o.targetEmptyOutput, canonical); err != nil {
			return err
		}
		targetBytes = canonical
	}
	expected, err := parsePDAddrs(o.expectedPDAddrs)
	if err != nil {
		return err
	}
	r, err := nativepitr.BuildTargetQualificationReceipt(provisioning, target, writers, digest(provisioningBytes), digest(targetBytes), digest(writerBytes), expected, now)
	if err != nil {
		return err
	}
	_, data, err := nativepitr.DigestCanonicalJSON(r)
	if err != nil {
		return err
	}
	return writeExclusive(o.output, data)
}

func verifyQualification(o options) error {
	if o.newProvisioning == "" || o.targetEmpty == "" || o.writerExclusion == "" || o.expectedPDAddrs == "" || o.createdObject == "" {
		return errors.New("new provisioning, target-empty, exact PD endpoints, and qualification receipt are required")
	}
	provisioningBytes, err := read(o.newProvisioning)
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningBytes))
	if err != nil {
		return err
	}
	targetBytes, err := read(o.targetEmpty)
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	writerBytes, err := read(o.writerExclusion)
	if err != nil {
		return err
	}
	writers, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(writerBytes))
	if err != nil {
		return err
	}
	qualificationBytes, err := read(o.createdObject)
	if err != nil {
		return err
	}
	qualification, err := nativepitr.DecodeTargetQualificationReceipt(bytes.NewReader(qualificationBytes))
	if err != nil {
		return err
	}
	expected, err := parsePDAddrs(o.expectedPDAddrs)
	if err != nil {
		return err
	}
	return nativepitr.VerifyTargetQualificationBinding(qualification, provisioning, target, writers, digest(provisioningBytes), digest(targetBytes), digest(writerBytes), expected)
}

func parsePDAddrs(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	for _, part := range parts {
		if part == "" || strings.TrimSpace(part) != part {
			return nil, errors.New("invalid expected PD endpoints")
		}
	}
	sort.Strings(parts)
	for i := 1; i < len(parts); i++ {
		if parts[i] == parts[i-1] {
			return nil, errors.New("duplicate expected PD endpoint")
		}
	}
	return parts, nil
}

func verifyDryRun(o options) error {
	if o.authorization == "" || o.dryRunReceipt == "" {
		return errors.New("authorization and server dry-run receipt are required")
	}
	authBytes, err := read(o.authorization)
	if err != nil {
		return err
	}
	a, err := nativepitr.DecodeTargetProvisionAuthorization(bytes.NewReader(authBytes))
	if err != nil {
		return err
	}
	dryRunBytes, err := read(o.dryRunReceipt)
	if err != nil {
		return err
	}
	dryRun, err := nativepitr.DecodeTargetProvisionDryRunReceipt(bytes.NewReader(dryRunBytes))
	if err != nil {
		return err
	}
	return nativepitr.VerifyTargetProvisionDryRunReceipt(a, dryRun, digest(authBytes))
}

func recordDryRun(o options, now int64) error {
	if o.authorization == "" || o.dryRunObject == "" || o.output == "" {
		return errors.New("authorization, server dry-run object, and output are required")
	}
	authBytes, err := read(o.authorization)
	if err != nil {
		return err
	}
	a, err := nativepitr.DecodeTargetProvisionAuthorization(bytes.NewReader(authBytes))
	if err != nil {
		return err
	}
	objectBytes, err := read(o.dryRunObject)
	if err != nil {
		return err
	}
	object, err := nativepitr.DecodeAdmittedTidbCluster(bytes.NewReader(objectBytes))
	if err != nil {
		return err
	}
	r, err := nativepitr.BuildTargetProvisionDryRunReceipt(a, digest(authBytes), object, now)
	if err != nil {
		return err
	}
	_, data, err := nativepitr.DigestCanonicalJSON(r)
	if err != nil {
		return err
	}
	return writeExclusive(o.output, data)
}

func verifyAuthorization(o options) error {
	if o.retirement == "" || o.oldProvisioning == "" || o.manifest == "" || o.authorization == "" {
		return errors.New("authorization and its exact source evidence are required")
	}
	retirementBytes, err := read(o.retirement)
	if err != nil {
		return err
	}
	retirement, err := nativepitr.DecodeTargetRetirementReceipt(bytes.NewReader(retirementBytes))
	if err != nil {
		return err
	}
	oldBytes, err := read(o.oldProvisioning)
	if err != nil {
		return err
	}
	old, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(oldBytes))
	if err != nil {
		return err
	}
	manifestBytes, err := read(o.manifest)
	if err != nil {
		return err
	}
	manifest, err := nativepitr.DecodeReplacementTidbClusterManifest(bytes.NewReader(manifestBytes))
	if err != nil {
		return err
	}
	authBytes, err := read(o.authorization)
	if err != nil {
		return err
	}
	a, err := nativepitr.DecodeTargetProvisionAuthorization(bytes.NewReader(authBytes))
	if err != nil {
		return err
	}
	expected, err := nativepitr.BuildTargetProvisionAuthorization(retirement, old, digest(retirementBytes), digest(oldBytes), digest(manifestBytes), a.AuthorizationID, manifest, a.AuthorizedAtUnix)
	if err != nil {
		return err
	}
	if expected != a {
		return errors.New("target provision authorization does not match its exact source evidence")
	}
	return nil
}

func authorize(o options, now int64) error {
	if o.retirement == "" || o.oldProvisioning == "" || o.manifest == "" || o.authorizationID == "" || o.output == "" {
		return errors.New("authorization evidence, ID, and output are required")
	}
	retirementBytes, err := read(o.retirement)
	if err != nil {
		return err
	}
	retirement, err := nativepitr.DecodeTargetRetirementReceipt(bytes.NewReader(retirementBytes))
	if err != nil {
		return err
	}
	oldBytes, err := read(o.oldProvisioning)
	if err != nil {
		return err
	}
	old, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(oldBytes))
	if err != nil {
		return err
	}
	manifestBytes, err := read(o.manifest)
	if err != nil {
		return err
	}
	manifest, err := nativepitr.DecodeReplacementTidbClusterManifest(bytes.NewReader(manifestBytes))
	if err != nil {
		return err
	}
	a, err := nativepitr.BuildTargetProvisionAuthorization(retirement, old, digest(retirementBytes), digest(oldBytes), digest(manifestBytes), o.authorizationID, manifest, now)
	if err != nil {
		return err
	}
	_, data, err := nativepitr.DigestCanonicalJSON(a)
	if err != nil {
		return err
	}
	return writeExclusive(o.output, data)
}

func recordCreation(o options, now int64) error {
	if o.authorization == "" || o.dryRunReceipt == "" || o.createdObject == "" || o.output == "" {
		return errors.New("authorization, server dry-run receipt, created object, and output are required")
	}
	authBytes, err := read(o.authorization)
	if err != nil {
		return err
	}
	a, err := nativepitr.DecodeTargetProvisionAuthorization(bytes.NewReader(authBytes))
	if err != nil {
		return err
	}
	objectBytes, err := read(o.createdObject)
	if err != nil {
		return err
	}
	object, err := nativepitr.DecodeAdmittedTidbCluster(bytes.NewReader(objectBytes))
	if err != nil {
		return err
	}
	dryRunBytes, err := read(o.dryRunReceipt)
	if err != nil {
		return err
	}
	dryRun, err := nativepitr.DecodeTargetProvisionDryRunReceipt(bytes.NewReader(dryRunBytes))
	if err != nil {
		return err
	}
	if object.Metadata.Annotations["dbaas.kubebrain.io/native-pitr-provision-authorization"] != a.ManifestAuthorizationID {
		return errors.New("created object is not the authorized TidbCluster")
	}
	identitySHA, err := nativepitr.AdmittedTidbClusterIdentitySHA256(object)
	if err != nil {
		return err
	}
	if identitySHA != dryRun.AdmittedIdentitySHA256 {
		return errors.New("created TidbCluster differs from its exact server dry-run admission")
	}
	r := nativepitr.TargetProvisionCreationReceipt{
		Format: nativepitr.TargetProvisionCreationFormat, AuthorizationSHA256: digest(authBytes),
		ManifestSHA256: a.ManifestSHA256, DryRunReceiptSHA256: digest(dryRunBytes), Namespace: object.Metadata.Namespace,
		TidbCluster: object.Metadata.Name, TidbClusterUID: object.Metadata.UID,
		ResourceVersion: object.Metadata.ResourceVersion, AdmittedIdentitySHA256: identitySHA,
		CreatedAtUnix: now, ServerDryRunPassed: true,
	}
	r, err = nativepitr.BuildTargetProvisionCreationReceipt(a, dryRun, digest(authBytes), digest(dryRunBytes), r)
	if err != nil {
		return err
	}
	_, data, err := nativepitr.DigestCanonicalJSON(r)
	if err != nil {
		return err
	}
	return writeExclusive(o.output, data)
}

func verifyCurrent(o options) error {
	if o.authorization == "" || o.createdObject == "" || o.currentObject == "" {
		return errors.New("authorization, creation receipt, and current object are required")
	}
	authBytes, err := read(o.authorization)
	if err != nil {
		return err
	}
	a, err := nativepitr.DecodeTargetProvisionAuthorization(bytes.NewReader(authBytes))
	if err != nil {
		return err
	}
	creationBytes, err := read(o.createdObject)
	if err != nil {
		return err
	}
	creation, err := nativepitr.DecodeTargetProvisionCreationReceipt(bytes.NewReader(creationBytes))
	if err != nil {
		return err
	}
	currentBytes, err := read(o.currentObject)
	if err != nil {
		return err
	}
	current, err := nativepitr.DecodeAdmittedTidbCluster(bytes.NewReader(currentBytes))
	if err != nil {
		return err
	}
	return nativepitr.VerifyCurrentTargetProvisioningObject(a, creation, current, digest(authBytes))
}

func verifyCompletion(o options) error {
	if o.authorization == "" || o.createdObject == "" || o.oldProvisioning == "" || o.newProvisioning == "" {
		return errors.New("authorization, creation, and provisioning receipts are required")
	}
	authBytes, err := read(o.authorization)
	if err != nil {
		return err
	}
	a, err := nativepitr.DecodeTargetProvisionAuthorization(bytes.NewReader(authBytes))
	if err != nil {
		return err
	}
	creationBytes, err := read(o.createdObject)
	if err != nil {
		return err
	}
	creation, err := nativepitr.DecodeTargetProvisionCreationReceipt(bytes.NewReader(creationBytes))
	if err != nil {
		return err
	}
	oldBytes, err := read(o.oldProvisioning)
	if err != nil {
		return err
	}
	old, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(oldBytes))
	if err != nil {
		return err
	}
	newBytes, err := read(o.newProvisioning)
	if err != nil {
		return err
	}
	newReceipt, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(newBytes))
	if err != nil {
		return err
	}
	return nativepitr.VerifyAuthorizedReplacementProvisioning(a, creation, old, newReceipt, digest(authBytes))
}

func read(path string) (data []byte, retErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	data, err = io.ReadAll(io.LimitReader(file, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > 8<<20 {
		return nil, errors.New("target provision evidence is empty or exceeds 8 MiB")
	}
	return data, nil
}
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func writeExclusive(path string, data []byte) (retErr error) {
	clean := filepath.Clean(path)
	dirPath := filepath.Dir(clean)
	temp, err := os.CreateTemp(dirPath, "."+filepath.Base(clean)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, removeErr)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return errors.Join(err, temp.Close())
	}
	_, writeErr := temp.Write(data)
	var syncErr error
	if writeErr == nil {
		syncErr = temp.Sync()
	}
	if err := errors.Join(writeErr, syncErr, temp.Close()); err != nil {
		return err
	}
	if err := os.Link(tempPath, clean); err != nil {
		return err
	}
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("remove published target provision temporary link: %w", err)
	}
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
