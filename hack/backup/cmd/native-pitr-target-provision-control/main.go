package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

type options struct {
	mode, retirement, oldProvisioning, newProvisioning, manifest                                      string
	authorization, dryRunObject, dryRunReceipt, createdObject, currentObject, output, authorizationID string
}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "", "authorize, verify-authorization, record-dry-run, record-creation, verify-current, or verify-completion")
	flag.StringVar(&o.retirement, "retirement", "", "old target retirement receipt")
	flag.StringVar(&o.oldProvisioning, "old-target-provisioning", "", "old target provisioning receipt")
	flag.StringVar(&o.newProvisioning, "new-target-provisioning", "", "new target provisioning receipt")
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
	default:
		return errors.New("invalid mode")
	}
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

func read(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > 8<<20 {
		return nil, errors.New("target provision evidence is empty or exceeds 8 MiB")
	}
	return data, nil
}
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func writeExclusive(path string, data []byte) error {
	clean := filepath.Clean(path)
	f, err := os.OpenFile(clean, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(clean))
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}
