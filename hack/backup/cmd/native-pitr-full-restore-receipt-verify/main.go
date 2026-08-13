// Command native-pitr-full-restore-receipt-verify validates a durable restore
// receipt before an Operation publishes or reconciles terminal success.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

type options struct {
	receipt, plan, artifacts, sourceExclusive, target, admission, targetReplacement, targetProvisioning string
	oldTarget, oldProvisioning, oldRetirement, oldAdmission                                             string
	approve, encryption, keyID                                                                          string
}

func main() {
	var o options
	flag.StringVar(&o.receipt, "receipt", "", "durable full restore execution receipt")
	flag.StringVar(&o.plan, "plan", "", "exact native PITR restore plan")
	flag.StringVar(&o.artifacts, "full-artifacts", "", "exact full artifact receipt")
	flag.StringVar(&o.sourceExclusive, "source-range-exclusive", "", "exact source range-exclusive receipt")
	flag.StringVar(&o.target, "target-snapshot-empty", "", "exact pre-write target-empty receipt")
	flag.StringVar(&o.admission, "restore-admission", "", "exact restore admission receipt")
	flag.StringVar(&o.targetReplacement, "target-replacement-handoff", "", "optional receipt-less failure to replacement-target lineage handoff")
	flag.StringVar(&o.targetProvisioning, "target-provisioning", "", "replacement target physical provisioning receipt")
	flag.StringVar(&o.oldTarget, "old-target-snapshot-empty", "", "old target-empty receipt")
	flag.StringVar(&o.oldProvisioning, "old-target-provisioning", "", "old target physical provisioning receipt")
	flag.StringVar(&o.oldRetirement, "old-target-retirement", "", "old target retirement receipt")
	flag.StringVar(&o.oldAdmission, "old-restore-admission", "", "old restore admission receipt")
	flag.StringVar(&o.approve, "approve-plan-sha256", "", "operation-approved exact plan digest")
	flag.StringVar(&o.encryption, "encryption", nativepitr.CipherMethodPlaintext, "expected artifact cipher method")
	flag.StringVar(&o.keyID, "encryption-key-id", "", "expected immutable encryption key version")
	flag.Parse()
	if err := verify(o); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR full restore receipt verify:", err)
		os.Exit(1)
	}
}

func verify(o options) error {
	for _, value := range []string{o.receipt, o.plan, o.artifacts, o.sourceExclusive, o.target, o.admission, o.approve} {
		if value == "" {
			return errors.New("all receipt binding inputs and approve-plan-sha256 are required")
		}
	}
	receiptBytes, err := readBounded(o.receipt)
	if err != nil {
		return err
	}
	receipt, err := nativepitr.DecodeFullRestoreExecution(bytes.NewReader(receiptBytes))
	if err != nil {
		return err
	}
	planBytes, err := readBounded(o.plan)
	if err != nil {
		return err
	}
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	if err != nil {
		return err
	}
	artifactsBytes, err := readBounded(o.artifacts)
	if err != nil {
		return err
	}
	artifacts, err := nativepitr.DecodeArtifactReceipt(bytes.NewReader(artifactsBytes))
	if err != nil {
		return err
	}
	sourceBytes, err := readBounded(o.sourceExclusive)
	if err != nil {
		return err
	}
	source, err := nativepitr.DecodeSourceRangeExclusive(bytes.NewReader(sourceBytes))
	if err != nil {
		return err
	}
	targetBytes, err := readBounded(o.target)
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	admissionBytes, err := readBounded(o.admission)
	if err != nil {
		return err
	}
	admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	if err != nil {
		return err
	}
	planSHA := digest(planBytes)
	if o.approve != planSHA {
		return errors.New("approve-plan-sha256 does not match the exact plan")
	}
	binding := nativepitr.FullRestoreOperationBinding{
		PlanSHA256: planSHA, SourceExclusiveSHA256: digest(sourceBytes), FullArtifactSHA256: digest(artifactsBytes),
		TargetSnapshotSHA256: digest(targetBytes), RestoreAdmissionSHA256: digest(admissionBytes), Encryption: o.encryption, EncryptionKeyID: o.keyID,
	}
	if err := nativepitr.VerifyFullRestoreOperationBinding(receipt, plan, artifacts, source, target, admission, binding); err != nil {
		return err
	}
	if o.targetReplacement == "" && o.targetProvisioning == "" && o.oldTarget == "" && o.oldProvisioning == "" && o.oldRetirement == "" && o.oldAdmission == "" {
		return nil
	}
	if o.targetReplacement == "" || o.targetProvisioning == "" || o.oldTarget == "" || o.oldProvisioning == "" || o.oldRetirement == "" || o.oldAdmission == "" {
		return errors.New("target replacement handoff, old target/provisioning/retirement/admission, and new provisioning receipts must be supplied together")
	}
	handoffBytes, err := readBounded(o.targetReplacement)
	if err != nil {
		return err
	}
	handoff, err := nativepitr.DecodeTargetReplacementHandoff(bytes.NewReader(handoffBytes))
	if err != nil {
		return err
	}
	provisioningBytes, err := readBounded(o.targetProvisioning)
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningBytes))
	if err != nil {
		return err
	}
	oldTargetBytes, err := readBounded(o.oldTarget)
	if err != nil {
		return err
	}
	oldTarget, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(oldTargetBytes))
	if err != nil {
		return err
	}
	oldProvisioningBytes, err := readBounded(o.oldProvisioning)
	if err != nil {
		return err
	}
	oldProvisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(oldProvisioningBytes))
	if err != nil {
		return err
	}
	oldRetirementBytes, err := readBounded(o.oldRetirement)
	if err != nil {
		return err
	}
	oldRetirement, err := nativepitr.DecodeTargetRetirementReceipt(bytes.NewReader(oldRetirementBytes))
	if err != nil {
		return err
	}
	oldAdmissionBytes, err := readBounded(o.oldAdmission)
	if err != nil {
		return err
	}
	oldAdmission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(oldAdmissionBytes))
	if err != nil {
		return err
	}
	return nativepitr.VerifyTargetReplacementHandoffBinding(handoff, plan, oldTarget, target, oldProvisioning, provisioning, oldRetirement, oldAdmission, digest(oldTargetBytes), digest(oldProvisioningBytes), digest(provisioningBytes), digest(oldRetirementBytes), digest(oldAdmissionBytes), binding)
}

func readBounded(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > 8<<20 {
		return nil, errors.New("receipt binding input is empty or exceeds 8 MiB")
	}
	return data, nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
