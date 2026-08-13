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
	receipt, plan, artifacts, sourceExclusive, target, admission string
	approve, encryption, keyID                                   string
}

func main() {
	var o options
	flag.StringVar(&o.receipt, "receipt", "", "durable full restore execution receipt")
	flag.StringVar(&o.plan, "plan", "", "exact native PITR restore plan")
	flag.StringVar(&o.artifacts, "full-artifacts", "", "exact full artifact receipt")
	flag.StringVar(&o.sourceExclusive, "source-range-exclusive", "", "exact source range-exclusive receipt")
	flag.StringVar(&o.target, "target-snapshot-empty", "", "exact pre-write target-empty receipt")
	flag.StringVar(&o.admission, "restore-admission", "", "exact restore admission receipt")
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
	return nativepitr.VerifyFullRestoreOperationBinding(receipt, plan, artifacts, source, target, admission, nativepitr.FullRestoreOperationBinding{
		PlanSHA256: planSHA, SourceExclusiveSHA256: digest(sourceBytes), FullArtifactSHA256: digest(artifactsBytes),
		TargetSnapshotSHA256: digest(targetBytes), RestoreAdmissionSHA256: digest(admissionBytes), Encryption: o.encryption, EncryptionKeyID: o.keyID,
	})
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
