// Command native-pitr-target-replacement-handoff creates an immutable lineage
// receipt before a failed native restore is retried on a newly provisioned
// empty PD/TiKV cluster.
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
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/cmd/internal/failedrestore"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

type options struct {
	failedAudit, failedParameters, oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writerExclusion, oldRetirement, oldAdmission, newAdmission, output string
}

func main() {
	var o options
	flag.StringVar(&o.failedAudit, "failed-operation-audit", "", "canonical failed restore operation audit artifact")
	flag.StringVar(&o.failedParameters, "failed-operation-parameters", "", "exact immutable parameters of the failed restore operation")
	flag.StringVar(&o.oldPlan, "old-plan", "", "approved plan used by the failed restore")
	flag.StringVar(&o.newPlan, "new-plan", "", "approved plan for the replacement target")
	flag.StringVar(&o.oldTarget, "old-target-snapshot-empty", "", "old target-empty receipt")
	flag.StringVar(&o.newTarget, "new-target-snapshot-empty", "", "replacement target-empty receipt")
	flag.StringVar(&o.oldProvisioning, "old-target-provisioning", "", "old target physical provisioning receipt")
	flag.StringVar(&o.newProvisioning, "new-target-provisioning", "", "replacement target physical provisioning receipt")
	flag.StringVar(&o.qualification, "new-target-qualification", "", "replacement target qualification receipt")
	flag.StringVar(&o.writerExclusion, "target-writer-exclusion", "", "writer exclusion evidence bound by target qualification")
	flag.StringVar(&o.oldRetirement, "old-target-retirement", "", "old target released/detached/unreachable receipt")
	flag.StringVar(&o.oldAdmission, "old-restore-admission", "", "restore admission held by the failed operation")
	flag.StringVar(&o.newAdmission, "new-restore-admission", "", "replacement target admission receipt")
	flag.StringVar(&o.output, "output", "", "new handoff receipt path")
	flag.Parse()
	if err := run(o, time.Now().Unix()); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target replacement handoff:", err)
		os.Exit(1)
	}
}

func run(o options, now int64) error {
	for _, value := range []string{o.failedAudit, o.failedParameters, o.oldPlan, o.newPlan, o.oldTarget, o.newTarget, o.oldProvisioning, o.newProvisioning, o.qualification, o.writerExclusion, o.oldRetirement, o.oldAdmission, o.newAdmission, o.output} {
		if value == "" {
			return errors.New("all target replacement evidence paths and output are required")
		}
	}
	failed, err := failedrestore.Load(o.failedAudit, o.failedParameters, o.oldPlan)
	if err != nil {
		return err
	}
	audit, auditBytes, oldPlanBytes, oldPlan := failed.Audit, failed.AuditBytes, failed.PlanBytes, failed.Plan
	newPlanBytes, newPlan, err := decodePlan(o.newPlan)
	if err != nil {
		return err
	}
	oldTargetBytes, oldTarget, err := decodeTarget(o.oldTarget)
	if err != nil {
		return err
	}
	newTargetBytes, newTarget, err := decodeTarget(o.newTarget)
	if err != nil {
		return err
	}
	oldProvisioningBytes, oldProvisioning, err := decodeProvisioning(o.oldProvisioning)
	if err != nil {
		return err
	}
	newProvisioningBytes, newProvisioning, err := decodeProvisioning(o.newProvisioning)
	if err != nil {
		return err
	}
	qualificationBytes, err := readBounded(o.qualification)
	if err != nil {
		return err
	}
	qualification, err := nativepitr.DecodeTargetQualificationReceipt(bytes.NewReader(qualificationBytes))
	if err != nil {
		return err
	}
	writerBytes, err := readBounded(o.writerExclusion)
	if err != nil {
		return err
	}
	writers, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(writerBytes))
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
	admissionBytes, err := readBounded(o.newAdmission)
	if err != nil {
		return err
	}
	admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	if err != nil {
		return err
	}
	handoff, err := nativepitr.BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, oldRetirement, oldAdmission, admission, nativepitr.TargetReplacementInputs{
		FailedOperation: nativepitr.FailedRestoreOperationEvidence{AuditArtifactSHA256: digest(auditBytes), OperationName: audit.Name, ParametersSHA256: audit.ParametersSHA256, Attempt: audit.Attempt, MaxAttempts: audit.MaxAttempts},
		OldPlanSHA256:   digest(oldPlanBytes), NewPlanSHA256: digest(newPlanBytes), OldTargetReceiptSHA256: digest(oldTargetBytes), NewTargetReceiptSHA256: digest(newTargetBytes),
		OldTargetProvisioningSHA256: digest(oldProvisioningBytes), NewTargetProvisioningSHA256: digest(newProvisioningBytes),
		NewTargetQualificationSHA256: digest(qualificationBytes), WriterExclusionSHA256: digest(writerBytes),
		OldTargetRetirementSHA256: digest(oldRetirementBytes),
		OldRestoreAdmissionSHA256: digest(oldAdmissionBytes),
		NewRestoreAdmissionSHA256: digest(admissionBytes), SourceExclusiveSHA256: newPlan.Source.RangeExclusiveReceiptSHA256, FullArtifactSHA256: newPlan.Full.ArtifactReceiptSHA256, CreatedAtUnix: now,
	})
	if err != nil {
		return err
	}
	return writeExclusive(o.output, handoff)
}

func approvedPlanDigest(data []byte) (string, error) {
	return failedrestore.ApprovedPlanDigest(data)
}

func decodePlan(path string) ([]byte, nativepitr.Plan, error) {
	data, err := readBounded(path)
	if err != nil {
		return nil, nativepitr.Plan{}, err
	}
	value, err := nativepitr.DecodePlan(bytes.NewReader(data))
	return data, value, err
}

func decodeTarget(path string) ([]byte, nativepitr.TargetSnapshotEmptyReceipt, error) {
	data, err := readBounded(path)
	if err != nil {
		return nil, nativepitr.TargetSnapshotEmptyReceipt{}, err
	}
	value, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(data))
	return data, value, err
}

func decodeProvisioning(path string) ([]byte, nativepitr.TargetProvisioningReceipt, error) {
	data, err := readBounded(path)
	if err != nil {
		return nil, nativepitr.TargetProvisioningReceipt{}, err
	}
	value, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(data))
	return data, value, err
}

func readBounded(path string) (data []byte, retErr error) {
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
		return nil, errors.New("target replacement evidence is empty or exceeds 8 MiB")
	}
	return data, nil
}

func writeExclusive(path string, value nativepitr.TargetReplacementHandoff) (retErr error) {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	cleanPath := filepath.Clean(path)
	dir := filepath.Dir(cleanPath)
	file, err := os.CreateTemp(dir, "."+filepath.Base(cleanPath)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer func() {
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, removeErr)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return errors.Join(err, file.Close())
	}
	_, writeErr := file.Write(data)
	var syncErr error
	if writeErr == nil {
		syncErr = file.Sync()
	}
	if err := errors.Join(writeErr, syncErr, file.Close()); err != nil {
		return err
	}
	if err := os.Link(tempPath, cleanPath); err != nil {
		return err
	}
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("remove published replacement handoff temporary link: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr = dirFile.Sync()
	closeErr := dirFile.Close()
	return errors.Join(syncErr, closeErr)
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
