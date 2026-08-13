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

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
)

type options struct {
	failedAudit, failedParameters, oldPlan, newPlan, oldTarget, newTarget, newAdmission, output string
}

func main() {
	var o options
	flag.StringVar(&o.failedAudit, "failed-operation-audit", "", "canonical failed restore operation audit artifact")
	flag.StringVar(&o.failedParameters, "failed-operation-parameters", "", "exact immutable parameters of the failed restore operation")
	flag.StringVar(&o.oldPlan, "old-plan", "", "approved plan used by the failed restore")
	flag.StringVar(&o.newPlan, "new-plan", "", "approved plan for the replacement target")
	flag.StringVar(&o.oldTarget, "old-target-snapshot-empty", "", "old target-empty receipt")
	flag.StringVar(&o.newTarget, "new-target-snapshot-empty", "", "replacement target-empty receipt")
	flag.StringVar(&o.newAdmission, "new-restore-admission", "", "replacement target admission receipt")
	flag.StringVar(&o.output, "output", "", "new handoff receipt path")
	flag.Parse()
	if err := run(o, time.Now().Unix()); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target replacement handoff:", err)
		os.Exit(1)
	}
}

func run(o options, now int64) error {
	for _, value := range []string{o.failedAudit, o.failedParameters, o.oldPlan, o.newPlan, o.oldTarget, o.newTarget, o.newAdmission, o.output} {
		if value == "" {
			return errors.New("all target replacement evidence paths and output are required")
		}
	}
	auditStatus, auditBytes, err := operationaudit.InspectBytes(o.failedAudit)
	if err != nil {
		return err
	}
	audit := auditStatus.Artifact
	if audit.Namespace != "kubebrain-operations" || audit.Name != audit.OperationID || audit.Instance != "kubebrain" || audit.Type != "NativePITRFullRestore" || audit.Phase != "Failed" || audit.RequestedBy != "platform:native-pitr-full-restore" || audit.Attempt != 2 || audit.MaxAttempts != 2 || audit.ReceiptSHA256 != "" {
		return errors.New("operation audit is not an exhausted receipt-less native PITR full restore")
	}
	parameters, err := readBounded(o.failedParameters)
	if err != nil {
		return err
	}
	if digest(parameters) != audit.ParametersSHA256 {
		return errors.New("failed operation parameters do not match its audit artifact")
	}
	approvedOldPlan, err := approvedPlanDigest(parameters)
	if err != nil {
		return err
	}
	oldPlanBytes, oldPlan, err := decodePlan(o.oldPlan)
	if err != nil {
		return err
	}
	if digest(oldPlanBytes) != approvedOldPlan {
		return errors.New("failed operation parameters do not approve the supplied old plan")
	}
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
	admissionBytes, err := readBounded(o.newAdmission)
	if err != nil {
		return err
	}
	admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	if err != nil {
		return err
	}
	handoff, err := nativepitr.BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, admission, nativepitr.TargetReplacementInputs{
		FailedOperation: nativepitr.FailedRestoreOperationEvidence{AuditArtifactSHA256: digest(auditBytes), OperationName: audit.Name, ParametersSHA256: audit.ParametersSHA256, Attempt: audit.Attempt, MaxAttempts: audit.MaxAttempts},
		OldPlanSHA256:   digest(oldPlanBytes), NewPlanSHA256: digest(newPlanBytes), OldTargetReceiptSHA256: digest(oldTargetBytes), NewTargetReceiptSHA256: digest(newTargetBytes),
		NewRestoreAdmissionSHA256: digest(admissionBytes), SourceExclusiveSHA256: newPlan.Source.RangeExclusiveReceiptSHA256, FullArtifactSHA256: newPlan.Full.ArtifactReceiptSHA256, CreatedAtUnix: now,
	})
	if err != nil {
		return err
	}
	return writeExclusive(o.output, handoff)
}

func approvedPlanDigest(data []byte) (string, error) {
	var value map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&value); err != nil {
		return "", fmt.Errorf("decode failed restore parameters: %w", err)
	}
	if err := ensureEOF(dec); err != nil {
		return "", err
	}
	baseKeys := []string{"admission", "approve_plan_sha256", "artifact_root", "full_artifacts", "full_snapshot", "pd_addrs", "plan", "remote_inventory", "source_range_exclusive", "target_snapshot_empty"}
	if len(value) != len(baseKeys) && len(value) != len(baseKeys)+2 {
		return "", errors.New("failed restore parameters have an invalid schema")
	}
	for _, key := range baseKeys {
		if _, ok := value[key]; !ok {
			return "", errors.New("failed restore parameters have an invalid schema")
		}
	}
	if len(value) == len(baseKeys)+2 {
		if _, method := value["cipher_method"]; !method {
			return "", errors.New("failed restore parameters have an invalid encryption schema")
		}
		if _, keyID := value["encryption_key_id"]; !keyID {
			return "", errors.New("failed restore parameters have an invalid encryption schema")
		}
	}
	var approved string
	if err := json.Unmarshal(value["approve_plan_sha256"], &approved); err != nil || len(approved) != 64 {
		return "", errors.New("failed restore parameters contain no approved plan digest")
	}
	return approved, nil
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

func readBounded(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > 8<<20 {
		return nil, errors.New("target replacement evidence is empty or exceeds 8 MiB")
	}
	return data, nil
}

func writeExclusive(path string, value nativepitr.TargetReplacementHandoff) error {
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
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Link(tempPath, cleanPath); err != nil {
		return err
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = dirFile.Sync()
	if closeErr := dirFile.Close(); err == nil {
		err = closeErr
	}
	return err
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ensureEOF(dec *json.Decoder) error {
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("failed restore parameters contain trailing JSON")
		}
		return err
	}
	return nil
}
