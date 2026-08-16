package failedrestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
)

type Evidence struct {
	Audit               operationaudit.Artifact
	AuditBytes          []byte
	ParametersBytes     []byte
	Plan                nativepitr.Plan
	PlanBytes           []byte
	AuditSHA256         string
	ParametersSHA256    string
	PlanSHA256          string
	TargetPath          string
	AdmissionPath       string
	TargetProvisionPath string
}

func Load(auditPath, parametersPath, planPath string) (Evidence, error) {
	status, auditBytes, err := operationaudit.InspectBytes(auditPath)
	if err != nil {
		return Evidence{}, err
	}
	audit := status.Artifact
	if audit.Namespace != "kubebrain-operations" || audit.Name != audit.OperationID || audit.Instance != "kubebrain" || audit.Type != "NativePITRFullRestore" || audit.Phase != "Failed" || audit.RequestedBy != "platform:native-pitr-full-restore" || audit.Attempt != 2 || audit.MaxAttempts != 2 || audit.ReceiptSHA256 != "" {
		return Evidence{}, errors.New("operation audit is not an exhausted receipt-less native PITR full restore")
	}
	parameters, err := readBounded(parametersPath)
	if err != nil {
		return Evidence{}, err
	}
	if Digest(parameters) != audit.ParametersSHA256 {
		return Evidence{}, errors.New("failed operation parameters do not match its audit artifact")
	}
	approved, targetPath, admissionPath, provisioningPath, err := decodeParameters(parameters)
	if err != nil {
		return Evidence{}, err
	}
	planBytes, err := readBounded(planPath)
	if err != nil {
		return Evidence{}, err
	}
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	if err != nil {
		return Evidence{}, err
	}
	if Digest(planBytes) != approved {
		return Evidence{}, errors.New("failed operation parameters do not approve the supplied old plan")
	}
	return Evidence{Audit: audit, AuditBytes: auditBytes, ParametersBytes: parameters, Plan: plan, PlanBytes: planBytes,
		AuditSHA256: Digest(auditBytes), ParametersSHA256: Digest(parameters), PlanSHA256: Digest(planBytes), TargetPath: targetPath, AdmissionPath: admissionPath, TargetProvisionPath: provisioningPath}, nil
}

func decodeParameters(data []byte) (string, string, string, string, error) {
	var value map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&value); err != nil {
		return "", "", "", "", fmt.Errorf("decode failed restore parameters: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", "", "", "", errors.New("failed restore parameters contain trailing JSON")
	}
	base := []string{"admission", "approve_plan_sha256", "artifact_root", "full_artifacts", "full_snapshot", "pd_addrs", "plan", "remote_inventory", "source_range_exclusive", "target_provisioning", "target_provisioning_sha256", "target_snapshot_empty"}
	allowed := map[string]bool{}
	for _, k := range append(base, "cipher_method", "encryption_key_id", "target_replacement_handoff", "target_replacement_handoff_sha256", "target_provisioning", "target_provisioning_sha256", "old_target_snapshot_empty", "old_target_snapshot_empty_sha256", "old_target_provisioning", "old_target_provisioning_sha256", "old_target_retirement", "old_target_retirement_sha256", "old_restore_admission", "old_restore_admission_sha256") {
		allowed[k] = true
	}
	for _, k := range base {
		if _, ok := value[k]; !ok {
			return "", "", "", "", errors.New("failed restore parameters have an invalid schema")
		}
	}
	for k := range value {
		if !allowed[k] {
			return "", "", "", "", errors.New("failed restore parameters have an invalid schema")
		}
	}
	_, cipher := value["cipher_method"]
	_, keyID := value["encryption_key_id"]
	if cipher != keyID {
		return "", "", "", "", errors.New("failed restore parameters have an invalid encryption schema")
	}
	replacement := []string{"target_replacement_handoff", "target_replacement_handoff_sha256"}
	if err := completeGroup(value, replacement, "replacement"); err != nil {
		return "", "", "", "", err
	}
	retirement := []string{"old_target_snapshot_empty", "old_target_snapshot_empty_sha256", "old_target_provisioning", "old_target_provisioning_sha256", "old_target_retirement", "old_target_retirement_sha256", "old_restore_admission", "old_restore_admission_sha256"}
	if err := completeGroup(value, retirement, "retirement"); err != nil {
		return "", "", "", "", err
	}
	if _, ok := value[retirement[0]]; ok {
		if _, ok := value[replacement[0]]; !ok {
			return "", "", "", "", errors.New("failed restore parameters have an invalid retirement schema")
		}
	}
	var approved, target, admission, provisioning string
	if json.Unmarshal(value["approve_plan_sha256"], &approved) != nil || len(approved) != 64 {
		return "", "", "", "", errors.New("failed restore parameters contain no approved plan digest")
	}
	if json.Unmarshal(value["target_snapshot_empty"], &target) != nil || json.Unmarshal(value["admission"], &admission) != nil {
		return "", "", "", "", errors.New("failed restore parameters contain invalid evidence paths")
	}
	if raw, ok := value["target_provisioning"]; ok {
		if json.Unmarshal(raw, &provisioning) != nil {
			return "", "", "", "", errors.New("failed restore parameters contain invalid provisioning path")
		}
	}
	return approved, target, admission, provisioning, nil
}

func ApprovedPlanDigest(data []byte) (string, error) {
	approved, _, _, _, err := decodeParameters(data)
	return approved, err
}

func completeGroup(value map[string]json.RawMessage, keys []string, name string) error {
	count := 0
	for _, k := range keys {
		if _, ok := value[k]; ok {
			count++
		}
	}
	if count != 0 && count != len(keys) {
		return fmt.Errorf("failed restore parameters have an invalid %s schema", name)
	}
	return nil
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
		return nil, errors.New("failed restore evidence is empty or exceeds 8 MiB")
	}
	return data, nil
}
func Digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
