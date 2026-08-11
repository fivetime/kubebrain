package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/pkg/backend/admissionfence"
)

const RestoreAdmissionReceiptFormat = "kubebrain.native-pitr-restore-admission.v1"

type RestoreAdmissionReceipt struct {
	Format          string `json:"format"`
	OperationID     string `json:"operation_id"`
	PlanSHA256      string `json:"plan_sha256"`
	TargetClusterID uint64 `json:"target_cluster_id"`
	Keyspace        string `json:"keyspace"`
	MetadataPrefix  string `json:"metadata_prefix"`
	TokenSHA256     string `json:"token_sha256"`
	ActiveSessions  int64  `json:"active_sessions"`
	AcquiredAtUnix  int64  `json:"acquired_at_unix"`
	Resumed         bool   `json:"resumed_existing_ownership"`
	GateHeld        bool   `json:"gate_held"`
}

func BuildRestoreAdmissionReceipt(plan Plan, planSHA, operationID string, acquiredAt int64, resumed bool) (RestoreAdmissionReceipt, admissionfence.Token, error) {
	if err := plan.Validate(); err != nil {
		return RestoreAdmissionReceipt{}, admissionfence.Token{}, err
	}
	if !sha256RE.MatchString(planSHA) || !operationIDRE.MatchString(operationID) || acquiredAt <= 0 {
		return RestoreAdmissionReceipt{}, admissionfence.Token{}, errors.New("invalid restore admission receipt inputs")
	}
	token, err := admissionfence.NewToken(operationID, planSHA, plan.Target.ClusterID, plan.Source.Keyspace)
	if err != nil {
		return RestoreAdmissionReceipt{}, admissionfence.Token{}, err
	}
	tokenSHA, err := token.SHA256()
	if err != nil {
		return RestoreAdmissionReceipt{}, admissionfence.Token{}, err
	}
	r := RestoreAdmissionReceipt{Format: RestoreAdmissionReceiptFormat, OperationID: operationID, PlanSHA256: planSHA, TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace, MetadataPrefix: admissionfence.ScopePrefix(plan.Source.Keyspace), TokenSHA256: tokenSHA, AcquiredAtUnix: acquiredAt, Resumed: resumed, GateHeld: true}
	return r, token, r.Validate()
}

func (r RestoreAdmissionReceipt) Validate() error {
	if r.Format != RestoreAdmissionReceiptFormat || !operationIDRE.MatchString(r.OperationID) || !sha256RE.MatchString(r.PlanSHA256) || r.TargetClusterID == 0 || r.MetadataPrefix != admissionfence.ScopePrefix(r.Keyspace) || !sha256RE.MatchString(r.TokenSHA256) || r.ActiveSessions != 0 || r.AcquiredAtUnix <= 0 || !r.GateHeld {
		return errors.New("invalid native PITR restore admission receipt")
	}
	token, err := admissionfence.NewToken(r.OperationID, r.PlanSHA256, r.TargetClusterID, r.Keyspace)
	if err != nil {
		return err
	}
	want, err := token.SHA256()
	if err != nil || want != r.TokenSHA256 {
		return errors.New("restore admission receipt token digest mismatch")
	}
	return nil
}

func (r RestoreAdmissionReceipt) Token() (admissionfence.Token, error) {
	if err := r.Validate(); err != nil {
		return admissionfence.Token{}, err
	}
	return admissionfence.NewToken(r.OperationID, r.PlanSHA256, r.TargetClusterID, r.Keyspace)
}

func DecodeRestoreAdmissionReceipt(reader io.Reader) (RestoreAdmissionReceipt, error) {
	var receipt RestoreAdmissionReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode restore admission receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("restore admission receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
