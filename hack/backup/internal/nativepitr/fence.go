package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
)

const RestorationFenceReceiptFormat = "kubebrain.native-pitr-restoration-fence.v1"

var operationIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type RestorationFenceReceipt struct {
	Format             string `json:"format"`
	OperationID        string `json:"operation_id"`
	PlanSHA256         string `json:"plan_sha256"`
	TargetClusterID    uint64 `json:"target_cluster_id"`
	Keyspace           string `json:"keyspace"`
	CoordinationPrefix string `json:"coordination_prefix"`
	TokenSHA256        string `json:"token_sha256"`
	FenceKeyCount      int    `json:"fence_key_count"`
	VerifiedAtUnix     int64  `json:"verified_at_unix"`
	Resumed            bool   `json:"resumed_existing_ownership"`
	AllKeysHeld        bool   `json:"all_keys_held"`
}

func CoordinationPrefix(keyspace string) string {
	return "/kubebrain-internal/ks-" + keyspace
}

func BuildRestorationFenceReceipt(plan Plan, planSHA, operationID string, verifiedAt int64, resumed bool) (RestorationFenceReceipt, restorationfence.Token, error) {
	if err := plan.Validate(); err != nil {
		return RestorationFenceReceipt{}, restorationfence.Token{}, err
	}
	if !sha256RE.MatchString(planSHA) || !operationIDRE.MatchString(operationID) || verifiedAt <= 0 {
		return RestorationFenceReceipt{}, restorationfence.Token{}, errors.New("invalid restoration fence receipt inputs")
	}
	token, err := restorationfence.NewToken(operationID, planSHA, plan.Target.ClusterID, plan.Source.Keyspace)
	if err != nil {
		return RestorationFenceReceipt{}, restorationfence.Token{}, err
	}
	tokenSHA, err := token.SHA256()
	if err != nil {
		return RestorationFenceReceipt{}, restorationfence.Token{}, err
	}
	receipt := RestorationFenceReceipt{Format: RestorationFenceReceiptFormat, OperationID: operationID, PlanSHA256: planSHA, TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace, CoordinationPrefix: CoordinationPrefix(plan.Source.Keyspace), TokenSHA256: tokenSHA, FenceKeyCount: restorationfence.ShardCount + 1, VerifiedAtUnix: verifiedAt, Resumed: resumed, AllKeysHeld: true}
	return receipt, token, receipt.Validate()
}

func (r RestorationFenceReceipt) Validate() error {
	if r.Format != RestorationFenceReceiptFormat || !operationIDRE.MatchString(r.OperationID) || !sha256RE.MatchString(r.PlanSHA256) || r.TargetClusterID == 0 || r.Keyspace == "" || r.CoordinationPrefix != CoordinationPrefix(r.Keyspace) || !sha256RE.MatchString(r.TokenSHA256) || r.FenceKeyCount != restorationfence.ShardCount+1 || r.VerifiedAtUnix <= 0 || !r.AllKeysHeld {
		return errors.New("invalid native PITR restoration fence receipt")
	}
	token, err := restorationfence.NewToken(r.OperationID, r.PlanSHA256, r.TargetClusterID, r.Keyspace)
	if err != nil {
		return err
	}
	want, err := token.SHA256()
	if err != nil || want != r.TokenSHA256 {
		return errors.New("restoration fence receipt token digest mismatch")
	}
	return nil
}

func DecodeRestorationFenceReceipt(reader io.Reader) (RestorationFenceReceipt, error) {
	var receipt RestorationFenceReceipt
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode restoration fence receipt: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return receipt, errors.New("restoration fence receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}

func (r RestorationFenceReceipt) Token() (restorationfence.Token, error) {
	if err := r.Validate(); err != nil {
		return restorationfence.Token{}, err
	}
	return restorationfence.NewToken(r.OperationID, r.PlanSHA256, r.TargetClusterID, r.Keyspace)
}
