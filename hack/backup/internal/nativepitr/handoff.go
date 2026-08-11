package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
)

const RestorationFenceHandoffFormat = "kubebrain.native-pitr-restoration-fence-handoff.v1"

type RestorationFenceHandoffReceipt struct {
	Format                        string `json:"format"`
	PlanSHA256                    string `json:"plan_sha256"`
	RestorationFenceReceiptSHA256 string `json:"restoration_fence_receipt_sha256"`
	LogReplayReceiptSHA256        string `json:"log_replay_receipt_sha256"`
	OperationID                   string `json:"operation_id"`
	TargetClusterID               uint64 `json:"target_cluster_id"`
	Keyspace                      string `json:"keyspace"`
	FenceKeyCount                 int    `json:"fence_key_count"`
	ReplayWriteFenceProven        bool   `json:"replay_write_fence_proven"`
	AllKeysReopened               bool   `json:"all_keys_reopened"`
	ReleasedAtUnix                int64  `json:"released_at_unix"`
}

func BuildRestorationFenceHandoff(plan Plan, planSHA string, fence RestorationFenceReceipt, fenceSHA string, replay LogReplayExecutionReceipt, replaySHA string, releasedAt int64) (RestorationFenceHandoffReceipt, error) {
	if err := plan.Validate(); err != nil {
		return RestorationFenceHandoffReceipt{}, err
	}
	if err := fence.Validate(); err != nil {
		return RestorationFenceHandoffReceipt{}, err
	}
	if err := replay.Validate(); err != nil {
		return RestorationFenceHandoffReceipt{}, err
	}
	if !sha256RE.MatchString(planSHA) || !sha256RE.MatchString(fenceSHA) || !sha256RE.MatchString(replaySHA) ||
		fence.PlanSHA256 != planSHA || fence.TargetClusterID != plan.Target.ClusterID || fence.Keyspace != plan.Source.Keyspace ||
		replay.PlanSHA256 != planSHA || replay.TargetClusterID != plan.Target.ClusterID || replay.Keyspace != plan.Source.Keyspace ||
		replay.RestorationFenceReceiptSHA256 != fenceSHA || releasedAt < replay.CompletedAtUnix {
		return RestorationFenceHandoffReceipt{}, errors.New("restoration fence handoff evidence does not match replay plan")
	}
	receipt := RestorationFenceHandoffReceipt{
		Format: RestorationFenceHandoffFormat, PlanSHA256: planSHA,
		RestorationFenceReceiptSHA256: fenceSHA, LogReplayReceiptSHA256: replaySHA,
		OperationID: fence.OperationID, TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace,
		FenceKeyCount: fence.FenceKeyCount, ReplayWriteFenceProven: true, AllKeysReopened: true,
		ReleasedAtUnix: releasedAt,
	}
	return receipt, receipt.Validate()
}

func (r RestorationFenceHandoffReceipt) Validate() error {
	if r.Format != RestorationFenceHandoffFormat || !sha256RE.MatchString(r.PlanSHA256) ||
		!sha256RE.MatchString(r.RestorationFenceReceiptSHA256) || !sha256RE.MatchString(r.LogReplayReceiptSHA256) ||
		!operationIDRE.MatchString(r.OperationID) || r.TargetClusterID == 0 || r.Keyspace == "" ||
		r.FenceKeyCount != restorationfence.ShardCount+1 || !r.ReplayWriteFenceProven || !r.AllKeysReopened || r.ReleasedAtUnix <= 0 {
		return errors.New("invalid native PITR restoration fence handoff receipt")
	}
	return nil
}

func DecodeRestorationFenceHandoff(reader io.Reader) (RestorationFenceHandoffReceipt, error) {
	var receipt RestorationFenceHandoffReceipt
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode restoration fence handoff receipt: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return receipt, errors.New("restoration fence handoff receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
