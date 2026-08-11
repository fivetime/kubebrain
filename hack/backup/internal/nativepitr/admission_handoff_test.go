package nativepitr

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdmissionHandoffBindsContinuousFenceChain(t *testing.T) {
	plan := validReceiptPlan(t)
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, "restore-1", 9, false)
	require.NoError(t, err)
	full := validFullRestoreExecution()
	full.CompletedAtUnix = 11
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", 12, false)
	require.NoError(t, err)
	receipt, err := BuildAdmissionHandoff(plan, digest, admission, digest, full, digest, fence, digest, 13)
	require.NoError(t, err)
	require.True(t, receipt.ContinuousWriterExclusion)

	fence, _, err = BuildRestorationFenceReceipt(plan, digest, "other", 12, false)
	require.NoError(t, err)
	_, err = BuildAdmissionHandoff(plan, digest, admission, digest, full, digest, fence, digest, 13)
	require.ErrorContains(t, err, "continuous")
}

func TestAdmissionHandoffRejectsAdmissionAfterFullRestoreStarted(t *testing.T) {
	plan := validReceiptPlan(t)
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, "restore-1", 11, false)
	require.NoError(t, err)
	full := validFullRestoreExecution()
	full.StartedAtUnix = 10
	full.CompletedAtUnix = 12
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", 13, false)
	require.NoError(t, err)

	_, err = BuildAdmissionHandoff(plan, digest, admission, digest, full, digest, fence, digest, 14)
	require.ErrorContains(t, err, "continuous")
}

func TestDecodeAdmissionHandoffIsStrict(t *testing.T) {
	plan := validReceiptPlan(t)
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, "restore-1", 9, false)
	require.NoError(t, err)
	full := validFullRestoreExecution()
	full.CompletedAtUnix = 11
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", 12, false)
	require.NoError(t, err)
	receipt, err := BuildAdmissionHandoff(plan, digest, admission, digest, full, digest, fence, digest, 13)
	require.NoError(t, err)
	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)

	decoded, err := DecodeAdmissionHandoff(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, receipt, decoded)
	_, err = DecodeAdmissionHandoff(strings.NewReader(string(encoded) + `{}`))
	require.ErrorContains(t, err, "trailing JSON")
	_, err = DecodeAdmissionHandoff(strings.NewReader(strings.TrimSuffix(string(encoded), "}") + `,"unknown":true}`))
	require.ErrorContains(t, err, "unknown field")
}
