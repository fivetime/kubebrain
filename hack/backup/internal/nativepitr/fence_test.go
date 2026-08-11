package nativepitr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildRestorationFenceReceiptBindsPlanAndToken(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt, token, err := BuildRestorationFenceReceipt(plan, strings.Repeat("a", 64), "restore-1", 123, false)
	require.NoError(t, err)
	require.Equal(t, token.OperationID, receipt.OperationID)
	require.Equal(t, plan.Target.ClusterID, receipt.TargetClusterID)
	require.Equal(t, CoordinationPrefix(plan.Source.Keyspace), receipt.CoordinationPrefix)
	require.NoError(t, receipt.Validate())

	receipt.PlanSHA256 = strings.Repeat("b", 64)
	require.ErrorContains(t, receipt.Validate(), "token digest")
}
