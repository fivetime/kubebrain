package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJWTKeyRotationTakeoverDrillRequiresExplicitConfirmation(t *testing.T) {
	output, err := runProductionCommand(t, "bash", []string{"drill-jwt-key-rotation-takeover.sh"}, nil)
	require.Error(t, err)
	require.Contains(t, string(output), "CONFIRM_JWT_ROTATION_TAKEOVER_DRILL=yes")
}

func TestJWTKeyRotationTakeoverDrillRejectsUnknownScenarioBeforeClusterAccess(t *testing.T) {
	output, err := runProductionCommand(t, "bash", []string{"drill-jwt-key-rotation-takeover.sh"}, []string{
		"CONFIRM_JWT_ROTATION_TAKEOVER_DRILL=yes",
		"SCENARIO=unknown",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "SCENARIO must be phase-b-one-pod, ttl-wait-takeover, or phase-c-terminal-fencing")
}

func TestJWTKeyRotationTakeoverDrillPinsFaultAndEvidenceInvariants(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(".", "drill-jwt-key-rotation-takeover.sh"))
	require.NoError(t, err)
	text := string(data)
	for _, required := range []string{
		`--uid "$executor_uid" --resource-version "$executor_rv"`,
		`updatedReplicas==1`,
		`JWT rotation fault drill hold reached: point=$hold_point`,
		`one exact six-stage receipt chain`,
		`.old_token_rejected==true and .new_token_accepted==true`,
		`.status.phase=="Succeeded" and .status.attempt>=2`,
		`.restartCount==0`,
		`restore_deployment`,
	} {
		require.Contains(t, text, required)
	}
}
