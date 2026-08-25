package production_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderJWTKeyRotationDataPlaneRBACIsOperationAndInstanceScoped(t *testing.T) {
	operation := "jwt-key-rotate-0123456789abcdefabcd"
	output, err := runProductionScriptCommand(t, "render-jwt-key-rotation-data-plane-rbac.sh", []string{
		"OPERATION_ID=" + operation,
		"KUBEBRAIN_NAMESPACE=instance-a",
		"KUBEBRAIN_STATEFULSET=kubebrain",
	})
	require.NoError(t, err, string(output))
	text := string(output)
	require.Equal(t, 1, strings.Count(text, "kind: Role\nmetadata:"))
	require.Equal(t, 1, strings.Count(text, "kind: RoleBinding\nmetadata:"))
	require.Equal(t, 2, strings.Count(text, "namespace: instance-a"))
	require.Contains(t, text, "resourceNames: [kubebrain]")
	require.Contains(t, text, "resourceNames: ["+operation+"-keys]")
	require.Contains(t, text, "verbs: [get, patch]")
	require.Contains(t, text, "verbs: [create]")
	require.Contains(t, text, "name: kubebrain-jwt-key-rotation-executor")
	require.Contains(t, text, "namespace: kubebrain-operations")
	require.NotContains(t, text, "verbs: [delete")
	require.NotContains(t, text, "verbs: [list")
	require.NotContains(t, text, "verbs: [watch")
}

func TestRenderJWTKeyRotationDataPlaneRBACRejectsUnboundSecret(t *testing.T) {
	output, err := runProductionScriptCommand(t, "render-jwt-key-rotation-data-plane-rbac.sh", []string{
		"OPERATION_ID=jwt-key-rotate-0123456789abcdefabcd",
		"KUBEBRAIN_NAMESPACE=instance-a",
		"KEY_SECRET=another-secret",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "operation-bound key Secret")
}
