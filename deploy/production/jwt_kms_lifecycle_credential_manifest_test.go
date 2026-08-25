package production

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestJWTKMSLifecycleCredentialManifestIsIsolatedAndFailClosed(t *testing.T) {
	data, err := os.ReadFile("kubebrain-jwt-kms-lifecycle-credential.yaml")
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "name: kubebrain-kms-lifecycle")
	require.Contains(t, text, "automountServiceAccountToken: false")
	require.Contains(t, text, `has(object.metadata.labels)`)
	require.Contains(t, text, `has(oldObject.metadata.labels)`)
	require.Contains(t, text, `"dbaas.kubebrain.io/credential-kind" in object.metadata.labels`)
	require.Contains(t, text, "lifecycle credential label cannot be removed or changed")
	require.Contains(t, text, `int(object.data["readiness-expires-at-unix"]) > int(object.data["activated-at-unix"])`)
	require.Contains(t, text, `object.immutable == true`)
	require.Contains(t, text, `validationActions: [Deny]`)

	for _, document := range splitYAMLDocuments(t, data) {
		var object map[string]any
		require.NoError(t, yaml.Unmarshal(document, &object))
		require.NotEmpty(t, object["apiVersion"])
		require.NotEmpty(t, object["kind"])
	}
}

func splitYAMLDocuments(t *testing.T, data []byte) [][]byte {
	t.Helper()
	parts := [][]byte{}
	for _, part := range bytes.Split(data, []byte("\n---\n")) {
		if len(part) > 0 {
			parts = append(parts, part)
		}
	}
	return parts
}
