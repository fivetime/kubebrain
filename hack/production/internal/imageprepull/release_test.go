package imageprepull

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func releaseFixture(t *testing.T) (map[string]any, map[string]string) {
	t.Helper()
	reviewed := map[string]string{"linux/amd64": "sha256:" + strings.Repeat("a", 64), "linux/arm64": "sha256:" + strings.Repeat("b", 64)}
	var manifests []any
	for _, arch := range []string{"amd64", "arm64"} {
		manifests = append(manifests, map[string]any{"mediaType": ociManifestMediaType, "digest": reviewed["linux/"+arch], "size": 123,
			"platform": map[string]any{"os": "linux", "architecture": arch}})
	}
	return map[string]any{"schemaVersion": 2, "mediaType": ociIndexMediaType, "manifests": manifests}, reviewed
}

func releaseBytes(t *testing.T, index map[string]any) (string, []byte) {
	t.Helper()
	data, err := json.Marshal(index)
	require.NoError(t, err)
	return fmt.Sprintf("ghcr.io/fivetime/kubebrain@sha256:%x", sha256.Sum256(data)), data
}

func TestApprovedRuntimeDigestsVerifiesIndexAndPlatformMapping(t *testing.T) {
	index, reviewed := releaseFixture(t)
	image, data := releaseBytes(t, index)
	approved, err := ApprovedRuntimeDigests(image, data, reviewed)
	require.NoError(t, err)
	_, indexDigest, _ := strings.Cut(image, "@")
	require.Equal(t, []string{reviewed["linux/amd64"], indexDigest}, approved["linux/amd64"])
	require.Equal(t, []string{reviewed["linux/arm64"], indexDigest}, approved["linux/arm64"])
	require.True(t, imageIDMatches(image, "cri-o://"+indexDigest, approved["linux/amd64"]))
	require.False(t, imageIDMatches(image, reviewed["linux/arm64"], approved["linux/amd64"]))
	_, err = ApprovedRuntimeDigests(image, append(data, '\n'), reviewed)
	require.ErrorContains(t, err, "bytes do not match")
	reviewed["linux/amd64"] = "sha256:" + strings.Repeat("c", 64)
	_, err = ApprovedRuntimeDigests(image, data, reviewed)
	require.ErrorContains(t, err, "CI evidence")
}

func TestApprovedRuntimeDigestsNeverRunsAttestation(t *testing.T) {
	index, reviewed := releaseFixture(t)
	attestationDigest := "sha256:" + strings.Repeat("c", 64)
	index["manifests"] = append(index["manifests"].([]any), map[string]any{
		"mediaType": ociManifestMediaType, "digest": attestationDigest, "size": 100,
		"platform":    map[string]any{"os": "unknown", "architecture": "unknown"},
		"annotations": map[string]string{"vnd.docker.reference.type": "attestation-manifest", "vnd.docker.reference.digest": reviewed["linux/amd64"]},
	})
	image, data := releaseBytes(t, index)
	approved, err := ApprovedRuntimeDigests(image, data, reviewed)
	require.NoError(t, err)
	for _, digests := range approved {
		require.NotContains(t, digests, attestationDigest)
	}
}

func TestApprovedRuntimeDigestsRejectsAmbiguousOrUnsupportedSelection(t *testing.T) {
	for _, mode := range []string{"schema", "index type", "artifact", "missing platform", "uppercase architecture", "duplicate platform", "duplicate digest", "nested index", "bad size", "digest type", "high CPU variant", "OS feature", "extra platform", "unlinked attestation"} {
		t.Run(mode, func(t *testing.T) {
			index, reviewed := releaseFixture(t)
			manifests := index["manifests"].([]any)
			first := manifests[0].(map[string]any)
			platform := first["platform"].(map[string]any)
			switch mode {
			case "schema":
				index["schemaVersion"] = 1
			case "index type":
				index["mediaType"] = "unknown"
			case "artifact":
				index["artifactType"] = "application/example"
			case "missing platform":
				delete(first, "platform")
			case "uppercase architecture":
				delete(platform, "architecture")
				platform["Architecture"] = "amd64"
			case "duplicate platform":
				manifests[1].(map[string]any)["platform"] = platform
			case "duplicate digest":
				manifests[1].(map[string]any)["digest"] = first["digest"]
			case "nested index":
				first["mediaType"] = ociIndexMediaType
			case "bad size":
				first["size"] = 0
			case "digest type":
				first["digest"] = 123
			case "high CPU variant":
				platform["variant"] = "v3"
			case "OS feature":
				platform["os.features"] = []string{"required"}
			case "extra platform":
				platform["architecture"] = "riscv64"
			case "unlinked attestation":
				platform["os"], platform["architecture"] = "unknown", "unknown"
			}
			image, data := releaseBytes(t, index)
			approved, err := ApprovedRuntimeDigests(image, data, reviewed)
			require.Error(t, err)
			require.Nil(t, approved)
		})
	}
}

func TestReleaseJSONRejectsAliasesDuplicatesAndTrailingData(t *testing.T) {
	for _, data := range []string{`{"digest":"first","digest":"second"}`, `{"platform":{"os":"linux","OS":"windows"}}`, `{} {}`, strings.Repeat("[", 34) + strings.Repeat("]", 34), `{"incomplete":`} {
		require.Error(t, uniqueJSON([]byte(data)), data)
	}
	require.NoError(t, uniqueJSON([]byte(`{"schemaVersion":2,"manifests":[],"annotations":{"note":"test"}}`)))
}
