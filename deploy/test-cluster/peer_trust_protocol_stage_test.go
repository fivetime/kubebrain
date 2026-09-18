package testcluster_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerProtocolPreflightBindsAuditedArtifactsWithoutExecutingThem(t *testing.T) {
	for _, scenario := range []string{"expand", "restore", "changed-audit", "changed-probe", "wrong-image", "wrong-source", "missing-ci", "symlink", "not-executable"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			in := memberStageFixture(t, dir, false)
			in["phase"] = "protocol"
			image := "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("a", 64)
			in["candidate_image"] = image
			audit := map[string]any{"scope": "published_image_identity_only", "executed_platform": "linux/amd64", "image": image, "source": strings.Repeat("b", 40), "amd64_digest": "sha256:" + strings.Repeat("c", 64), "image_ci": 1, "probe_ci": 2}
			switch scenario {
			case "wrong-image":
				audit["image"] = "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("d", 64)
			case "wrong-source":
				audit["source"] = "branch-name"
			case "missing-ci":
				delete(audit, "image_ci")
			}
			auditPath, probePath := filepath.Join(dir, "audit.json"), filepath.Join(dir, "control-probe")
			data, err := json.Marshal(audit)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(auditPath, data, 0600))
			auditHash := sha256.Sum256(data)
			probe := []byte("#!/bin/sh\ntouch \"$UNEXPECTED_API\"\nexit 99\n")
			require.NoError(t, os.WriteFile(probePath, probe, 0700))
			probeHash := sha256.Sum256(probe)
			v := trustMap(in, "verification")
			v["image_audit"], v["image_audit_sha256"] = auditPath, hex.EncodeToString(auditHash[:])
			v["control_probe"], v["control_probe_sha256"] = probePath, hex.EncodeToString(probeHash[:])
			switch scenario {
			case "changed-audit":
				require.NoError(t, os.WriteFile(auditPath, append(data, '\n'), 0600))
			case "changed-probe":
				require.NoError(t, os.WriteFile(probePath, append(probe, '\n'), 0700))
			case "symlink":
				link := filepath.Join(dir, "probe-link")
				require.NoError(t, os.Symlink(probePath, link))
				v["control_probe"] = link
			case "not-executable":
				require.NoError(t, os.Chmod(probePath, 0600))
			}
			data, err = json.Marshal(in)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "receipt.json"), data, 0600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "kubectl"), probe, 0700))
			marker := filepath.Join(dir, "unexpected-api")
			mode := "expand"
			if scenario == "restore" {
				mode = "restore"
			}
			cmd := exec.Command("bash", "verify-peer-trust-stage.sh", mode, "preflight", dir, "/unused/config", "test")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "UNEXPECTED_API="+marker)
			output, err := cmd.CombinedOutput()
			if scenario == "expand" || scenario == "restore" {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.NoFileExists(t, marker, "preflight must neither contact API nor execute the control probe")
			require.NotContains(t, string(output), "BEGIN PRIVATE KEY")
		})
	}
}
