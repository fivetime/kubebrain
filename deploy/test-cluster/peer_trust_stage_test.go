package testcluster_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerTrustStagePreflightRejectsUnverifiedInputs(t *testing.T) {
	for _, scenario := range []string{"valid-expand", "valid-restore", "changed-verifier", "changed-root", "changed-key", "wrong-client-key", "wrong-client-root", "insecure-baseline", "wrong-mode"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			trustMaterialFixture(t, dir, false, false, false)
			read := func(path string) []byte { data, err := os.ReadFile(path); require.NoError(t, err); return data }
			write := func(path string, data []byte) { require.NoError(t, os.WriteFile(path, data, 0600)) }
			// Fixture members may share a test leaf: this is the old-shared-leaf first
			// phase, not validation of the subsequent holder-distinct policy deployment.
			bundle := filepath.Join(dir, "bundle")
			require.NoError(t, os.Mkdir(bundle, 0700))
			for _, name := range []string{"kubebrain-local-0", "kubebrain-local-1", "kubebrain-local-2"} {
				member := filepath.Join(bundle, name)
				require.NoError(t, os.Mkdir(member, 0700))
				for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
					write(filepath.Join(member, file), read(filepath.Join(dir, "member", file)))
				}
			}
			client := filepath.Join(dir, "client")
			require.NoError(t, os.Mkdir(client, 0700))
			for target, source := range map[string]string{"ca.crt": "ca.crt", "probe.crt": "tls.crt", "probe.key": "tls.key"} {
				write(filepath.Join(client, target), read(filepath.Join(dir, "original", source)))
			}
			checker, err := filepath.Abs("verify-peer-trust-material.sh")
			require.NoError(t, err)
			digest := sha256.Sum256(read(checker))
			receipt := trustPlanInput(t)
			for target, source := range map[string]string{"original_secret": "original", "expanded_secret": "expanded"} {
				data := map[string]any{}
				for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
					data[file] = base64.StdEncoding.EncodeToString(read(filepath.Join(dir, source, file)))
				}
				trustMap(receipt, target)["data"] = data
			}
			receipt["verification"] = map[string]any{"bundle_dir": bundle, "client_tls_dir": client, "material_verifier": checker, "material_verifier_sha256": hex.EncodeToString(digest[:]), "peer_dns": "peer.test", "client_dns": "client.test", "cluster_id": "42"}
			mode := "expand"
			switch scenario {
			case "valid-restore":
				mode = "restore"
			case "wrong-mode":
				mode = "anything"
			case "changed-verifier":
				trustMap(receipt, "verification")["material_verifier_sha256"] = hex.EncodeToString(make([]byte, 32))
			case "changed-root":
				trustMap(receipt, "expanded_secret", "data")["ca.crt"] = trustMap(receipt, "original_secret", "data")["ca.crt"]
			case "changed-key":
				trustMap(receipt, "expanded_secret", "data")["tls.key"] = base64.StdEncoding.EncodeToString(read(filepath.Join(dir, "member", "tls.key")))
			case "wrong-client-key":
				write(filepath.Join(client, "probe.key"), read(filepath.Join(dir, "member", "tls.key")))
			case "wrong-client-root":
				write(filepath.Join(client, "ca.crt"), read(filepath.Join(dir, "member", "ca.crt")))
			case "insecure-baseline":
				trustMap(receipt, "baseline", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)["args"] = []any{"--peer-allow-insecure=true"}
			}
			raw, err := json.Marshal(receipt)
			require.NoError(t, err)
			write(filepath.Join(dir, "receipt.json"), raw)
			// Preflight must not depend on a healthy cluster, especially during restore.
			// Any attempted Kubernetes access fails and leaves a visible marker.
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/bin/sh\ntouch \"$UNEXPECTED_API\"\nexit 77\n"), 0700))
			cmd := exec.Command("bash", "verify-peer-trust-stage.sh", mode, "preflight", dir, "/unused/kubeconfig", "test-context")
			marker := filepath.Join(dir, "unexpected-api")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "UNEXPECTED_API="+marker)
			output, err := cmd.CombinedOutput()
			if scenario == "valid-expand" || scenario == "valid-restore" {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "FIRST_TRUST_MATERIAL_PREFLIGHT_OK")
			} else {
				require.Error(t, err, string(output))
			}
			require.NoFileExists(t, marker)
			require.NotContains(t, string(output), "BEGIN PRIVATE KEY")
		})
	}
}
