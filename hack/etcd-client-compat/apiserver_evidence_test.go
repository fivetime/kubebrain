package compat

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIServerWatchLeaseCleanupFailureRetainsEvidence(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "dev", "apiserver-watch-soak.sh"))
	require.NoError(t, err)
	body := string(data)
	start := strings.Index(body, "cleanup() {\n")
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(body[start:], "\n}\n")
	require.Greater(t, end, 0)
	cleanup := body[start : start+end+3]
	work, evidence := t.TempDir(), t.TempDir()
	for _, name := range []string{"configmap-watch.jsonl", "kube-apiserver.log", "private.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(work, name), []byte(name), 0600))
	}
	out, err := runCompatCommandContext(t, context.Background(), "bash", []string{"-c", `set -u
source ../dev/apiserver-watch-evidence.sh
namespace_created=false
process_owned=false
prefix_owned=true
work_dir_created=true
evidence_dir_created=true
bin_lock_owned=false
port_lock_owned=false
ETCD_PREFIX=/registry-kubebrain-apiserver-cleanup-evidence
ETCDCTL=(fake_etcdctl)
fake_etcdctl() {
  case "$1" in
    del) echo 0 ;;
    get) echo '{"count":0,"kvs":[]}' ;;
    *) return 99 ;;
  esac
}
# Valid long-lived leases can outlast the bounded cleanup check. Simulate that
# result without waiting, revoking anything, or changing the real deadline.
verify_apiserver_lease_cleanup() { return 1; }
` + cleanup + "\n(exit 0)\ncleanup\n"}, []string{"WORK_DIR=" + work, "EVIDENCE_DIR=" + evidence})
	require.Error(t, err, "%s", out)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 70, exitErr.ExitCode())
	resultBytes, err := os.ReadFile(filepath.Join(evidence, "result.json"))
	require.NoError(t, err)
	var result map[string]int
	require.NoError(t, json.Unmarshal(resultBytes, &result))
	require.Equal(t, map[string]int{"operation_exit": 0, "cleanup_failed": 1, "archive_failed": 0, "runner_exit": 70}, result)
	for _, name := range []string{"configmap-watch.jsonl", "kube-apiserver.log"} {
		archived, err := os.ReadFile(filepath.Join(evidence, name))
		require.NoError(t, err)
		require.Equal(t, name, string(archived))
	}
	require.NoFileExists(t, filepath.Join(evidence, "private.key"))
	require.NoDirExists(t, work)
}

func TestAPIServerWatchCleanupPersistsFailureBeforeRemovingWork(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "dev", "apiserver-watch-soak.sh"))
	require.NoError(t, err)
	body := string(data)
	start := strings.Index(body, "cleanup() {\n")
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(body[start:], "\n}\n")
	require.Greater(t, end, 0)
	cleanup := body[start : start+end+3]
	for _, archiveFails := range []bool{false, true} {
		name := "operation-failed"
		if archiveFails {
			name = "archive-failed"
		}
		t.Run(name, func(t *testing.T) {
			work, evidence := t.TempDir(), t.TempDir()
			if archiveFails {
				require.NoError(t, os.Symlink("missing", filepath.Join(work, "configmap-watch.jsonl")))
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(work, "configmap-watch.jsonl"), []byte("watch evidence"), 0600))
			}
			out, err := runCompatCommandContext(t, context.Background(), "bash", []string{"-c", `set -u
source ../dev/apiserver-watch-evidence.sh
namespace_created=false
process_owned=false
prefix_owned=false
work_dir_created=true
evidence_dir_created=true
bin_lock_owned=false
port_lock_owned=false
` + cleanup + "\n(exit 42)\ncleanup\n"}, []string{"WORK_DIR=" + work, "EVIDENCE_DIR=" + evidence})
			require.Error(t, err, "%s", out)
			resultBytes, err := os.ReadFile(filepath.Join(evidence, "result.json"))
			require.NoError(t, err)
			var result map[string]int
			require.NoError(t, json.Unmarshal(resultBytes, &result))
			require.Equal(t, 42, result["operation_exit"])
			_, statErr := os.Stat(work)
			if archiveFails {
				require.Equal(t, 70, result["runner_exit"])
				require.Equal(t, 1, result["archive_failed"])
				require.NoError(t, statErr)
			} else {
				require.Equal(t, 42, result["runner_exit"])
				require.True(t, os.IsNotExist(statErr))
				archived, err := os.ReadFile(filepath.Join(evidence, "configmap-watch.jsonl"))
				require.NoError(t, err)
				require.Equal(t, "watch evidence", string(archived))
			}
		})
	}
}

func TestAPIServerWatchEvidenceArchivesOnlyDiagnostics(t *testing.T) {
	work, evidence := t.TempDir(), t.TempDir()
	for _, name := range []string{"configmap-watch.jsonl", "kube-apiserver.log", "kubeconfig", "private.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(work, name), []byte(name), 0600))
	}
	run := func() ([]byte, error) {
		return runCompatCommandContext(t, context.Background(), "bash", []string{"-c", `set -euo pipefail
source ../dev/apiserver-watch-evidence.sh
archive_apiserver_watch_evidence "$EVIDENCE" "$WORK"
`}, []string{"EVIDENCE=" + evidence, "WORK=" + work})
	}
	out, err := run()
	require.NoError(t, err, "%s", out)
	entries, err := os.ReadDir(evidence)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for _, name := range []string{"configmap-watch.jsonl", "kube-apiserver.log"} {
		data, err := os.ReadFile(filepath.Join(evidence, name))
		require.NoError(t, err)
		require.Equal(t, name, string(data))
		info, err := os.Stat(filepath.Join(evidence, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
	_, err = run()
	require.Error(t, err, "must not overwrite a prior archive")
}

func TestAPIServerWatchEvidenceRejectsSymlink(t *testing.T) {
	work, evidence := t.TempDir(), t.TempDir()
	require.NoError(t, os.Symlink("missing-private-key", filepath.Join(work, "configmap-watch.jsonl")))
	out, err := runCompatCommandContext(t, context.Background(), "bash", []string{"-c", `source ../dev/apiserver-watch-evidence.sh
archive_apiserver_watch_evidence "$EVIDENCE" "$WORK"`}, []string{"EVIDENCE=" + evidence, "WORK=" + work})
	require.Error(t, err)
	require.Contains(t, string(out), "refusing symlink evidence source")
}

func TestAPIServerWatchRejectsOverlappingEvidenceDirectory(t *testing.T) {
	for _, kind := range []string{"same", "child", "existing"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			work := filepath.Join(root, "work")
			evidence := work
			if kind == "child" {
				evidence = filepath.Join(work, "evidence")
			}
			if kind == "existing" {
				evidence = filepath.Join(root, "archive")
				require.NoError(t, os.Mkdir(evidence, 0700))
			}
			out, err := runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "dev", "apiserver-watch-soak.sh")}, []string{
				"ALLOW_MUTATING_APISERVER_WATCH_SOAK=true", "RUN_ID=evidence-test", "ETCD_PREFIX=/registry-kubebrain-apiserver-evidence-test",
				"WORK_ROOT=" + root, "WORK_DIR=" + work, "EVIDENCE_DIR=" + evidence,
			})
			require.Error(t, err)
			require.Contains(t, string(out), "EVIDENCE_DIR must be a new child")
		})
	}
}
