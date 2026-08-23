package production_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBackupSchedulerMemoryMatchesSuccessfulCgroupDrill(t *testing.T) {
	raw := mustRead(t, filepath.Join("..", "..", "deploy", "production", "kubebrain-backup-scheduler.yaml"))
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var document struct {
			Kind string `yaml:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name      string `yaml:"name"`
							Resources struct {
								Requests map[string]string `yaml:"requests"`
								Limits   map[string]string `yaml:"limits"`
							} `yaml:"resources"`
						} `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&document); err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		if document.Kind != "Deployment" {
			continue
		}
		require.Len(t, document.Spec.Template.Spec.Containers, 1)
		container := document.Spec.Template.Spec.Containers[0]
		require.Equal(t, "scheduler", container.Name)
		require.Equal(t, "256Mi", container.Resources.Requests["memory"])
		require.Equal(t, "512Mi", container.Resources.Limits["memory"])
		return
	}
	t.Fatal("backup scheduler Deployment not found")
}

func TestScanMemoryCgroupDrillPersistsExactPodEvidenceAndCleansNamespaces(t *testing.T) {
	f := newScanMemoryCgroupFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"drill-scan-memory-cgroup.sh"}, f.env())
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "503 real apiserver objects")
	require.FileExists(t, filepath.Join(f.evidence, "result.json"))
	require.FileExists(t, filepath.Join(f.evidence, "job.json"))
	require.FileExists(t, filepath.Join(f.evidence, "pod.json"))
	require.Equal(t, os.FileMode(0o600), mustStat(t, filepath.Join(f.evidence, "result.json")).Mode().Perm())
	log := string(mustRead(t, f.log))
	require.Equal(t, 33, strings.Count(log, " create -f -"))
	require.Equal(t, 2, strings.Count(log, "delete namespace kb-scan-memory-"))
}

func TestScanMemoryCgroupDrillRejectsPodImageIDDrift(t *testing.T) {
	f := newScanMemoryCgroupFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"drill-scan-memory-cgroup.sh"}, append(f.env(), "POD_IMAGE_ID=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	require.Error(t, err)
	require.Contains(t, string(out), "imageID drifted")
	require.NoFileExists(t, filepath.Join(f.evidence, "result.json"))
	log := string(mustRead(t, f.log))
	require.Equal(t, 2, strings.Count(log, "delete namespace kb-scan-memory-"))
}

func TestScanMemoryCgroupDrillRequiresExplicitConfirmation(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"drill-scan-memory-cgroup.sh"}, nil)
	require.Error(t, err)
	require.Contains(t, string(out), "CONFIRM_SCAN_MEMORY_CGROUP_DRILL=yes")
}

type scanMemoryCgroupFixture struct {
	dir, evidence, kubectl, log string
}

func newScanMemoryCgroupFixture(t *testing.T) scanMemoryCgroupFixture {
	t.Helper()
	dir := t.TempDir()
	f := scanMemoryCgroupFixture{dir: dir, evidence: filepath.Join(dir, "evidence"), kubectl: filepath.Join(dir, "kubectl"), log: filepath.Join(dir, "calls.log")}
	require.NoError(t, os.Mkdir(f.evidence, 0o700))
	writeTrafficExecutable(t, f.kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CALL_LOG"
args=" $* "
if [[ "$args" == *" create namespace "* ]]; then
  name="${args#* create namespace }"; name="${name%% *}"
  printf '{"metadata":{"name":"%s","uid":"uid-%s"}}\n' "$name" "$name"
  exit 0
fi
if [[ "$args" == *" get namespace "* ]]; then
  name="${args#* get namespace }"; name="${name%% *}"
  printf '{"metadata":{"name":"%s","uid":"uid-%s"}}\n' "$name" "$name"
  exit 0
fi
if [[ "$args" == *" get job scan-memory-probe "* ]]; then
  printf '{"metadata":{"name":"scan-memory-probe"},"status":{"conditions":[{"type":"Complete","status":"True"}]}}\n'
  exit 0
fi
if [[ "$args" == *" get pods "* ]]; then
  printf '{"items":[{"metadata":{"name":"probe-pod"},"spec":{"containers":[{"image":"registry.example/kubebrain:probe","resources":{"requests":{"memory":"256Mi"},"limits":{"memory":"512Mi"}},"securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true}}]},"status":{"phase":"Succeeded","containerStatuses":[{"imageID":"containerd://%s"}]}}]}\n' "${POD_IMAGE_ID:-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
  exit 0
fi
if [[ "$args" == *" logs probe-pod "* ]]; then
  namespace="$(awk '/ create namespace kb-scan-memory-a-/ {for (i=1;i<=NF;i++) if ($i=="namespace") {print $(i+1); exit}}' "$CALL_LOG")"
  suffix="${namespace#kb-scan-memory-a-}"
  printf '{"format":"kubebrain.scan-memory-probe.v1","namespaces":2,"items":503,"charged_bytes":60450000,"page_limit":500,"max_items":10000,"max_bytes":67108864,"label_selector":"dbaas.kubebrain.io/scan-memory-probe=%s","memory_limit_bytes":536870912,"memory_peak_bytes":300000000,"memory_end_bytes":140000000,"heap_alloc_bytes":70000000,"heap_sys_bytes":140000000,"num_gc":4,"pause_total_ns":1000000,"oom_events_delta":0,"oom_kill_events_delta":0,"oom_group_kill_events_delta":0,"elapsed_millis":500}\n' "$suffix"
  exit 0
fi
if [[ "$args" == *" create -f - "* ]]; then cat >/dev/null; exit 0; fi
exit 0
`)
	return f
}

func (f scanMemoryCgroupFixture) env() []string {
	return []string{
		"KUBE_CONTEXT=production", "IMAGE=registry.example/kubebrain:probe",
		"EXPECTED_IMAGE_ID=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EVIDENCE_DIR=" + f.evidence, "CONFIRM_SCAN_MEMORY_CGROUP_DRILL=yes", "KUBECTL=" + f.kubectl, "JQ=jq", "CALL_LOG=" + f.log,
	}
}
