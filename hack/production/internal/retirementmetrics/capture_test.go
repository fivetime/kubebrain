package retirementmetrics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func captureFixture(t *testing.T) (string, CaptureBinding) {
	t.Helper()
	dir := t.TempDir()
	raw := []byte("# TYPE leader_retirement_peer_result counter\nleader_retirement_peer_result{outcome=\"confirmed\"} 1\n")
	start := time.Unix(1800000000, 0).UTC()
	summary, err := json.Marshal(summaryFixture(t, raw, start))
	require.NoError(t, err)
	spec := `{"template":{"spec":{"containers":[{"name":"brain","image":"fixed"}]}}}`
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(spec), &decoded))
	canonical, err := json.Marshal(decoded)
	require.NoError(t, err)
	hash := sha256.Sum256(canonical)
	binding := CaptureBinding{NamespaceUID: "ns-uid", StatefulSetUID: "sts-uid", PodUID: "pod-uid", SpecSHA256: hex.EncodeToString(hash[:])}
	pod := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"ns","uid":"pod-uid","ownerReferences":[{"kind":"StatefulSet","uid":"sts-uid","controller":true}]},"spec":{"nodeName":"node","containers":[{"name":"brain","image":"fixed"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","imageID":"fixed-id","containerID":"process","restartCount":0,"ready":true,"state":{"running":{"startedAt":"start"}}}]}}`
	trace := ""
	for i, stage := range []string{"verify-inputs", "snapshot-before", "identity-before", "protected-probe", "snapshot-after", "identity-after"} {
		sec := start.Unix() + int64(i-3)*2
		trace += fmt.Sprintf("%d.000000\t%s\tstart\t-\n%d.000000\t%s\tend\t0\n", sec, stage, sec+1, stage)
	}
	for name, data := range map[string][]byte{
		"namespace.json":  []byte(`{"metadata":{"name":"ns","uid":"ns-uid"}}`),
		"sts.json":        []byte(`{"metadata":{"namespace":"ns","uid":"sts-uid","generation":1},"status":{"observedGeneration":1},"spec":` + spec + `}`),
		"pod-before.json": []byte(pod), "pod-after.json": []byte(pod), "probe.json": summary, "metrics.txt": raw, "timing.tsv": []byte(trace), "probe.stderr": {},
		"COMPLETE": []byte("CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n"),
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	}
	writeCaptureManifest(t, dir)
	return dir, binding
}

func writeCaptureManifest(t *testing.T, dir string) {
	t.Helper()
	manifest := ""
	for _, name := range []string{"namespace.json", "sts.json", "pod-before.json", "pod-after.json", "probe.json", "metrics.txt", "timing.tsv", "probe.stderr"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		hash := sha256.Sum256(data)
		manifest += fmt.Sprintf("%x  %s\n", hash, path)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "evidence.sha256"), []byte(manifest), 0600))
}

func TestLoadCapture(t *testing.T) {
	for _, scenario := range []string{"success", "readiness-change", "missing-complete", "tampered", "duplicate-manifest", "foreign-path", "symlink", "restarted", "image-changed", "wrong-owner", "changed-spec", "duplicate-json", "failed-stage", "backward-time", "outside-probe", "wrong-binding"} {
		t.Run(scenario, func(t *testing.T) {
			dir, binding := captureFixture(t)
			replace := func(name, old, new string) {
				data, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				require.Contains(t, string(data), old)
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(strings.ReplaceAll(string(data), old, new)), 0600))
			}
			rehash := true
			switch scenario {
			case "readiness-change":
				replace("pod-after.json", `"ready":true`, `"ready":false`)
			case "missing-complete":
				require.NoError(t, os.Remove(filepath.Join(dir, "COMPLETE")))
			case "tampered":
				replace("metrics.txt", " 1\n", " 2\n")
				rehash = false
			case "duplicate-manifest":
				replace("evidence.sha256", "pod-after.json", "pod-before.json")
				rehash = false
			case "foreign-path":
				replace("evidence.sha256", dir, dir+"-other")
				rehash = false
			case "symlink":
				require.NoError(t, os.Rename(filepath.Join(dir, "metrics.txt"), filepath.Join(dir, "saved")))
				require.NoError(t, os.Symlink("saved", filepath.Join(dir, "metrics.txt")))
			case "restarted":
				replace("pod-after.json", `"restartCount":0`, `"restartCount":1`)
			case "image-changed":
				replace("pod-after.json", "fixed-id", "changed-id")
			case "wrong-owner":
				replace("pod-after.json", "sts-uid", "other")
			case "changed-spec":
				replace("pod-after.json", `"nodeName":"node"`, `"nodeName":"other"`)
			case "duplicate-json":
				replace("pod-after.json", `"restartCount":0`, `"restartCount":1,"restartCount":0`)
			case "failed-stage":
				replace("timing.tsv", "\tend\t0", "\tend\t1")
			case "backward-time":
				replace("timing.tsv", "1800000005.000000", "1799999990.000000")
			case "outside-probe":
				replace("timing.tsv", "1800000000.000000", "1800000000.500000")
			case "wrong-binding":
				binding.NamespaceUID = "other"
			}
			if rehash {
				writeCaptureManifest(t, dir)
			}
			_, err := LoadCapture(dir, binding)
			if scenario == "success" || scenario == "readiness-change" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
