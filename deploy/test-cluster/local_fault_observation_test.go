package testcluster_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalDropAndLabelIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, kind, mode string
		code             int
	}{
		{"stable", "drop", "", 0}, {"ready-change", "drop", "", 0},
		{"pod-restart", "drop", "", 1}, {"agent-restart", "drop", "", 1},
		{"pod-image", "drop", "", 1}, {"cep-identity", "drop", "", 1},
		{"remote-failed", "drop", "", 1}, {"missing-remote-marker", "drop", "", 1},
		{"initial-capture-failed", "drop", "", 1}, {"cancel", "drop", "", 124},
		{"stable", "label", "present", 0}, {"stable", "label", "absent", 0},
		{"ready-change", "label", "absent", 0}, {"pending", "label", "absent", 75},
		{"pending", "label", "present", 75}, {"foreign-endpoint", "label", "absent", 65},
		{"foreign-pod", "label", "absent", 65}, {"expected-replaced", "label", "absent", 65},
		{"identity-wait", "label", "present", 75}, {"identity-wait", "label", "absent", 75},
		{"identity-wait-foreign", "label", "present", 65}, {"unknown-state", "label", "present", 65},
		{"identity-wait", "drop", "", 1},
		{"identity-wait-replaced", "label", "present", 65}, {"identity-wait-mismatch", "label", "present", 65},
		{"regenerating", "label", "present", 75}, {"regenerating", "label", "absent", 75},
		{"cep-lag", "label", "present", 75}, {"cep-lag", "label", "absent", 75},
		{"label-base-drift", "label", "present", 65}, {"regenerating", "drop", "", 1},
		{"label-cancel", "label", "present", 124}, {"label-inner-timeout", "label", "absent", 124},
	} {
		t.Run(tc.kind+"/"+tc.mode+"/"+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "mock-api"), []byte(ciliumCaptureMock), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "kubectl"), []byte(localFaultObservationMock), 0700))
			if tc.name == "label-inner-timeout" {
				// Compress only the inner kubectl deadline; keep real timeout signaling.
				require.NoError(t, os.WriteFile(filepath.Join(bin, "timeout"), []byte("#!/usr/bin/env bash\nset -euo pipefail\nif [[ ${1:-} == --foreground && ${3:-} == 20s && ${4:-} == kubectl ]]; then set -- \"$1\" \"$2\" 0.2s \"${@:4}\"; fi\nexec /usr/bin/timeout \"$@\"\n"), 0700))
			}
			env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "CAPTURE_FIXTURE="+dir, "CAPTURE_SCENARIO=stable", "FAULT_KIND="+tc.kind, "FAULT_MODE="+tc.mode, "FAULT_SCENARIO="+tc.name)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "capture-local-cilium-drops.sh", dir, "kubebrain-local-0", "1")
			if tc.name == "cancel" {
				cmd = exec.CommandContext(ctx, "timeout", "--kill-after=1s", "2s", "bash", "capture-local-cilium-drops.sh", dir, "kubebrain-local-0", "1")
			}
			if tc.kind == "label" {
				seed := exec.Command("bash", filepath.Join(dir, "mock-api"), "get", "pod", "kubebrain-local-0")
				seed.Env = env
				pod, err := seed.CombinedOutput()
				require.NoError(t, err, string(pod))
				if tc.name == "expected-replaced" || tc.name == "identity-wait-replaced" {
					pod = []byte(strings.Replace(string(pod), `"uid": "app"`, `"uid": "old"`, 1))
				}
				expected := filepath.Join(dir, "expected.json")
				require.NoError(t, os.WriteFile(expected, pod, 0600))
				require.NoError(t, os.Remove(filepath.Join(dir, "app-seen")))
				cmd = exec.CommandContext(ctx, "bash", "observe-local-fault-label.sh", dir, tc.mode, expected, "term-test")
				if tc.name == "label-cancel" {
					cmd = exec.CommandContext(ctx, "timeout", "--kill-after=1s", "2s", "bash", "observe-local-fault-label.sh", dir, tc.mode, expected, "term-test")
				}
			}
			cmd.Env = env
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(output))
			if tc.code == 0 {
				require.NoError(t, err, string(output))
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				// jq identity mismatch returns 1; parsing failures may return 5.
				if tc.kind == "label" || tc.name == "cancel" {
					require.Equal(t, tc.code, exit.ExitCode(), string(output))
				}
			}
			pattern := "drops.*"
			if tc.kind == "label" {
				pattern = "identity-observation.*"
			}
			matches, err := filepath.Glob(filepath.Join(dir, pattern))
			require.NoError(t, err)
			require.Len(t, matches, 1)
			manifest := filepath.Join(matches[0], "evidence.sha256")
			if tc.code == 0 || tc.code == 75 {
				verify := exec.Command("sha256sum", "-c", manifest)
				result, err := verify.CombinedOutput()
				require.NoError(t, err, string(result))
			} else {
				_, err := os.Stat(manifest)
				require.True(t, os.IsNotExist(err))
			}
			if tc.name == "initial-capture-failed" {
				_, err := os.Stat(filepath.Join(dir, "window-after"))
				require.True(t, os.IsNotExist(err), "must not start monitor after rejected initial capture")
			}
			if tc.name == "cep-lag" {
				// A second full capture observes convergence; the first must remain pending.
				next := exec.CommandContext(ctx, "bash", "observe-local-fault-label.sh", dir, tc.mode, filepath.Join(dir, "expected.json"), "term-test")
				next.Env = env
				output, err := next.CombinedOutput()
				require.NoError(t, err, string(output))
				observations, err := filepath.Glob(filepath.Join(dir, "identity-observation.*"))
				require.NoError(t, err)
				require.Len(t, observations, 2)
				for _, observation := range observations {
					output, err := exec.Command("sha256sum", "-c", filepath.Join(observation, "evidence.sha256")).CombinedOutput()
					require.NoError(t, err, string(output))
				}
			}
			if tc.name == "cancel" {
				pid, err := os.ReadFile(filepath.Join(dir, "blocked.pid"))
				require.NoError(t, err)
				require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run())
				code, err := os.ReadFile(filepath.Join(matches[0], "capture.exit"))
				require.NoError(t, err)
				require.Equal(t, "143", strings.TrimSpace(string(code)))
			}
			if strings.HasPrefix(tc.name, "label-") && tc.code == 124 {
				pid, err := os.ReadFile(filepath.Join(dir, "blocked.pid"))
				require.NoError(t, err)
				require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run())
				want := "124"
				if tc.name == "label-cancel" {
					want = "143"
				}
				code, err := os.ReadFile(filepath.Join(matches[0], "observation.exit"))
				require.NoError(t, err)
				require.Equal(t, want, strings.TrimSpace(string(code)))
				captures, err := filepath.Glob(filepath.Join(dir, "endpoint.*"))
				require.NoError(t, err)
				require.Len(t, captures, 1)
				code, err = os.ReadFile(filepath.Join(captures[0], "capture.exit"))
				require.NoError(t, err)
				require.Equal(t, want, strings.TrimSpace(string(code)))
				_, err = os.Stat(filepath.Join(captures[0], "evidence.sha256"))
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}

const localFaultObservationMock = `#!/usr/bin/env bash
set -euo pipefail
if [[ ( $FAULT_SCENARIO == label-cancel || $FAULT_SCENARIO == label-inner-timeout ) && " $* " == *" cilium-dbg endpoint get "* ]]; then
 printf '%s\n' "$BASHPID" > "$CAPTURE_FIXTURE/blocked.pid"
 exec sleep 60
fi
if [[ $FAULT_SCENARIO == initial-capture-failed ]]; then export CAPTURE_SCENARIO=wrong-namespace; fi
if [[ " $* " == *" monitor "* ]]; then
 if [[ $FAULT_SCENARIO == cancel ]]; then printf '%s\n' "$BASHPID" > "$CAPTURE_FIXTURE/blocked.pid"; exec sleep 60; fi
 touch "$CAPTURE_FIXTURE/window-after"
 [[ $FAULT_SCENARIO != remote-failed ]] || exit 1
 [[ $FAULT_SCENARIO == missing-remote-marker ]] || echo 'command terminated with exit code 124' >&2
 printf '{"synthetic":true}\n'
 exit 124
fi
after=false
[[ ! -e $CAPTURE_FIXTURE/window-after ]] || after=true
cep_after=false
[[ ! -e $CAPTURE_FIXTURE/cep-seen ]] || cep_after=true
bash "$CAPTURE_FIXTURE/mock-api" "$@" |
jq --arg kind "$FAULT_KIND" --arg mode "$FAULT_MODE" --arg scenario "$FAULT_SCENARIO" --argjson after "$after" --argjson cep_after "$cep_after" '
 def labelset:
  ["k8s:app=kubebrain"] +
  (if $scenario=="foreign-endpoint" or $scenario=="identity-wait-foreign" then ["k8s:kubebrain.io/fault-owner=term-foreign"]
   elif ($mode=="present" and $scenario!="pending") or ($mode=="absent" and $scenario=="pending") then ["k8s:kubebrain.io/fault-owner=term-test"] else [] end);
 def change:
  if $kind=="drop" and $after then
   if .kind=="Pod" and .metadata.uid=="app" then
    if $scenario=="ready-change" then .status.containerStatuses[0].ready=false
    elif $scenario=="pod-restart" then .status.containerStatuses[0].restartCount=1
    elif $scenario=="pod-image" then .status.containerStatuses[0].imageID="other" else . end
   elif .kind=="Pod" and .metadata.uid=="agent" and $scenario=="agent-restart" then .status.containerStatuses[0].restartCount=1
   elif .metadata.uid=="cep-uid" and $scenario=="cep-identity" then .status.identity.id=456
   else . end
  elif $kind=="label" and .metadata.uid=="cep-uid" then
   .status.identity.labels=labelset |
   if $scenario=="cep-lag" then
    .status.identity.id=(if $cep_after then 456 else 123 end) |
    if $cep_after then . else
     .status.identity.labels=(["k8s:app=kubebrain"]+(if $mode=="absent" then ["k8s:kubebrain.io/fault-owner=term-test"] else [] end)) end
   elif $scenario=="label-base-drift" then .status.identity.labels += ["k8s:unexpected=other"]
   else . end
  elif $kind=="label" and .kind=="Pod" and .metadata.uid=="app" then
   (if $mode=="present" then .metadata.labels={"kubebrain.io/fault-owner":"term-test"} else . end) |
   (if $scenario=="foreign-pod" then .metadata.labels={"kubebrain.io/fault-owner":"term-foreign"} else . end) |
   (if $scenario=="ready-change" then .status.containerStatuses[0].ready=false else . end)
  else . end;
 if type=="array" then
  (if ($scenario|startswith("identity-wait")) then .[0].status.state="waiting-for-identity"
   elif $scenario=="unknown-state" then .[0].status.state="unknown-state"
   elif $scenario=="regenerating" then .[0].status.state="regenerating" else . end) |
  (if $scenario=="identity-wait-mismatch" then .[0].status.identity.id=456 else . end) |
  (if $scenario=="cep-lag" then .[0].status.identity.id=456 else . end) |
  if $kind=="label" then
   .[0].status.identity.labels=labelset
  elif $after and $scenario=="cep-identity" then .[0].status.identity.id=456 else . end
 elif has("items") then .items |= map(change)
 else change end'
`
