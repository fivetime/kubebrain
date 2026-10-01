package testcluster_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalPolicyObserverAndRecoveryWait(t *testing.T) {
	slots := make(chan struct{}, 4)
	observer, err := filepath.Abs("observe-local-policy-state.sh")
	require.NoError(t, err)
	waiter, err := filepath.Abs("../../hack/production/wait-policy-absence.sh")
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		code int
	}{
		{"absent", 0}, {"ready-restored", 0}, {"present", 0}, {"pending", 75}, {"revision-pending", 75},
		{"wrong-identity", 65}, {"conflicting-policy", 65}, {"malformed", 65},
		{"noop-revision-ahead", 0}, {"noop-revision-ahead-present", 0},
		{"noop-revision-ahead-policy-remains", 75}, {"pod-restart", 65},
		{"unlabelled-absent", 0}, {"unlabelled-stale-identity", 65}, {"unlabelled-policy-remains", 75},
		{"unlabelled-foreign-identity", 65}, {"unlabelled-pod-labelled", 65},
		{"wait-converges", 0}, {"wait-fatal", 65}, {"wait-drift-after-pending", 65},
		{"wait-cancel", 124},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // Independent temp directory and per-command environment.
			slots <- struct{}{}
			defer func() { <-slots }()
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "mock-api"), []byte(ciliumCaptureMock), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "kubectl"), []byte(typedPolicyMock), 0700))
			env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "CAPTURE_FIXTURE="+dir, "CAPTURE_SCENARIO=stable", "POLICY_SCENARIO="+tc.name)
			seed := exec.Command("bash", filepath.Join(dir, "mock-api"), "get", "pod", "kubebrain-local-0")
			seed.Env = env
			pod, err := seed.CombinedOutput()
			require.NoError(t, err, string(pod))
			expected := filepath.Join(dir, "expected.json")
			require.NoError(t, os.WriteFile(expected, pod, 0600))
			require.NoError(t, os.Remove(filepath.Join(dir, "app-seen")))
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			mode := "absent"
			if tc.name == "present" || tc.name == "noop-revision-ahead-present" {
				mode = "present"
			}
			if strings.HasPrefix(tc.name, "unlabelled-") {
				mode = "absent-unlabelled"
			}
			cmd := exec.CommandContext(ctx, "bash", observer, dir, mode, "policy-uid", "kb-term-test", expected)
			if strings.HasPrefix(tc.name, "wait-") {
				callback := filepath.Join(dir, "callback.sh")
				// Test paths are quoted; only generated private temp paths are embedded.
				body := "#!/usr/bin/env bash\nexec bash '" + observer + "' '" + dir + "' \"$1\" policy-uid kb-term-test '" + expected + "'\n"
				require.NoError(t, os.WriteFile(callback, []byte(body), 0600))
				hash := sha256.Sum256([]byte(body))
				cmd = exec.CommandContext(ctx, "bash", waiter, filepath.Join(dir, "wait"), strconv.FormatInt(time.Now().Add(10*time.Second).UnixNano(), 10), callback, hex.EncodeToString(hash[:]))
				if tc.name == "wait-cancel" {
					cmd = exec.CommandContext(ctx, "timeout", "--kill-after=1s", "2s", "bash", waiter, filepath.Join(dir, "wait"), strconv.FormatInt(time.Now().Add(10*time.Second).UnixNano(), 10), callback, hex.EncodeToString(hash[:]))
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
				require.Equal(t, tc.code, exit.ExitCode(), string(output))
			}
			observations, err := filepath.Glob(filepath.Join(dir, "policy-observation.*"))
			require.NoError(t, err)
			if tc.name == "wait-converges" || tc.name == "wait-drift-after-pending" {
				require.Len(t, observations, 2)
			} else {
				require.Len(t, observations, 1)
			}
			var codes []string
			for _, observation := range observations {
				code, err := os.ReadFile(filepath.Join(observation, "observation.exit"))
				require.NoError(t, err)
				codes = append(codes, strings.TrimSpace(string(code)))
				if tc.name == "wait-cancel" || (tc.code == 65 && !(tc.name == "wait-drift-after-pending" && strings.TrimSpace(string(code)) == "75")) {
					if tc.name == "wait-cancel" {
						require.Equal(t, "143", strings.TrimSpace(string(code)))
					} else {
						require.Equal(t, "65", strings.TrimSpace(string(code)))
					}
					_, err := os.Stat(filepath.Join(observation, "evidence.sha256"))
					require.True(t, os.IsNotExist(err))
				} else {
					check := exec.Command("sha256sum", "-c", filepath.Join(observation, "evidence.sha256"))
					result, err := check.CombinedOutput()
					require.NoError(t, err, string(result))
				}
			}
			if tc.name == "wait-drift-after-pending" {
				require.ElementsMatch(t, []string{"75", "65"}, codes)
			}
			if tc.name == "wait-cancel" {
				pid, err := os.ReadFile(filepath.Join(dir, "blocked.pid"))
				require.NoError(t, err)
				require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run())
			}
		})
	}
}

const typedPolicyMock = `#!/usr/bin/env bash
set -euo pipefail
if [[ $POLICY_SCENARIO == pod-restart ]]; then export CAPTURE_SCENARIO=pod-restart; fi
if [[ $POLICY_SCENARIO == ready-restored ]]; then export CAPTURE_SCENARIO=ready-restored; fi
if [[ $POLICY_SCENARIO == wait-drift-after-pending ]]; then
 count=0
 [[ ! -e $CAPTURE_FIXTURE/capture-count ]] || count=$(<"$CAPTURE_FIXTURE/capture-count")
 if [[ " $* " == *" get namespace "* ]]; then
  count=$((count+1)); printf '%s\n' "$count" > "$CAPTURE_FIXTURE/capture-count"
 fi
 if (( count >= 2 )); then export CAPTURE_SCENARIO=pod-uid-change; fi
fi
if [[ $POLICY_SCENARIO == unlabelled-pod-labelled && " $* " == *" get pod kubebrain-local-0 "* ]]; then
 bash "$CAPTURE_FIXTURE/mock-api" "$@" | jq '.metadata.labels["kubebrain.io/fault-owner"]="term-test"'
 exit
fi
if [[ " $* " != *" exec "* ]]; then exec bash "$CAPTURE_FIXTURE/mock-api" "$@"; fi
if [[ $POLICY_SCENARIO == wait-cancel ]]; then printf '%s\n' "$BASHPID" > "$CAPTURE_FIXTURE/blocked.pid"; exec sleep 60; fi
present=false
case $POLICY_SCENARIO in
 present|pending|unlabelled-policy-remains|noop-revision-ahead-present|noop-revision-ahead-policy-remains) present=true;;
 wait-converges|wait-drift-after-pending) if [[ ! -e $CAPTURE_FIXTURE/policy-seen ]]; then present=true; fi; touch "$CAPTURE_FIXTURE/policy-seen";;
esac
bash "$CAPTURE_FIXTURE/mock-api" "$@" |
 jq --argjson present "$present" --arg scenario "$POLICY_SCENARIO" '
 .[0].status.identity.labels=["k8s:kubebrain.io/fault-owner=term-test"] |
 .[0].status.policy={spec:{"policy-revision":4},realized:{"policy-revision":4,l4:{ingress:[],egress:[]}}} |
 if $present then .[0].status.policy.realized.l4.egress=[{"derived-from-rules":[["k8s:io.cilium.k8s.policy.uid=policy-uid","k8s:io.cilium.k8s.policy.name=kb-term-test"]]}] else . end |
 if $scenario=="wrong-identity" or $scenario=="wait-fatal" or $scenario=="unlabelled-absent" or $scenario=="unlabelled-policy-remains" then .[0].status.identity.labels=[]
 elif $scenario=="unlabelled-foreign-identity" then .[0].status.identity.labels=["k8s:kubebrain.io/fault-owner=foreign"]
 elif $scenario=="conflicting-policy" then .[0].status.policy.realized.l4.egress=[{"derived-from-rules":[["k8s:io.cilium.k8s.policy.name=kb-term-test"]]}]
 elif $scenario=="revision-pending" then .[0].status.policy.spec["policy-revision"]=5
 elif ($scenario|startswith("noop-revision-ahead")) then .[0].status.policy.spec["policy-revision"]=3
 elif $scenario=="malformed" then del(.[0].status.policy.realized.l4)
 else . end'
`
