package production_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSuccessorObservationPreservesHistoryAndOriginalDeadline(t *testing.T) {
	for _, scenario := range []string{"history", "wrong-cluster", "wrong-member", "same-term", "numeric-term", "same-leader", "multiple-json", "late-success", "expired", "future"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			callback := filepath.Join(dir, "callback")
			require.NoError(t, os.WriteFile(callback, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" >> "$FIXTURE/budgets"
n=${2%/status.json}; n=${n##*/sample-}
if [[ $SCENARIO == history && $n == 0 ]]; then printf 'partial{' > "$2"; echo failure >&2; exit 7; fi
if [[ $SCENARIO == late-success ]]; then sleep 2; fi
cluster=42; member=7; leader=9; term='"9007199254740993"'
case "$SCENARIO" in
 wrong-cluster) cluster=43;;
 wrong-member) member=8;;
 same-term) term='"9007199254740992"';;
 numeric-term) term=9007199254740993;;
 same-leader) leader=5;;
 history) if [[ $n == 1 ]]; then leader=5; fi;;
esac
if [[ $SCENARIO == multiple-json ]]; then printf '{}\n' > "$2";else : > "$2";fi
printf '{"header":{"cluster_id":"%s","member_id":"%s","raft_term":%s},"leader":"%s"}' "$cluster" "$member" "$term" "$leader" >> "$2"
`), 0700))
			start := time.Now().Add(-29 * time.Second).UnixNano()
			if scenario == "history" {
				start = time.Now().UnixNano()
			}
			if scenario == "expired" {
				start = time.Now().Add(-31 * time.Second).UnixNano()
			}
			if scenario == "future" {
				start = time.Now().Add(time.Hour).UnixNano()
			}
			out := filepath.Join(dir, "observations")
			cmd := exec.Command("bash", "observe-successor.sh", out, strconv.FormatInt(start, 10), "42", "7", "5", "9007199254740992", callback)
			cmd.Env = append(os.Environ(), "SCENARIO="+scenario, "FIXTURE="+dir)
			began := time.Now()
			output, err := cmd.CombinedOutput()
			require.Less(t, time.Since(began), 5*time.Second, "must not start a fresh thirty-second gate")
			if scenario == "history" {
				require.NoError(t, err, string(output))
				path, err := os.ReadFile(filepath.Join(out, "successor-sample"))
				require.NoError(t, err)
				require.Equal(t, filepath.Join(out, "sample-2")+"\n", string(path))
				partial, err := os.ReadFile(filepath.Join(out, "sample-0", "status.json"))
				require.NoError(t, err)
				require.Equal(t, "partial{", string(partial))
				for i := 0; i < 3; i++ {
					raw, err := os.ReadFile(filepath.Join(out, fmt.Sprintf("sample-%d", i), "observation.json"))
					require.NoError(t, err)
					var event struct {
						Started string `json:"started_ns"`
						Ended   string `json:"ended_ns"`
						Exit    int    `json:"exit_code"`
					}
					require.NoError(t, json.Unmarshal(raw, &event))
					s, err := strconv.ParseInt(event.Started, 10, 64)
					require.NoError(t, err)
					e, err := strconv.ParseInt(event.Ended, 10, 64)
					require.NoError(t, err)
					require.GreaterOrEqual(t, s, start)
					require.GreaterOrEqual(t, e, s)
					require.LessOrEqual(t, e, start+int64(30*time.Second))
					if i == 0 {
						require.Equal(t, 7, event.Exit)
					} else {
						require.Zero(t, event.Exit)
					}
				}
			} else {
				require.Error(t, err, string(output))
				require.NoFileExists(t, filepath.Join(out, "successor-sample"))
			}
			if scenario == "expired" || scenario == "future" {
				require.NoFileExists(t, filepath.Join(dir, "budgets"))
			} else {
				data, err := os.ReadFile(filepath.Join(dir, "budgets"))
				require.NoError(t, err)
				for _, s := range strings.Fields(string(data)) {
					b, err := strconv.ParseFloat(s, 64)
					require.NoError(t, err)
					require.Greater(t, b, 0.0)
					require.LessOrEqual(t, b, 5.0)
					if scenario != "history" {
						require.Less(t, b, 1.0)
					}
				}
			}
			// Exclusive output path prevents overwriting earlier observations.
			cmd = exec.Command("bash", "observe-successor.sh", out, strconv.FormatInt(time.Now().UnixNano(), 10), "42", "7", "5", "9007199254740992", callback)
			require.Error(t, cmd.Run())
		})
	}
}

func TestSuccessorCallbackAdmissionChecksModeWithoutRunning(t *testing.T) {
	for _, scenario := range []string{"executable", "mode-lost", "missing", "symlink", "directory", "relative"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			callback := filepath.Join(dir, "callback")
			marker := filepath.Join(dir, "invoked")
			require.NoError(t, os.WriteFile(callback, []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0700))
			switch scenario {
			case "mode-lost":
				require.NoError(t, os.Chmod(callback, 0600))
			case "missing":
				callback += ".missing"
			case "symlink":
				link := filepath.Join(dir, "link")
				require.NoError(t, os.Symlink(callback, link))
				callback = link
			case "directory":
				callback = dir
			case "relative":
				callback = "observe-successor.sh"
			}
			output, err := exec.Command("bash", "observe-successor.sh", "--check-callback", callback).CombinedOutput()
			if scenario == "executable" {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.NoFileExists(t, marker)
			if scenario == "mode-lost" {
				out := filepath.Join(dir, "observations")
				output, err = exec.Command("bash", "observe-successor.sh", out, strconv.FormatInt(time.Now().UnixNano(), 10), "42", "7", "5", "1", callback).CombinedOutput()
				require.Error(t, err, string(output))
				require.NoDirExists(t, out, "runtime check must still fail before observation")
			}
		})
	}
}
