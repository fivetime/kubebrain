package production_test

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

func TestWaitPolicyAbsenceBoundedTypedRetries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		wantCode int
		calls    int
	}{
		{"success", 0, 1}, {"pending-then-success", 0, 3},
		{"identity-drift", 65, 1}, {"unknown-error", 1, 1},
		{"fatal-after-pending", 65, 2}, {"observer-timeout", 124, 1},
		{"blocked", 124, 1}, {"always-pending", 124, -1},
		{"tampered", 65, 1}, {"wrong-hash", 65, 0},
		{"expired", 124, 0}, {"oversized-budget", 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			callback := filepath.Join(dir, "observer.sh")
			require.NoError(t, os.WriteFile(callback, []byte(policyAbsenceCallback), 0600))
			hash := sha256.Sum256([]byte(policyAbsenceCallback))
			digest := hex.EncodeToString(hash[:])
			if tc.name == "wrong-hash" {
				digest = strings.Repeat("0", 64)
			}
			budget := 3 * time.Second
			switch tc.name {
			case "blocked", "always-pending":
				budget = 900 * time.Millisecond
			case "expired":
				budget = -time.Second
			case "oversized-budget":
				budget = 70 * time.Second
			}
			deadline := time.Now().Add(budget).UnixNano()
			out := filepath.Join(dir, "observations")
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "wait-policy-absence.sh", out, strconv.FormatInt(deadline, 10), callback, digest)
			cmd.Env = append(os.Environ(), "OBSERVATION_DIR="+dir, "OBSERVATION_SCENARIO="+tc.name)
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(output))
			if tc.wantCode == 0 {
				require.NoError(t, err, string(output))
				stamp, err := os.ReadFile(filepath.Join(out, "verified.ns"))
				require.NoError(t, err)
				verified, err := strconv.ParseInt(strings.TrimSpace(string(stamp)), 10, 64)
				require.NoError(t, err)
				require.Less(t, verified, deadline)
				match, err := os.ReadFile(filepath.Join(out, "matched-sample"))
				require.NoError(t, err)
				require.Equal(t, filepath.Join(out, "sample-"+strconv.Itoa(tc.calls-1)), strings.TrimSpace(string(match)))
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				require.Equal(t, tc.wantCode, exit.ExitCode(), string(output))
				_, err := os.Stat(filepath.Join(out, "matched-sample"))
				require.True(t, os.IsNotExist(err))
			}
			calls, readErr := os.ReadFile(filepath.Join(dir, "calls"))
			if tc.calls == 0 {
				require.True(t, os.IsNotExist(readErr))
			} else {
				require.NoError(t, readErr)
				actual := strings.Count(string(calls), "absent\n")
				if tc.calls > 0 {
					require.Equal(t, tc.calls, actual)
				} else {
					require.Greater(t, actual, 0)
					require.LessOrEqual(t, actual, 4)
				}
			}
			if tc.name == "blocked" {
				pid, err := os.ReadFile(filepath.Join(dir, "pid"))
				require.NoError(t, err)
				require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run())
			}
		})
	}
}

const policyAbsenceCallback = `#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && $1 == absent ]] || exit 2
printf 'absent\n' >> "$OBSERVATION_DIR/calls"
calls=$(wc -l < "$OBSERVATION_DIR/calls")
case $OBSERVATION_SCENARIO in
 success) exit 0;;
 pending-then-success) if (( calls < 3 )); then exit 75; fi;;
 identity-drift) exit 65;;
 unknown-error) exit 1;;
 fatal-after-pending) if (( calls == 1 )); then exit 75; else exit 65; fi;;
 observer-timeout) exit 124;;
 blocked) printf '%s\n' "$BASHPID" > "$OBSERVATION_DIR/pid"; exec sleep 60;;
 always-pending) exit 75;;
 tampered) printf '# changed\n' >> "$0";;
 *) exit 99;;
esac
`

func TestWaitPolicyAbsenceOuterCancellation(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	callback := filepath.Join(dir, "observer.sh")
	require.NoError(t, os.WriteFile(callback, []byte(policyAbsenceCallback), 0600))
	hash := sha256.Sum256([]byte(policyAbsenceCallback))
	out := filepath.Join(dir, "observations")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "timeout", "--kill-after=1s", "1s", "bash", "wait-policy-absence.sh", out,
		strconv.FormatInt(time.Now().Add(20*time.Second).UnixNano(), 10), callback, hex.EncodeToString(hash[:]))
	cmd.Env = append(os.Environ(), "OBSERVATION_DIR="+dir, "OBSERVATION_SCENARIO=blocked")
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), string(output))
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	require.Equal(t, 124, exit.ExitCode(), string(output))
	pid, err := os.ReadFile(filepath.Join(dir, "pid"))
	require.NoError(t, err)
	require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run())
	_, err = os.Stat(filepath.Join(out, "matched-sample"))
	require.True(t, os.IsNotExist(err))
	code, err := os.ReadFile(filepath.Join(out, "exit-code"))
	require.NoError(t, err)
	require.Equal(t, "143", strings.TrimSpace(string(code)))
}
