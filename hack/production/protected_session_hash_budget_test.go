package production_test

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

func TestProtectedSessionHashVerificationHonorsFaultDeadline(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(owner, "sha256sum"), []byte("#!/bin/bash\nprintf '%s\\n' \"$BASHPID\" > \"$stack_owner/hash-called\"\nexec /usr/bin/sleep 60\n"), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", `
set -euo pipefail
source "$1"
export stack_owner=$2
export PATH="$stack_owner:$PATH"
stack_fault_start=$(($(date -u +%s%N)-29000000000))
rc=0
stack_session_verify_inputs || rc=$?
[[ $rc == 124 && -s $stack_owner/hash-called ]]
echo HASH_DEADLINE_ENFORCED
`, "test", library, owner)
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), string(output))
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "HASH_DEADLINE_ENFORCED")
	pid, err := os.ReadFile(filepath.Join(owner, "hash-called"))
	require.NoError(t, err)
	require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run(), "hash subprocess survived deadline")
}
