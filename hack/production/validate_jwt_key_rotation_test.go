package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateJWTKeyRotationLifecycle(t *testing.T) {
	f := newJWTRotationFixture(t)
	f.run(t, "phase-a", true, "FAKE_REVISION=rev-a", "FAKE_NOW=100")
	f.run(t, "phase-a", true, "FAKE_REVISION=rev-a", "FAKE_NOW=150")
	f.run(t, "phase-b", true, "FAKE_REVISION=rev-b", "FAKE_NOW=200")
	f.run(t, "phase-b", true, "FAKE_REVISION=rev-b", "FAKE_NOW=220")
	f.run(t, "phase-c", false, "FAKE_REVISION=rev-c", "FAKE_NOW=269", "phase C is too early")
	f.run(t, "phase-c", true, "FAKE_REVISION=rev-c", "FAKE_NOW=270", "FAKE_REJECT_OLD=true")
	f.run(t, "phase-c", true, "FAKE_REVISION=rev-c", "FAKE_NOW=300", "FAKE_REJECT_OLD=true")

	data, err := os.ReadFile(filepath.Join(f.stateDir, "rotation-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.jwt-key-rotation.receipt.v1", receipt["format"])
	require.Equal(t, true, receipt["old_token_rejected"])
	require.Equal(t, true, receipt["new_token_accepted"])
	require.NotEmpty(t, receipt["phase_a_sha256"])
	require.NotEmpty(t, receipt["phase_b_sha256"])
	info, err := os.Stat(filepath.Join(f.stateDir, "rotation-1.phase-b.json"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestValidateJWTKeyRotationRejectsSkippedOrDriftedStages(t *testing.T) {
	t.Run("skip phase A", func(t *testing.T) {
		f := newJWTRotationFixture(t)
		f.run(t, "phase-b", false, "FAKE_REVISION=rev-b", "phase-a.v1 evidence is missing")
	})
	t.Run("same rollout revision", func(t *testing.T) {
		f := newJWTRotationFixture(t)
		f.run(t, "phase-a", true, "FAKE_REVISION=rev-a")
		f.run(t, "phase-b", false, "FAKE_REVISION=rev-a", "phase B must use a new StatefulSet revision")
	})
	t.Run("secret changed", func(t *testing.T) {
		f := newJWTRotationFixture(t)
		f.run(t, "phase-a", true, "FAKE_REVISION=rev-a")
		f.run(t, "phase-b", false, "FAKE_REVISION=rev-b", "FAKE_SECRET_RV=12", "rotation object binding changed")
	})
	t.Run("timing changed", func(t *testing.T) {
		f := newJWTRotationFixture(t)
		f.run(t, "phase-a", true, "FAKE_REVISION=rev-a")
		f.run(t, "phase-b", true, "FAKE_REVISION=rev-b")
		f.run(t, "phase-c", false, "FAKE_REVISION=rev-c", "JWT_TTL_SECONDS=61", "retirement timing inputs changed")
	})
}

func TestValidateJWTKeyRotationRejectsPartialRetirementAndOutage(t *testing.T) {
	for _, tc := range []struct{ name, setting, want string }{
		{"old accepted", "FAKE_REJECT_OLD_ENDPOINTS=https://member-0:2379,https://member-1:2379", "old token is still accepted by https://member-2:2379"},
		{"endpoint outage", "FAKE_REJECT_OLD=true", "new token failed after old-token rejection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newJWTRotationFixture(t)
			f.run(t, "phase-a", true, "FAKE_REVISION=rev-a", "FAKE_NOW=100")
			f.run(t, "phase-b", true, "FAKE_REVISION=rev-b", "FAKE_NOW=200")
			extra := []string{"FAKE_REVISION=rev-c", "FAKE_NOW=270", tc.setting}
			if tc.name == "endpoint outage" {
				extra = append(extra, "FAKE_FAIL_NEW_AFTER_OLD=true")
			}
			extra = append(extra, tc.want)
			f.run(t, "phase-c", false, extra...)
			require.NoFileExists(t, filepath.Join(f.stateDir, "rotation-1.receipt.json"))
		})
	}
}

func TestValidateJWTKeyRotationRejectsUnsafeTokenBeforeKubernetes(t *testing.T) {
	f := newJWTRotationFixture(t)
	require.NoError(t, os.Chmod(f.oldToken, 0o644))
	f.run(t, "phase-a", false, "old-token evidence must be inaccessible to group/other")
	require.NoFileExists(t, filepath.Join(f.stateDir, "rotation-1.phase-a.json"))
}

type jwtRotationFixture struct {
	dir, stateDir, oldToken string
	env                     []string
}

func newJWTRotationFixture(t *testing.T) *jwtRotationFixture {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(stateDir, 0o700))
	oldToken, newToken := filepath.Join(dir, "old.jwt"), filepath.Join(dir, "new.jwt")
	require.NoError(t, os.WriteFile(oldToken, []byte("old-token"), 0o600))
	require.NoError(t, os.WriteFile(newToken, []byte("new-token"), 0o600))
	writeExecutable(t, filepath.Join(dir, "date"), `#!/usr/bin/env bash
printf '%s\n' "${FAKE_NOW:-200}"
`)
	writeExecutable(t, filepath.Join(dir, "kubectl"), `#!/usr/bin/env bash
if [[ "$*" == *"get statefulset"* ]]; then
  printf '{"metadata":{"uid":"sts-uid","generation":2},"spec":{"replicas":3},"status":{"observedGeneration":2,"readyReplicas":3,"updatedReplicas":3,"currentRevision":"%s","updateRevision":"%s"}}\n' "${FAKE_REVISION:-rev-a}" "${FAKE_REVISION:-rev-a}"
else
  printf '{"metadata":{"uid":"secret-uid","resourceVersion":"%s"},"immutable":true,"data":{"old-key":"b2xk","new-key":"bmV3"}}\n' "${FAKE_SECRET_RV:-11}"
fi
`)
	writeExecutable(t, filepath.Join(dir, "probe"), `#!/usr/bin/env bash
endpoint= token=
while (($#)); do case "$1" in --endpoint) endpoint="$2"; shift 2;; --token-file) token="$(<"$2")"; shift 2;; --probe-key|--timeout|--cacert|--cert|--key|--server-name) shift 2;; *) exit 2;; esac; done
if [[ "$token" == old-token ]]; then
  if [[ "${FAKE_REJECT_OLD:-false}" == true || ",${FAKE_REJECT_OLD_ENDPOINTS:-}," == *",${endpoint},"* ]]; then
    [[ "${FAKE_FAIL_NEW_AFTER_OLD:-false}" != true ]] || touch "$FAKE_STATE_DIR/outage"
    exit 1
  fi
fi
[[ ! -f "$FAKE_STATE_DIR/outage" ]]
`)
	return &jwtRotationFixture{dir: dir, stateDir: stateDir, oldToken: oldToken, env: []string{
		"ROTATION_ID=rotation-1", "INSTANCE=instance-a", "STATE_DIR=" + stateDir,
		"KEY_SECRET=jwt-keys", "ENDPOINTS=https://member-0:2379,https://member-1:2379,https://member-2:2379",
		"OLD_TOKEN_FILE=" + oldToken, "NEW_TOKEN_FILE=" + newToken,
		"JWT_TTL_SECONDS=60", "MAX_CLOCK_SKEW_SECONDS=10", "EXPECTED_REPLICAS=3",
		"KUBECTL=" + filepath.Join(dir, "kubectl"), "TOKEN_PROBE=" + filepath.Join(dir, "probe"),
		"DATE=" + filepath.Join(dir, "date"), "FAKE_STATE_DIR=" + stateDir,
	}}
}

func (f *jwtRotationFixture) run(t *testing.T, action string, success bool, settings ...string) string {
	t.Helper()
	want := ""
	if !success && len(settings) > 0 && !strings.Contains(settings[len(settings)-1], "=") {
		want, settings = settings[len(settings)-1], settings[:len(settings)-1]
	}
	cmd := exec.Command("bash", filepath.Join(productionRepositoryRoot(t), "hack/production/validate-jwt-key-rotation.sh"))
	cmd.Env = append(os.Environ(), append(f.env, append([]string{"ACTION=" + action}, settings...)...)...)
	out, err := cmd.CombinedOutput()
	if success {
		require.NoError(t, err, "%s", out)
	} else {
		require.Error(t, err, "%s", out)
		require.Contains(t, string(out), want)
	}
	return string(out)
}
