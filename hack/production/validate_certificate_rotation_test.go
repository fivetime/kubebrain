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

const (
	oldCertificateFingerprintSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	newCertificateFingerprintSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestValidateCertificateRotationLifecycle(t *testing.T) {
	fixture := newRotationFixture(t)
	fixture.run(t, "begin", true, "")
	fixture.run(t, "overlap", true, "")
	output := fixture.run(t, "complete", true, "")
	require.Contains(t, output, "completion gate passed")

	data, err := os.ReadFile(filepath.Join(fixture.stateDir, "rotation-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.certificate-rotation.receipt.v1", receipt["format"])
	require.Equal(t, true, receipt["pods_unchanged"])
	require.Equal(t, true, receipt["old_certificate_rejected"])
	require.Equal(t, float64(3), receipt["replicas"])

	// Completion is retry-safe when the evidence and inputs are identical.
	fixture.run(t, "complete", true, "")
}

func TestValidateCertificateRotationRejectsNestedExistingReceiptFields(t *testing.T) {
	fixture := newRotationFixture(t)
	fixture.run(t, "begin", true, "")
	fixture.run(t, "overlap", true, "")

	receiptPath := filepath.Join(fixture.stateDir, "rotation-1.receipt.json")
	shadowReceipt := `{"format":"wrong","old_certificate_rejected":false,"shadow":{"format":"kubebrain.certificate-rotation.receipt.v1","instance":"instance-a","rotation_id":"rotation-1","endpoint":"https://instance.example:2379","replicas":3,"old_certificate_sha256":"aa11","new_certificate_sha256":"bb22","pods_unchanged":true,"old_certificate_rejected":true,"completed_at_unix":1}}` + "\n"
	require.NoError(t, os.WriteFile(receiptPath, []byte(shadowReceipt), 0o600))

	fixture.run(t, "complete", false, "", "existing receipt does not match the completed rotation")
	data, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Equal(t, shadowReceipt, string(data))
}

func TestValidateCertificateRotationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prepare    func(*testing.T, *rotationFixture)
		action     string
		env        string
		wantOutput string
	}{
		{
			name:       "overlap before begin",
			action:     "overlap",
			wantOutput: "begin evidence is missing",
		},
		{
			name:       "invalid old certificate fingerprint",
			action:     "begin",
			env:        "FAKE_SHORT_FINGERPRINT=true",
			wantOutput: "certificate SHA-256 fingerprint is invalid",
		},
		{
			name: "new credential rejected during overlap",
			prepare: func(t *testing.T, f *rotationFixture) {
				f.run(t, "begin", true, "")
			},
			action:     "overlap",
			env:        "FAKE_REJECT_NEW=true",
			wantOutput: "unhealthy",
		},
		{
			name: "non canonical state before overlap",
			prepare: func(t *testing.T, f *rotationFixture) {
				f.run(t, "begin", true, "")
				statePath := filepath.Join(f.stateDir, "rotation-1.state")
				require.NoError(t, os.WriteFile(statePath, append(mustRead(t, statePath), []byte("UNKNOWN\trow\n")...), 0o600))
			},
			action:     "overlap",
			wantOutput: "rotation state has invalid schema",
		},
		{
			name: "pod replaced before completion",
			prepare: func(t *testing.T, f *rotationFixture) {
				f.run(t, "begin", true, "")
				f.run(t, "overlap", true, "")
			},
			action:     "complete",
			env:        "FAKE_PODS=kubebrain-0\\tuid-0\\t0\\ttrue\\nkubebrain-1\\tuid-replaced\\t0\\ttrue\\nkubebrain-2\\tuid-2\\t0\\ttrue",
			wantOutput: "Pods were replaced",
		},
		{
			name: "non canonical overlap marker before completion",
			prepare: func(t *testing.T, f *rotationFixture) {
				f.run(t, "begin", true, "")
				f.run(t, "overlap", true, "")
				overlapPath := filepath.Join(f.stateDir, "rotation-1.overlap")
				require.NoError(t, os.WriteFile(overlapPath, append(mustRead(t, overlapPath), []byte("UNKNOWN\trow\n")...), 0o600))
			},
			action:     "complete",
			wantOutput: "rotation overlap marker has invalid schema",
		},
		{
			name: "old credential remains accepted",
			prepare: func(t *testing.T, f *rotationFixture) {
				f.run(t, "begin", true, "")
				f.run(t, "overlap", true, "")
			},
			action:     "complete",
			env:        "FAKE_ACCEPT_OLD_FINAL=true",
			wantOutput: "still accepted",
		},
		{
			name: "endpoint outage is not old credential rejection",
			prepare: func(t *testing.T, f *rotationFixture) {
				f.run(t, "begin", true, "")
				f.run(t, "overlap", true, "")
			},
			action:     "complete",
			env:        "FAKE_FAIL_HEALTH_AFTER_OLD=true",
			wantOutput: "refusing to treat an endpoint outage",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRotationFixture(t)
			if tc.prepare != nil {
				tc.prepare(t, fixture)
			}
			fixture.run(t, tc.action, false, tc.env, tc.wantOutput)
			_, err := os.Stat(filepath.Join(fixture.stateDir, "rotation-1.receipt.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestValidateCertificateRotationRejectsUnsafeEvidenceFields(t *testing.T) {
	fixture := newRotationFixture(t)
	fixture.env = append(fixture.env, "ENDPOINT=https://example.invalid/\"bad")
	fixture.run(t, "begin", false, "", "must not contain control characters")
}

type rotationFixture struct {
	dir      string
	stateDir string
	env      []string
}

func newRotationFixture(t *testing.T) *rotationFixture {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(stateDir, 0o700))
	for _, name := range []string{"old-ca", "old-cert", "old-key", "new-ca", "new-cert", "new-key", "overlap-ca"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600))
	}
	writeExecutable(t, filepath.Join(dir, "kubectl"), `#!/usr/bin/env bash
printf '%b\n' "${FAKE_PODS:-kubebrain-0\tuid-0\t0\ttrue\nkubebrain-1\tuid-1\t0\ttrue\nkubebrain-2\tuid-2\t0\ttrue}"
`)
	writeExecutable(t, filepath.Join(dir, "openssl"), `#!/usr/bin/env bash
file=
while (($#)); do
  if [[ "$1" == "-in" ]]; then file="$2"; shift 2; else shift; fi
done
case "$file" in
  *old-cert)
    [[ "${FAKE_SHORT_FINGERPRINT:-false}" != true ]] || { printf 'sha256 Fingerprint=AA:11\n'; exit 0; }
    printf 'sha256 Fingerprint=`+oldCertificateFingerprintSHA256+`\n'
    ;;
  *new-cert) printf 'sha256 Fingerprint=`+newCertificateFingerprintSHA256+`\n' ;;
  *) exit 1 ;;
esac
`)
	writeExecutable(t, filepath.Join(dir, "etcdctl"), `#!/usr/bin/env bash
cert=
cacert=
for arg in "$@"; do
  case "$arg" in
    --cert=*) cert="${arg#*=}" ;;
    --cacert=*) cacert="${arg#*=}" ;;
  esac
done
if [[ "$cert" == *new-cert && "${FAKE_REJECT_NEW:-false}" == true ]]; then
  echo unhealthy >&2; exit 1
fi
if [[ "$cert" == *old-cert && "$cacert" == *new-ca && "${FAKE_ACCEPT_OLD_FINAL:-false}" != true ]]; then
  if [[ "${FAKE_FAIL_HEALTH_AFTER_OLD:-false}" == true ]]; then
    touch "${FAKE_STATE_DIR}/fail-new"
  fi
  echo rejected >&2; exit 1
fi
if [[ "$cert" == *new-cert && -f "${FAKE_STATE_DIR}/fail-new" ]]; then
  echo unhealthy >&2; exit 1
fi
echo healthy
`)
	return &rotationFixture{
		dir:      dir,
		stateDir: stateDir,
		env: []string{
			"ROTATION_ID=rotation-1",
			"INSTANCE=instance-a",
			"STATE_DIR=" + stateDir,
			"ENDPOINT=https://instance.example:2379",
			"KUBECTL=" + filepath.Join(dir, "kubectl"),
			"ETCDCTL=" + filepath.Join(dir, "etcdctl"),
			"OPENSSL=" + filepath.Join(dir, "openssl"),
			"OLD_CACERT=" + filepath.Join(dir, "old-ca"),
			"OLD_CERT=" + filepath.Join(dir, "old-cert"),
			"OLD_KEY=" + filepath.Join(dir, "old-key"),
			"NEW_CACERT=" + filepath.Join(dir, "new-ca"),
			"NEW_CERT=" + filepath.Join(dir, "new-cert"),
			"NEW_KEY=" + filepath.Join(dir, "new-key"),
			"OVERLAP_CACERT=" + filepath.Join(dir, "overlap-ca"),
			"FAKE_STATE_DIR=" + stateDir,
		},
	}
}

func (f *rotationFixture) run(t *testing.T, action string, wantOK bool, extraEnv string, outputs ...string) string {
	t.Helper()
	command := exec.Command("bash", "validate-certificate-rotation.sh")
	command.Env = append(os.Environ(), append(f.env, "ACTION="+action, extraEnv)...)
	output, err := command.CombinedOutput()
	if wantOK {
		require.NoError(t, err, string(output))
	} else {
		require.Error(t, err, string(output))
	}
	for _, expected := range outputs {
		require.Contains(t, strings.TrimSpace(string(output)), expected)
	}
	return string(output)
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
}
