package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationAPITLSRotationCapturesAndVerifiesUIDBoundFingerprint(t *testing.T) {
	dir := t.TempDir()
	baseline := filepath.Join(dir, "baseline.json")
	caFile, tokenFile, expectedCert := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token"), filepath.Join(dir, "new.crt")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(expectedCert, []byte("NEWCERT"), 0o600))
	checker, kubectl, openssl, timeout := writeOperationAPITLSRotationFakes(t, dir)
	oldFingerprint, newFingerprint := strings.Repeat("a", 64), strings.Repeat("b", 64)
	baseEnv := []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPENSSL=" + openssl, "TIMEOUT=" + timeout,
		"OPERATION_API_CHECKER=" + checker, "OPERATION_API_ENDPOINT=https://operation-api.example.test",
		"OPERATION_API_CA_FILE=" + caFile, "OPERATION_API_TOKEN_FILE=" + tokenFile,
		"OPERATION_API_TLS_BASELINE_FILE=" + baseline,
	}
	out, err := runProductionCommand(t, "bash", []string{"check-operation-api-tls-rotation.sh", "--capture"}, append(baseEnv,
		"PRESENTED_FP="+oldFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=old"))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "captured Operation API TLS rotation baseline")
	require.Equal(t, os.FileMode(0o600), mustStat(t, baseline).Mode().Perm())

	verifyEnv := append(append([]string{}, baseEnv...),
		"PRESENTED_FP="+newFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=old",
		"OPERATION_API_EXPECTED_TLS_CERT_FILE="+expectedCert)
	out, err = runProductionCommand(t, "bash", []string{"check-operation-api-tls-rotation.sh", "--verify"}, verifyEnv)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified Operation API TLS hot rotation across unchanged Pod UIDs")
}

func TestOperationAPITLSRotationRejectsPodRestart(t *testing.T) {
	dir := t.TempDir()
	baseline := filepath.Join(dir, "baseline.json")
	caFile, tokenFile, expectedCert := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token"), filepath.Join(dir, "new.crt")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(expectedCert, []byte("NEWCERT"), 0o600))
	checker, kubectl, openssl, timeout := writeOperationAPITLSRotationFakes(t, dir)
	oldFingerprint, newFingerprint := strings.Repeat("a", 64), strings.Repeat("b", 64)
	baseEnv := []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPENSSL=" + openssl, "TIMEOUT=" + timeout,
		"OPERATION_API_CHECKER=" + checker, "OPERATION_API_ENDPOINT=https://operation-api.example.test",
		"OPERATION_API_CA_FILE=" + caFile, "OPERATION_API_TOKEN_FILE=" + tokenFile,
		"OPERATION_API_TLS_BASELINE_FILE=" + baseline, "EXPECTED_FP=" + newFingerprint,
	}
	_, err := runProductionCommand(t, "bash", []string{"check-operation-api-tls-rotation.sh", "--capture"}, append(baseEnv,
		"PRESENTED_FP="+oldFingerprint, "UID_SET=old"))
	require.NoError(t, err)
	out, err := runProductionCommand(t, "bash", []string{"check-operation-api-tls-rotation.sh", "--verify"}, append(baseEnv,
		"PRESENTED_FP="+newFingerprint, "UID_SET=new", "OPERATION_API_EXPECTED_TLS_CERT_FILE="+expectedCert))
	require.Error(t, err)
	require.Contains(t, string(out), "Pod UIDs changed during TLS rotation")
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info
}

func writeOperationAPITLSRotationFakes(t *testing.T, dir string) (string, string, string, string) {
	t.Helper()
	checker := filepath.Join(dir, "checker")
	writeTrafficExecutable(t, checker, "#!/usr/bin/env bash\n[[ \"$1\" == --check-enabled ]]\n")
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --context ]]; then shift 2; fi
if [[ "$2" == namespace ]]; then
  printf '{"metadata":{"uid":"namespace-uid"}}\n'
  exit 0
fi
[[ "$1" == get && "$2" == pods ]] || exit 99
if [[ "${UID_SET:-old}" == old ]]; then
  uids=(uid-a uid-b uid-c)
else
  uids=(uid-a uid-b uid-new)
fi
printf '{"items":['
for i in 0 1 2; do
  ((i == 0)) || printf ','
  printf '{"metadata":{"uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}' "${uids[$i]}"
done
printf ']}\n'
`)
	openssl := filepath.Join(dir, "openssl")
	writeTrafficExecutable(t, openssl, `#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  s_client)
    printf 'FAKE-CERT %s\n' "$PRESENTED_FP"
    ;;
  x509)
    file=""
    while [[ "$#" -gt 0 ]]; do
      if [[ "$1" == -in ]]; then file="$2"; break; fi
      shift
    done
    if grep -Fq NEWCERT "$file"; then fp="$EXPECTED_FP"; else fp="$(awk '/FAKE-CERT/{print $2; exit}' "$file")"; fi
    printf 'sha256 Fingerprint=%s\n' "$fp"
    ;;
  *) exit 99 ;;
esac
`)
	timeout := filepath.Join(dir, "timeout")
	writeTrafficExecutable(t, timeout, "#!/usr/bin/env bash\nshift\nexec \"$@\"\n")
	return checker, kubectl, openssl, timeout
}
