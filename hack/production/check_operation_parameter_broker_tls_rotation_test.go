package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationParameterBrokerTLSRotationCapturesAndVerifiesBothPods(t *testing.T) {
	dir := t.TempDir()
	baseline, caFile := filepath.Join(dir, "baseline.json"), filepath.Join(dir, "ca.crt")
	tokenFile, expectedCert := filepath.Join(dir, "token"), filepath.Join(dir, "new.crt")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(expectedCert, []byte("NEWCERT"), 0o600))
	checker, kubectl, openssl, timeout := writeOperationParameterBrokerTLSRotationFakes(t, dir)
	oldFingerprint, newFingerprint := strings.Repeat("a", 64), strings.Repeat("b", 64)
	baseEnv := operationParameterBrokerTLSRotationEnv(checker, kubectl, openssl, timeout, caFile, tokenFile, baseline)

	out, err := runProductionCommand(t, "bash", []string{"check-operation-parameter-broker-tls-rotation.sh", "--capture"}, append(baseEnv,
		"PRESENTED_FP="+oldFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=old"))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "captured parameter broker TLS rotation baseline")
	require.Equal(t, os.FileMode(0o600), mustStat(t, baseline).Mode().Perm())

	verifyEnv := append(append([]string{}, baseEnv...),
		"PRESENTED_FP="+newFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=old",
		"BROKER_EXPECTED_TLS_CERT_FILE="+expectedCert)
	out, err = runProductionCommand(t, "bash", []string{"check-operation-parameter-broker-tls-rotation.sh", "--verify"}, verifyEnv)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified parameter broker TLS hot rotation across unchanged Pod UIDs")
}

func TestOperationParameterBrokerTLSRotationRejectsPodRestart(t *testing.T) {
	dir := t.TempDir()
	baseline, caFile := filepath.Join(dir, "baseline.json"), filepath.Join(dir, "ca.crt")
	tokenFile, expectedCert := filepath.Join(dir, "token"), filepath.Join(dir, "new.crt")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(expectedCert, []byte("NEWCERT"), 0o600))
	checker, kubectl, openssl, timeout := writeOperationParameterBrokerTLSRotationFakes(t, dir)
	oldFingerprint, newFingerprint := strings.Repeat("a", 64), strings.Repeat("b", 64)
	baseEnv := operationParameterBrokerTLSRotationEnv(checker, kubectl, openssl, timeout, caFile, tokenFile, baseline)

	_, err := runProductionCommand(t, "bash", []string{"check-operation-parameter-broker-tls-rotation.sh", "--capture"}, append(baseEnv,
		"PRESENTED_FP="+oldFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=old"))
	require.NoError(t, err)
	out, err := runProductionCommand(t, "bash", []string{"check-operation-parameter-broker-tls-rotation.sh", "--verify"}, append(baseEnv,
		"PRESENTED_FP="+newFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=new",
		"BROKER_EXPECTED_TLS_CERT_FILE="+expectedCert))
	require.Error(t, err)
	require.Contains(t, string(out), "broker Pod identities changed during TLS rotation")
}

func TestOperationParameterBrokerTLSRotationRejectsOneStalePod(t *testing.T) {
	dir := t.TempDir()
	baseline, caFile := filepath.Join(dir, "baseline.json"), filepath.Join(dir, "ca.crt")
	tokenFile, expectedCert := filepath.Join(dir, "token"), filepath.Join(dir, "new.crt")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(expectedCert, []byte("NEWCERT"), 0o600))
	checker, kubectl, openssl, timeout := writeOperationParameterBrokerTLSRotationFakes(t, dir)
	oldFingerprint, newFingerprint, staleFingerprint := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	baseEnv := operationParameterBrokerTLSRotationEnv(checker, kubectl, openssl, timeout, caFile, tokenFile, baseline)

	_, err := runProductionCommand(t, "bash", []string{"check-operation-parameter-broker-tls-rotation.sh", "--capture"}, append(baseEnv,
		"PRESENTED_FP="+oldFingerprint, "EXPECTED_FP="+newFingerprint, "UID_SET=old"))
	require.NoError(t, err)
	out, err := runProductionCommand(t, "bash", []string{"check-operation-parameter-broker-tls-rotation.sh", "--verify"}, append(baseEnv,
		"PRESENTED_FP="+newFingerprint, "POD1_PRESENTED_FP="+staleFingerprint,
		"EXPECTED_FP="+newFingerprint, "UID_SET=old", "BROKER_EXPECTED_TLS_CERT_FILE="+expectedCert))
	require.Error(t, err)
	require.Contains(t, string(out), "not every broker Pod presents the expected rotated certificate")
}

func operationParameterBrokerTLSRotationEnv(checker, kubectl, openssl, timeout, caFile, tokenFile, baseline string) []string {
	return []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPENSSL=" + openssl, "TIMEOUT=" + timeout,
		"BROKER_CHECKER=" + checker, "BROKER_CA_FILE=" + caFile, "BROKER_TOKEN_FILE=" + tokenFile,
		"BROKER_TLS_BASELINE_FILE=" + baseline,
	}
}

func writeOperationParameterBrokerTLSRotationFakes(t *testing.T, dir string) (string, string, string, string) {
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
if [[ "${UID_SET:-old}" == old ]]; then uids=(uid-a uid-b); else uids=(uid-a uid-new); fi
if [[ "$1" == get && "$2" == pod ]]; then
  case "$3" in broker-0) uid="${uids[0]}" ;; broker-1) uid="${uids[1]}" ;; *) exit 99 ;; esac
  printf '{"metadata":{"uid":"%s"}}\n' "$uid"
  exit 0
fi
if [[ "$1" == port-forward ]]; then
  case "$*" in *pod/broker-0*) port=45681 ;; *pod/broker-1*) port=45682 ;; *) exit 99 ;; esac
  printf 'Forwarding from 127.0.0.1:%s -> 8443\n' "$port"
  while true; do sleep 1; done
fi
[[ "$1" == get && "$2" == pods ]] || exit 99
printf '{"items":['
for i in 0 1; do
  ((i == 0)) || printf ','
  printf '{"metadata":{"name":"broker-%s","uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}' "$i" "${uids[$i]}"
done
printf ']}\n'
`)
	openssl := filepath.Join(dir, "openssl")
	writeTrafficExecutable(t, openssl, `#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  s_client)
    fp="$PRESENTED_FP"
    if [[ "$*" == *"-connect 127.0.0.1:45682"* && -n "${POD1_PRESENTED_FP:-}" ]]; then fp="$POD1_PRESENTED_FP"; fi
    printf 'FAKE-CERT %s\n' "$fp"
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
