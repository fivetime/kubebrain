package production_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateInfoCertificateRotation(t *testing.T) {
	dir := t.TempDir()
	for name, value := range map[string]string{
		"old-ca": "old-ca", "new-ca": "new-ca", "old-cert": "old-cert", "new-cert": "new-cert",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600))
	}
	fakeOpenSSL := filepath.Join(dir, "openssl")
	writeExecutable(t, fakeOpenSSL, `#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  x509)
    shift
    input=""
    while (($#)); do
      if [[ "$1" == -in ]]; then input="$2"; shift 2; else shift; fi
    done
    if [[ -n "$input" ]]; then cat "$input"; else cat; fi
    ;;
  s_client)
    [[ " $* " == *" -servername info.example "* ]] || exit 1
    ca=""
    while (($#)); do
      if [[ "$1" == -CAfile ]]; then ca="$2"; shift 2; else shift; fi
    done
    if [[ "${FAKE_REJECT_OLD_CA:-false}" == true && "$(cat "$ca")" == old-ca ]]; then exit 1; fi
    cat "$FAKE_PRESENTED_CERT"
    ;;
  *) exit 1 ;;
esac
`)
	fakeKubectl := filepath.Join(dir, "kubectl")
	writeExecutable(t, fakeKubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubebrain-0\tuid-0\t0\ttrue\nkubebrain-1\tuid-1\t0\ttrue\nkubebrain-2\tuid-2\t0\ttrue\n'
`)
	stateDir := filepath.Join(dir, "state")
	receipt := filepath.Join(dir, "receipt.json")
	common := []string{
		"ROTATION_ID=rotation-1", "INSTANCE=instance-a", "STATE_DIR=" + stateDir,
		"RECEIPT_OUTPUT=" + receipt, "INFO_ENDPOINT=https://info.example:8080",
		"INFO_SERVER_NAME=info.example", "OLD_INFO_CACERT=" + filepath.Join(dir, "old-ca"),
		"OLD_INFO_CERT=" + filepath.Join(dir, "old-cert"), "NEW_INFO_CACERT=" + filepath.Join(dir, "new-ca"),
		"NEW_INFO_CERT=" + filepath.Join(dir, "new-cert"), "OPENSSL=" + fakeOpenSSL, "KUBECTL=" + fakeKubectl,
	}
	require.NoError(t, os.Truncate(filepath.Join(dir, "old-ca"), 1048577))
	out, err := runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(common,
		"ACTION=begin", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "old-cert")))
	require.Error(t, err)
	require.Contains(t, string(out), "OLD_INFO_CACERT must contain 1..1048576 bytes")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "old-ca"), []byte("old-ca"), 0o600))
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(common,
		"ACTION=begin", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "old-cert")))
	require.NoError(t, err, out)
	require.Contains(t, string(out), "begin gate passed")
	statePath := filepath.Join(stateDir, "rotation-1.info.state")
	stateData := mustRead(t, statePath)
	require.NoError(t, os.Truncate(statePath, 2097153))
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(common,
		"ACTION=complete", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "new-cert")))
	require.Error(t, err)
	require.Contains(t, string(out), "info rotation state must contain 1..2097152 bytes")
	require.NoError(t, os.WriteFile(statePath, stateData, 0o600))
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(common,
		"ACTION=complete", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "new-cert"),
		"REQUIRE_OLD_CA_REJECTION=true", "FAKE_REJECT_OLD_CA=true"))
	require.NoError(t, err, out)
	require.Contains(t, string(out), "completion gate passed")

	data, err := os.ReadFile(receipt)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(receipt, 1048577))
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(common,
		"ACTION=verify", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "new-cert"),
		"REQUIRE_OLD_CA_REJECTION=true", "FAKE_REJECT_OLD_CA=true"))
	require.Error(t, err)
	require.Contains(t, string(out), "info rotation receipt must contain 1..1048576 bytes")
	require.NoError(t, os.WriteFile(receipt, data, 0o600))
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, "kubebrain.info-certificate-rotation.receipt.v1", got["format"])
	require.Equal(t, true, got["pods_unchanged"])
	require.Equal(t, true, got["old_ca_rejection_required"])
	require.Equal(t, true, got["old_ca_rejected"])
	require.NotEqual(t, got["old_certificate_sha256"], got["new_certificate_sha256"])
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(common,
		"ACTION=verify", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "new-cert"),
		"REQUIRE_OLD_CA_REJECTION=true", "FAKE_REJECT_OLD_CA=true"))
	require.NoError(t, err, out)
	require.Contains(t, string(out), "receipt verification passed")

	rejectionCommon := append([]string{}, common...)
	rejectionCommon = append(rejectionCommon,
		"ROTATION_ID=rotation-rejection", "RECEIPT_OUTPUT="+filepath.Join(dir, "rejection-receipt.json"))
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(rejectionCommon,
		"ACTION=begin", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "old-cert")))
	require.NoError(t, err, string(out))
	out, err = runProductionScriptCommand(t, "validate-info-certificate-rotation.sh", append(rejectionCommon,
		"ACTION=complete", "FAKE_PRESENTED_CERT="+filepath.Join(dir, "new-cert"),
		"REQUIRE_OLD_CA_REJECTION=true", "FAKE_REJECT_OLD_CA=false"))
	require.Error(t, err)
	require.Contains(t, string(out), "old info CA still verifies the endpoint after cutover")
}
