package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func receiptDigestTamperEnv(t *testing.T, dir, receiptPath string) []string {
	t.Helper()
	realSHA, err := exec.LookPath("sha256sum")
	require.NoError(t, err)
	tamperedReceipt := filepath.Join(dir, "tampered-receipt.json")
	require.NoError(t, os.WriteFile(tamperedReceipt, []byte(`{"format":"tampered"}`+"\n"), 0o600))
	writeTrafficExecutable(t, filepath.Join(dir, "sha256sum"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "${TAMPER_BACKUP_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "${RUNNER_BACKUP_INPUT:-}" &&
  ! -f "$FAKE_DIR/backup-tampered-during-sha256" ]]; then
  "$REAL_SHA256SUM" "$@"
  printf 'changed\n' >"$RUNNER_BACKUP_INPUT"
  touch "$FAKE_DIR/backup-tampered-during-sha256"
  exit 0
fi
if [[ "${TAMPER_CREDENTIAL_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "${RUNNER_CREDENTIAL_INPUT:-}" &&
  ! -f "$FAKE_DIR/credential-tampered-during-sha256" ]]; then
  "$REAL_SHA256SUM" "$@"
  printf 'changed\n' >"$RUNNER_CREDENTIAL_INPUT"
  touch "$FAKE_DIR/credential-tampered-during-sha256"
  exit 0
fi
if [[ "${TAMPER_RECEIPT_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "$RUNNER_RECEIPT_OUTPUT" &&
  ! -f "$FAKE_DIR/receipt-tampered-during-sha256" ]]; then
  cp "$TAMPERED_RECEIPT_SOURCE" "$RUNNER_RECEIPT_OUTPUT"
  chmod 600 "$RUNNER_RECEIPT_OUTPUT"
  touch "$FAKE_DIR/receipt-tampered-during-sha256"
fi
exec "$REAL_SHA256SUM" "$@"
`)
	return []string{
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"REAL_SHA256SUM=" + realSHA,
		"RUNNER_RECEIPT_OUTPUT=" + receiptPath,
		"TAMPERED_RECEIPT_SOURCE=" + tamperedReceipt,
	}
}
