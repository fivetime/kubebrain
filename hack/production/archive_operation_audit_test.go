package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveOperationAuditPassesExpectedReceiptScopeToRelease(t *testing.T) {
	dir := t.TempDir()
	fakeGo := filepath.Join(dir, "go")
	logPath := filepath.Join(dir, "go.log")
	artifactPath := filepath.Join(dir, "artifact.json")
	receiptPath := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(fakeGo, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'PWD=%s ARGS=' "$PWD" >>"$FAKE_GO_LOG"
printf '[%s]' "$@" >>"$FAKE_GO_LOG"
printf '\n' >>"$FAKE_GO_LOG"
if [[ "$*" == *"./hack/production/cmd/operation-audit"* ]]; then
  action=capture
  output=
  receipt=
  previous=
  for arg in "$@"; do
    if [[ "$previous" == "--action" ]]; then action="$arg"; fi
    if [[ "$previous" == "--output" ]]; then output="$arg"; fi
    if [[ "$previous" == "--receipt" ]]; then receipt="$arg"; fi
    previous="$arg"
  done
  if [[ "$action" == "capture" && -n "$output" ]]; then
    printf '{"format":"kubebrain.operation-audit.v1"}\n' >"$output"
  elif [[ "$action" == "release" ]]; then
    printf 'RELEASE OUTPUT=%s RECEIPT=%s\n' "$output" "$receipt" >>"$FAKE_GO_LOG"
  fi
elif [[ "$*" == *"./cmd/logical-object"* ]]; then
  printf 'ARCHIVE INPUT=%s OBJECT_STORE_ID=%s S3_BUCKET=%s S3_OBJECT_KEY=%s RETENTION_MODE=%s RETAIN_UNTIL_UNIX=%s\n' \
    "$INPUT" "$OBJECT_STORE_ID" "$S3_BUCKET" "$S3_OBJECT_KEY" "$RETENTION_MODE" "$RETAIN_UNTIL_UNIX" >>"$FAKE_GO_LOG"
  printf '{"format":"kubebrain.object-operation-audit.receipt.v1"}\n' >"$RECEIPT_OUTPUT"
fi
`), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"FAKE_GO_LOG=" + logPath,
		"OPERATION_NAMESPACE=ops",
		"OPERATION_NAME=backup-1",
		"ARTIFACT_OUTPUT=" + artifactPath,
		"OBJECT_STORE_ID=store-a",
		"S3_BUCKET=audit-bucket",
		"S3_OBJECT_KEY=operation-audit/ops/uid-a.json",
		"RETENTION_MODE=COMPLIANCE",
		"RETAIN_UNTIL_UNIX=1900000000",
		"RECEIPT_OUTPUT=" + receiptPath,
	}
	output, err := runArchiveOperationAudit(t, env)
	require.NoError(t, err, string(output))

	log := mustReadText(t, logPath)
	require.Contains(t, log, "ARCHIVE INPUT=")
	require.NotContains(t, log, "ARCHIVE INPUT="+artifactPath+" ")
	require.Contains(t, log, "OBJECT_STORE_ID=store-a S3_BUCKET=audit-bucket S3_OBJECT_KEY=operation-audit/ops/uid-a.json RETENTION_MODE=COMPLIANCE RETAIN_UNTIL_UNIX=1900000000")
	require.Contains(t, log, "RELEASE OUTPUT=")
	require.Contains(t, log, " RECEIPT=")
	require.NotContains(t, log, "RELEASE OUTPUT="+artifactPath+" ")
	require.NotContains(t, log, " RECEIPT="+receiptPath)
	require.Contains(t, log, "[--action][release]")
	require.Contains(t, log, "[--object-store-id][store-a]")
	require.Contains(t, log, "[--bucket][audit-bucket]")
	require.Contains(t, log, "[--object-key][operation-audit/ops/uid-a.json]")
	require.Contains(t, log, "[--retention-mode][COMPLIANCE]")
	require.Contains(t, log, "[--retain-until-unix][1900000000]")
}

func TestArchiveOperationAuditRejectsInvalidRetentionBeforeCapture(t *testing.T) {
	dir := t.TempDir()
	fakeGo := filepath.Join(dir, "go")
	logPath := filepath.Join(dir, "go.log")
	require.NoError(t, os.WriteFile(fakeGo, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'called\n' >>"$FAKE_GO_LOG"
exit 99
`), 0o755))

	for _, tc := range []struct {
		name       string
		env        string
		wantOutput string
	}{
		{
			name:       "mode",
			env:        "RETENTION_MODE=mutable",
			wantOutput: "RETENTION_MODE must be COMPLIANCE or GOVERNANCE",
		},
		{
			name:       "retain until",
			env:        "RETAIN_UNTIL_UNIX=soon",
			wantOutput: "RETAIN_UNTIL_UNIX must be a positive Unix timestamp",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_ = os.Remove(logPath)
			env := []string{
				"PATH=" + dir + ":" + os.Getenv("PATH"),
				"FAKE_GO_LOG=" + logPath,
				"OPERATION_NAME=backup-1",
				"ARTIFACT_OUTPUT=" + filepath.Join(dir, "artifact-"+tc.name+".json"),
				"OBJECT_STORE_ID=store-a",
				"S3_BUCKET=audit-bucket",
				"S3_OBJECT_KEY=operation-audit/ops/uid-a.json",
				"RETENTION_MODE=COMPLIANCE",
				"RETAIN_UNTIL_UNIX=1900000000",
				"RECEIPT_OUTPUT=" + filepath.Join(dir, "receipt-"+tc.name+".json"),
				tc.env,
			}
			output, err := runArchiveOperationAudit(t, env)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.wantOutput)
			require.NoFileExists(t, logPath)
		})
	}
}

func TestArchiveOperationAuditRejectsCaptureDrift(t *testing.T) {
	for _, tc := range []struct {
		name       string
		env        string
		wantOutput string
		wantLog    string
	}{
		{
			name:       "artifact",
			env:        "TAMPER_ARTIFACT_DURING_SHA256=true",
			wantOutput: "operation audit artifact changed while being captured",
			wantLog:    "ARCHIVE ",
		},
		{
			name:       "receipt",
			env:        "TAMPER_RECEIPT_DURING_SHA256=true",
			wantOutput: "operation audit archive receipt changed while being captured",
			wantLog:    "RELEASE ",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeGo := filepath.Join(dir, "go")
			logPath := filepath.Join(dir, "go.log")
			artifactPath := filepath.Join(dir, "artifact.json")
			receiptPath := filepath.Join(dir, "receipt.json")
			require.NoError(t, os.WriteFile(fakeGo, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"./hack/production/cmd/operation-audit"* ]]; then
  action=capture
  output=
  previous=
  for arg in "$@"; do
    if [[ "$previous" == "--action" ]]; then action="$arg"; fi
    if [[ "$previous" == "--output" ]]; then output="$arg"; fi
    previous="$arg"
  done
  printf 'operation-audit %s\n' "$action" >>"$FAKE_GO_LOG"
  if [[ "$action" == "capture" && -n "$output" ]]; then
    printf '{"format":"kubebrain.operation-audit.v1"}\n' >"$output"
  fi
elif [[ "$*" == *"./cmd/logical-object"* ]]; then
  printf 'ARCHIVE INPUT=%s\n' "$INPUT" >>"$FAKE_GO_LOG"
  printf '{"format":"kubebrain.object-operation-audit.receipt.v1"}\n' >"$RECEIPT_OUTPUT"
fi
`), 0o755))
			realSHA, err := exec.LookPath("sha256sum")
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "sha256sum"), []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${TAMPER_ARTIFACT_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "$ARTIFACT_OUTPUT" &&
  ! -f "$FAKE_DIR/artifact-tampered-during-sha256" ]]; then
  "$REAL_SHA256SUM" "$@"
  printf 'changed\n' >"$ARTIFACT_OUTPUT"
  touch "$FAKE_DIR/artifact-tampered-during-sha256"
  exit 0
fi
if [[ "${TAMPER_RECEIPT_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "$RECEIPT_OUTPUT" &&
  ! -f "$FAKE_DIR/receipt-tampered-during-sha256" ]]; then
  "$REAL_SHA256SUM" "$@"
  printf 'changed\n' >"$RECEIPT_OUTPUT"
  touch "$FAKE_DIR/receipt-tampered-during-sha256"
  exit 0
fi
exec "$REAL_SHA256SUM" "$@"
`), 0o755))

			env := []string{
				"PATH=" + dir + ":" + os.Getenv("PATH"),
				"FAKE_DIR=" + dir,
				"FAKE_GO_LOG=" + logPath,
				"REAL_SHA256SUM=" + realSHA,
				"OPERATION_NAMESPACE=ops",
				"OPERATION_NAME=backup-1",
				"ARTIFACT_OUTPUT=" + artifactPath,
				"OBJECT_STORE_ID=store-a",
				"S3_BUCKET=audit-bucket",
				"S3_OBJECT_KEY=operation-audit/ops/uid-a.json",
				"RETENTION_MODE=COMPLIANCE",
				"RETAIN_UNTIL_UNIX=1900000000",
				"RECEIPT_OUTPUT=" + receiptPath,
				tc.env,
			}
			output, err := runArchiveOperationAudit(t, env)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.wantOutput)
			log := mustReadText(t, logPath)
			require.NotContains(t, log, tc.wantLog)
		})
	}
}

func runArchiveOperationAudit(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "archive-operation-audit.sh", env)
}

func mustReadText(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}
