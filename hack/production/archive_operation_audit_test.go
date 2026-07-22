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
  previous=
  for arg in "$@"; do
    if [[ "$previous" == "--action" ]]; then action="$arg"; fi
    if [[ "$previous" == "--output" ]]; then output="$arg"; fi
    previous="$arg"
  done
  if [[ "$action" == "capture" && -n "$output" ]]; then
    printf '{"format":"kubebrain.operation-audit.v1"}\n' >"$output"
  fi
elif [[ "$*" == *"./cmd/logical-object"* ]]; then
  printf 'ARCHIVE OBJECT_STORE_ID=%s S3_BUCKET=%s S3_OBJECT_KEY=%s RETENTION_MODE=%s RETAIN_UNTIL_UNIX=%s\n' \
    "$OBJECT_STORE_ID" "$S3_BUCKET" "$S3_OBJECT_KEY" "$RETENTION_MODE" "$RETAIN_UNTIL_UNIX" >>"$FAKE_GO_LOG"
  printf '{"format":"kubebrain.object-operation-audit.receipt.v1"}\n' >"$RECEIPT_OUTPUT"
fi
`), 0o755))

	command := exec.Command("bash", "archive-operation-audit.sh")
	command.Env = append(os.Environ(),
		"PATH="+dir+":"+os.Getenv("PATH"),
		"FAKE_GO_LOG="+logPath,
		"OPERATION_NAMESPACE=ops",
		"OPERATION_NAME=backup-1",
		"ARTIFACT_OUTPUT="+artifactPath,
		"OBJECT_STORE_ID=store-a",
		"S3_BUCKET=audit-bucket",
		"S3_OBJECT_KEY=operation-audit/ops/uid-a.json",
		"RETENTION_MODE=COMPLIANCE",
		"RETAIN_UNTIL_UNIX=1900000000",
		"RECEIPT_OUTPUT="+receiptPath,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	log := mustReadText(t, logPath)
	require.Contains(t, log, "ARCHIVE OBJECT_STORE_ID=store-a S3_BUCKET=audit-bucket S3_OBJECT_KEY=operation-audit/ops/uid-a.json RETENTION_MODE=COMPLIANCE RETAIN_UNTIL_UNIX=1900000000")
	require.Contains(t, log, "[--action][release]")
	require.Contains(t, log, "[--receipt]["+receiptPath+"]")
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
			command := exec.Command("bash", "archive-operation-audit.sh")
			command.Env = append(os.Environ(),
				"PATH="+dir+":"+os.Getenv("PATH"),
				"FAKE_GO_LOG="+logPath,
				"OPERATION_NAME=backup-1",
				"ARTIFACT_OUTPUT="+filepath.Join(dir, "artifact-"+tc.name+".json"),
				"OBJECT_STORE_ID=store-a",
				"S3_BUCKET=audit-bucket",
				"S3_OBJECT_KEY=operation-audit/ops/uid-a.json",
				"RETENTION_MODE=COMPLIANCE",
				"RETAIN_UNTIL_UNIX=1900000000",
				"RECEIPT_OUTPUT="+filepath.Join(dir, "receipt-"+tc.name+".json"),
				tc.env,
			)
			output, err := command.CombinedOutput()
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.wantOutput)
			require.NoFileExists(t, logPath)
		})
	}
}

func mustReadText(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}
