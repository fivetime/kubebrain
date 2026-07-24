package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionShellEntrypointsRejectInvalidNamespacesBeforeExternalCalls(t *testing.T) {
	dir := t.TempDir()
	externalLog := filepath.Join(dir, "external.log")
	fakeExternal := filepath.Join(dir, "external")
	require.NoError(t, os.WriteFile(fakeExternal, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'called %s\n' "$0 $*" >>"$EXTERNAL_LOG"
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go"), []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'called go %s\n' "$*" >>"$EXTERNAL_LOG"
`), 0o755))

	stateDir := filepath.Join(dir, "state")
	artifactPath := filepath.Join(dir, "artifact.json")
	receiptPath := filepath.Join(dir, "receipt.json")
	baseEnv := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"EXTERNAL_LOG=" + externalLog,
		"KUBECTL=" + fakeExternal,
		"ETCDCTL=" + fakeExternal,
		"OPENSSL=" + fakeExternal,
		"UID_DELETE=" + fakeExternal,
		"OPERATIONCTL=" + fakeExternal,
	}

	tests := []struct {
		name       string
		script     string
		env        []string
		wantOutput string
	}{
		{
			name:   "boundary cleanup kubebrain namespace",
			script: "cleanup-instance-boundaries.sh",
			env: []string{
				"ACTION=prepare", "CLEANUP_ID=cleanup-1", "INSTANCE=instance-a",
				"STATE_DIR=" + stateDir, "DESTROY_RECEIPT_INPUT=" + receiptPath,
				"KUBEBRAIN_NAMESPACE=instance.a", "TIDB_NAMESPACE=storage-a",
				"CREDENTIAL_NAMESPACE=control", "CREDENTIAL_SECRETS=client-tls",
			},
			wantOutput: "KUBEBRAIN_NAMESPACE must be a lowercase DNS label",
		},
		{
			name:   "destroy tidb namespace",
			script: "destroy-instance.sh",
			env: []string{
				"ACTION=prepare", "OPERATION_ID=destroy-1", "INSTANCE=instance-a",
				"STATE_DIR=" + stateDir, "KUBEBRAIN_NAMESPACE=instance-a",
				"TIDB_NAMESPACE=storage.a",
			},
			wantOutput: "TIDB_NAMESPACE must be a lowercase DNS label",
		},
		{
			name:   "traffic switch service namespace",
			script: "switch-restore-traffic.sh",
			env: []string{
				"ACTION=prepare", "OPERATION_ID=cutover-1", "INSTANCE=instance-a",
				"STATE_DIR=" + stateDir, "RESTORE_RECEIPT_INPUT=" + receiptPath,
				"SERVICE_NAMESPACE=service.ns", "SERVICE_NAME=kubebrain",
				"SOURCE_INSTANCE=source-a", "TARGET_INSTANCE=target-a",
			},
			wantOutput: "SERVICE_NAMESPACE must be a lowercase DNS label",
		},
		{
			name:   "restore audit service namespace",
			script: "audit-restored-instance.sh",
			env: []string{
				"OPERATION_ID=audit-1", "INSTANCE=instance-a", "STATE_DIR=" + stateDir,
				"CUTOVER_STATE_INPUT=" + artifactPath, "CUTOVER_RECEIPT_INPUT=" + receiptPath,
				"SERVICE_NAMESPACE=service.ns", "SERVICE_NAME=kubebrain",
				"TARGET_INSTANCE=target-a", "PUBLIC_ENDPOINT=https://example.invalid",
			},
			wantOutput: "SERVICE_NAMESPACE must be a lowercase DNS label",
		},
		{
			name:   "certificate rotation kubebrain namespace",
			script: "validate-certificate-rotation.sh",
			env: []string{
				"ACTION=begin", "ROTATION_ID=rotation-1", "INSTANCE=instance-a",
				"STATE_DIR=" + stateDir, "ENDPOINT=https://example.invalid",
				"OLD_CACERT=" + artifactPath, "OLD_CERT=" + artifactPath, "OLD_KEY=" + artifactPath,
				"NEW_CACERT=" + artifactPath, "NEW_CERT=" + artifactPath, "NEW_KEY=" + artifactPath,
				"KUBEBRAIN_NAMESPACE=kubebrain.system",
			},
			wantOutput: "KUBEBRAIN_NAMESPACE must be a lowercase DNS label",
		},
		{
			name:   "operation audit namespace",
			script: "archive-operation-audit.sh",
			env: []string{
				"OPERATION_NAMESPACE=ops.ns", "OPERATION_NAME=backup-1",
				"ARTIFACT_OUTPUT=" + artifactPath, "OBJECT_STORE_ID=store-a",
				"S3_BUCKET=audit-bucket", "S3_OBJECT_KEY=operation-audit/ops/uid-a.json",
				"RETENTION_MODE=COMPLIANCE", "RETAIN_UNTIL_UNIX=1900000000",
				"RECEIPT_OUTPUT=" + receiptPath,
			},
			wantOutput: "OPERATION_NAMESPACE must be a lowercase DNS label",
		},
	}
	for _, script := range []string{
		"run-backup-operation.sh",
		"run-backup-deletion-operation.sh",
		"run-certificate-rotation-operation.sh",
		"run-destroy-operation.sh",
		"run-restore-cutover-operation.sh",
		"run-post-restore-audit-operation.sh",
	} {
		tests = append(tests, struct {
			name       string
			script     string
			env        []string
			wantOutput string
		}{
			name:       script + " operation namespace",
			script:     script,
			env:        []string{"WORKER_ID=worker-a", "OPERATION_NAMESPACE=ops.ns"},
			wantOutput: "OPERATION_NAMESPACE must be a lowercase DNS label",
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.RemoveAll(externalLog))
			env := append([]string{}, baseEnv...)
			env = append(env, tc.env...)
			output, err := runProductionScriptCommand(t, tc.script, env)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.wantOutput)
			require.NoFileExists(t, externalLog)
		})
	}
}
