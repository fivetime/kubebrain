package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestNativePITRFullBackupIsCanonicalAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameters, []byte(`{
  "storage_prefix":"s3://immutable/instance/run/full",
  "pd_addrs":["pd-1:2379","pd-0:2379"],
  "cipher_method":"aes256-ctr",
  "encryption_key_id":"kms/prod/backup/versions/7",
  "backup_ts":"468294813545660418"
}`), 0o600))
	kubectlLog := filepath.Join(dir, "kubectl.log")
	operationLog := filepath.Join(dir, "operation.log")
	secretData := filepath.Join(dir, "secret.data")
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" get secret "* ]]; then
  [[ -f "$SECRET_DATA" ]] || exit 1
  printf 'true\tOpaque\t%s' "$(base64 -w0 "$SECRET_DATA")"
elif [[ "$*" == *" create secret generic "* ]]; then
  for arg in "$@"; do case "$arg" in --from-file=parameters.json=*) cp "${arg#--from-file=parameters.json=}" "$SECRET_DATA";; esac; done
  printf '{"apiVersion":"v1","data":{"parameters.json":"%s"},"kind":"Secret","metadata":{"name":"test"},"type":"Opaque"}\n' "$(base64 -w0 "$SECRET_DATA")"
elif [[ "$*" == *" create -f -"* ]]; then
  payload="$(cat)"
  [[ "$(jq -r .immutable <<<"$payload")" == true ]]
fi
`), 0o755))
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ -n "${FAIL_ONCE_FILE:-}" && ! -f "$FAIL_ONCE_FILE" ]]; then touch "$FAIL_ONCE_FILE"; exit 1; fi
`), 0o755))
	failOnce := filepath.Join(dir, "operation.failed")
	env := []string{
		"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl,
		"OPERATIONCTL=" + operationctl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATION_LOG=" + operationLog, "SECRET_DATA=" + secretData, "FAIL_ONCE_FILE=" + failOnce,
	}
	output, err := runProductionScriptCommand(t, "request-native-pitr-full-backup.sh", env)
	require.Error(t, err, string(output), "the first queue submission simulates a Secret/operation partial failure")
	canonical := []byte(`{"backup_ts":"468294813545660418","cipher_method":"aes256-ctr","encryption_key_id":"kms/prod/backup/versions/7","pd_addrs":["pd-0:2379","pd-1:2379"],"storage_prefix":"s3://immutable/instance/run/full"}` + "\n")
	require.Equal(t, canonical, requireReadFile(t, secretData))
	digest := fmt.Sprintf("%x", sha256.Sum256(canonical))
	operationName := "native-pitr-full-" + digest[:20]
	operation := string(requireReadFile(t, operationLog))
	for _, expected := range []string{
		"--action submit", "--name " + operationName, "--operation-id " + operationName,
		"--type NativePITRFullBackup", "--requested-by platform:native-pitr-full-backup",
		"--parameters-sha256 " + digest, "--parameters-secret " + operationName + "-parameters",
		"--max-attempts 2",
	} {
		require.Contains(t, operation, expected)
	}

	// A retry recovers the orphan immutable Secret without trying to recreate it.
	require.NoError(t, os.WriteFile(kubectlLog, nil, 0o600))
	recovered, recoveredErr := runProductionScriptCommand(t, "request-native-pitr-full-backup.sh", env)
	require.NoError(t, recoveredErr, string(recovered))
	require.NotContains(t, string(requireReadFile(t, kubectlLog)), "create secret generic")

	// Whitespace, object-key order, and PD endpoint order do not change request identity.
	reordered := `{"encryption_key_id":"kms/prod/backup/versions/7","backup_ts":"468294813545660418","pd_addrs":["pd-0:2379","pd-1:2379"],"cipher_method":"aes256-ctr","storage_prefix":"s3://immutable/instance/run/full"}`
	require.NoError(t, os.WriteFile(parameters, []byte(reordered), 0o600))
	require.NoError(t, os.WriteFile(kubectlLog, nil, 0o600))
	second, secondErr := runProductionScriptCommand(t, "request-native-pitr-full-backup.sh", env)
	require.NoError(t, secondErr, string(second))
	require.NotContains(t, string(requireReadFile(t, kubectlLog)), "create secret generic",
		"an identical retry must reuse the immutable Secret")
}

func TestRequestNativePITRFullBackupRejectsDriftAndUnsafeParameters(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	valid := `{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`
	require.NoError(t, os.WriteFile(parameters, []byte(valid), 0o600))
	secretData := filepath.Join(dir, "secret.data")
	require.NoError(t, os.WriteFile(secretData, []byte("drifted\n"), 0o600))
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
if [[ "$*" == *" get secret "* ]]; then printf 'true\tOpaque\t%s' "$(base64 -w0 "$SECRET_DATA")"; fi
`), 0o755))
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env sh\nprintf called >\"$OPERATION_LOG\"\n"), 0o755))
	base := []string{
		"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl,
		"OPERATIONCTL=" + operationctl, "SECRET_DATA=" + secretData, "OPERATION_LOG=" + operationLog,
	}
	output, err := runProductionScriptCommand(t, "request-native-pitr-full-backup.sh", base)
	require.Error(t, err)
	require.Contains(t, string(output), "parameter Secret drifted")
	require.NoFileExists(t, operationLog)

	for _, unsafe := range []string{
		`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://user@bucket/full"}`,
		`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full","extra":true}`,
		`{"backup_ts":"0","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`,
		`{"backup_ts":"18446744073709551616","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`,
		`{"backup_ts":"1","pd_addrs":["pd:2379","pd:2379"],"storage_prefix":"s3://bucket/full"}`,
		`{"backup_ts":"1","pd_addrs":["pd:65536"],"storage_prefix":"s3://bucket/full"}`,
		`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3:///full"}`,
		`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/"}`,
		`{"backup_ts":"1","cipher_method":"aes128-ctr","encryption_key_id":"key-7","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`,
		`{"backup_ts":"1","cipher_method":"aes256-ctr","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`,
	} {
		require.NoError(t, os.WriteFile(parameters, []byte(unsafe), 0o600))
		unsafeOutput, unsafeErr := runProductionScriptCommand(t, "request-native-pitr-full-backup.sh", base)
		require.Error(t, unsafeErr)
		require.Contains(t, string(unsafeOutput), "parameter schema is invalid")
	}

	// '@' and a trailing slash are valid inside an object prefix; only URL userinfo is forbidden.
	require.NoError(t, os.WriteFile(parameters, []byte(`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/team@prod/full/"}`), 0o600))
	require.NoError(t, os.WriteFile(secretData, []byte("drifted again\n"), 0o600))
	validOutput, validErr := runProductionScriptCommand(t, "request-native-pitr-full-backup.sh", base)
	require.Error(t, validErr)
	require.Contains(t, string(validOutput), "parameter Secret drifted", "the requester must reach Secret validation for a producer-valid prefix")
}

func requireReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
