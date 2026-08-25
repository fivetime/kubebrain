package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJWTKeyRotationOperationCompletesAndTakeoverReusesReceipt(t *testing.T) {
	dir := t.TempDir()
	oldKey, newKey := filepath.Join(dir, "old.key"), filepath.Join(dir, "new.key")
	require.NoError(t, os.WriteFile(oldKey, []byte("old-material"), 0o600))
	require.NoError(t, os.WriteFile(newKey, []byte("new-material"), 0o600))
	name := "jwt-key-rotate-0123456789abcdefabcd"
	stateDir, receipt := filepath.Join(dir, name+".state"), filepath.Join(dir, name+".operation.receipt.json")
	parameters := map[string]any{
		"state_dir": stateDir, "receipt_output": receipt, "kubebrain_namespace": "instance-a", "kubebrain_statefulset": "kubebrain",
		"key_secret": "kubebrain-jwt-rotation", "old_key_field": "old-key", "new_key_field": "new-key", "old_key_source": oldKey, "new_key_source": newKey,
		"old_key_sha256": testSHA([]byte("old-material")), "new_key_sha256": testSHA([]byte("new-material")), "key_volume": "jwt-keys", "key_mount_dir": "/etc/kubebrain-jwt", "sign_method": "HS256",
		"endpoints": []string{"https://member-0:2379"}, "expected_replicas": 1, "jwt_ttl_seconds": 90, "max_clock_skew_seconds": 2, "probe_range_key": "/probe",
		"probe_cacert": "", "probe_cert": "", "probe_key": "", "probe_server_name": "", "probe_cacert_sha256": "", "probe_cert_sha256": "", "probe_key_sha256": "",
		"data_kube_context": "", "data_kubeconfig_path": "",
	}
	parameterData, err := json.Marshal(parameters)
	require.NoError(t, err)
	parameterPath := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameterPath, append(parameterData, '\n'), 0o600))
	digest := testSHA(append(parameterData, '\n'))
	logPath := filepath.Join(dir, "operation.log")
	operationctl, publisher, gate, issuer := filepath.Join(dir, "operationctl"), filepath.Join(dir, "publisher"), filepath.Join(dir, "gate"), filepath.Join(dir, "issuer")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
case " $* " in
  *" --action claim "*) printf '{"namespace":"%s","name":"%s","operation_id":"%s","uid":"operation-uid","instance":"instance-a","type":"JWTKeyRotation","requested_by":"platform:jwt-key-rotation","owner":"worker-a","attempt":%s,"parameters_sha256":"%s","parameters_secret":"%s-parameters","parameters_key":"parameters.json"}\n' "${CLAIM_NAMESPACE:-kubebrain-operations}" "$OPERATION_NAME" "$OPERATION_NAME" "$CLAIM_ATTEMPT" "$PARAMETERS_SHA" "$OPERATION_NAME" ;;
  *" --action parameters "*) cat "$PARAMETERS_PATH" ;;
  *" --action heartbeat "*) [[ "${FAIL_HEARTBEAT:-false}" != true ]] ;;
  *" --action succeed "*|*" --action retry "*) ;;
  *) exit 99 ;;
esac
`)
	writeTrafficExecutable(t, issuer, `#!/usr/bin/env bash
set -euo pipefail
(umask 077; printf 'token-for-%s\n' "${JWT_SIGNING_KEY##*/}" >"$JWT_TOKEN_OUTPUT")
`)
	writeTrafficExecutable(t, publisher, `#!/usr/bin/env bash
set -euo pipefail
phase=""; output=""
while [[ "$#" -gt 0 ]]; do case "$1" in --phase) phase="$2"; shift 2;; --receipt-output) output="$2"; shift 2;; *) shift;; esac; done
[[ "${HANG_PUBLISH:-false}" != true ]] || /bin/sleep 10
if [[ ! -e "$output" ]]; then (umask 077; printf '{"format":"publish","phase":"%s"}\n' "$phase" >"$output"); fi
`)
	writeTrafficExecutable(t, gate, `#!/usr/bin/env bash
set -euo pipefail
case "$ACTION" in
  phase-a) output="$STATE_DIR/$ROTATION_ID.phase-a.json"; body='{"format":"phase-a"}' ;;
  phase-b)
    output="$STATE_DIR/$ROTATION_ID.phase-b.json"
    a_sha="$(sha256sum "$STATE_DIR/$ROTATION_ID.phase-a.json" | cut -d ' ' -f1)"
    old_sha="$(sha256sum "$OLD_TOKEN_FILE" | cut -d ' ' -f1)"; new_sha="$(sha256sum "$NEW_TOKEN_FILE" | cut -d ' ' -f1)"
    body="$(jq -cnS --arg operation "$ROTATION_ID" --arg instance "$INSTANCE" --argjson endpoints '["https://member-0:2379"]' --arg a "$a_sha" --arg old "$old_sha" --arg new "$new_sha" '{format:"kubebrain.jwt-key-rotation.phase-b.v1",instance:$instance,rotation_id:$operation,endpoints:$endpoints,phase_a_sha256:$a,observed_at_unix:100,earliest_retirement_at_unix:192,jwt_ttl_seconds:90,max_clock_skew_seconds:2,statefulset:{generation:2,revision:"rev-b",uid:"sts-uid"},secret:{data_sha256:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",resource_version:"20",uid:"secret-uid"},old_token_sha256:$old,new_token_sha256:$new,all_members_accept_old:true,all_members_accept_new:true}')" ;;
  phase-c) output="$STATE_DIR/$ROTATION_ID.receipt.json"; body='{"format":"phase-c"}' ;;
esac
if [[ ! -e "$output" ]]; then (umask 077; printf '%s\n' "$body" >"$output"); fi
`)
	dateCommand, sleepCommand := filepath.Join(dir, "date"), filepath.Join(dir, "sleep")
	writeTrafficExecutable(t, dateCommand, "#!/usr/bin/env bash\nprintf '200\\n'\n")
	writeTrafficExecutable(t, sleepCommand, "#!/usr/bin/env bash\nexit 0\n")
	baseEnv := []string{
		"WORKER_ID=worker-a", "WORK_DIR=" + dir, "PARAMETERS_INPUT=" + parameterPath, "OPERATIONCTL=" + operationctl,
		"PUBLISHER_COMMAND=" + publisher, "ROTATION_COMMAND=" + gate, "TOKEN_ISSUER_COMMAND=" + issuer,
		"DATE=" + dateCommand, "SLEEP=" + sleepCommand, "HEARTBEAT_INTERVAL_SECONDS=1", "LEASE_SECONDS=6",
		"OPERATION_LOG=" + logPath, "OPERATION_NAME=" + name, "PARAMETERS_SHA=" + digest, "PARAMETERS_PATH=" + parameterPath,
	}
	output, err := runProductionRunnerCommand(t, "run-jwt-key-rotation-operation.sh", append(baseEnv, "CLAIM_ATTEMPT=1"))
	require.NoError(t, err, string(output))
	firstReceipt := mustRead(t, receipt)
	log := string(mustRead(t, logPath))
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--receipt-sha256")

	output, err = runProductionRunnerCommand(t, "run-jwt-key-rotation-operation.sh", append(baseEnv, "CLAIM_ATTEMPT=2"))
	require.NoError(t, err, string(output))
	require.Equal(t, firstReceipt, mustRead(t, receipt))
	require.Equal(t, 2, strings.Count(string(mustRead(t, logPath)), "--action succeed"))

	output, err = runProductionRunnerCommand(t, "run-jwt-key-rotation-operation.sh", append(baseEnv, "CLAIM_ATTEMPT=3", "FAIL_HEARTBEAT=true", "HANG_PUBLISH=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "heartbeat failed")
	require.Equal(t, firstReceipt, mustRead(t, receipt))
	require.Equal(t, 2, strings.Count(string(mustRead(t, logPath)), "--action succeed"))
}

func testSHA(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
