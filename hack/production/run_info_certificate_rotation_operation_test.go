package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfoCertificateRotationOperationCompletesLifecycle(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, true, "")
	log := string(mustRead(t, f.log))
	requireOrdered(t, log, "gate begin", "hook publish", "gate complete", "--action succeed")
	require.Contains(t, log, "--receipt-sha256 ")
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
}

func TestInfoCertificateRotationOperationResumesFromDurableEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, evidence   string
		wanted, unwanted []string
	}{
		{"state", "state", []string{"hook publish", "gate complete", "--action succeed"}, []string{"gate begin"}},
		{"receipt", "receipt", []string{"gate verify", "--action succeed"}, []string{"gate begin", "hook publish", "gate complete"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInfoRotationRunnerFixture(t)
			f.publishEvidence(t, tc.evidence)
			f.run(t, true, "")
			log := string(mustRead(t, f.log))
			for _, wanted := range tc.wanted {
				require.Contains(t, log, wanted)
			}
			for _, unwanted := range tc.unwanted {
				require.NotContains(t, log, unwanted)
			}
		})
	}
}

func TestInfoCertificateRotationOperationRequeuesStepFailure(t *testing.T) {
	for _, step := range []string{"begin", "publish", "complete"} {
		t.Run(step, func(t *testing.T) {
			f := newInfoRotationRunnerFixture(t)
			f.run(t, false, "FAIL_STEP="+step, "was requeued")
			log := string(mustRead(t, f.log))
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestInfoCertificateRotationOperationRejectsInvalidReceipt(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, false, "INVALID_RECEIPT=true", "receipt invalid")
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestInfoCertificateRotationOperationRejectsInvalidStateBeforePublish(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, false, "MALFORMED_STATE=true", "was requeued")
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "hook publish")
}

func TestInfoCertificateRotationOperationRejectsInvalidClaimNamespace(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, false, "CLAIM_NAMESPACE=tenant/a", "claimed operation namespace is invalid")
	log := string(mustRead(t, f.log))
	require.NotContains(t, log, "gate ")
	require.NotContains(t, log, "hook ")
}

func TestInfoCertificateRotationOperationDoesNotSucceedAfterFinalFence(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, false, "FAIL_FINAL_HEARTBEAT=true", "final heartbeat failed")
	require.NotContains(t, string(mustRead(t, f.log)), "--action succeed")
}

type infoRotationRunnerFixture struct {
	dir, parameters, operationctl, rotation, publish, log, stateDir, receipt string
}

func newInfoRotationRunnerFixture(t *testing.T) *infoRotationRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	f := &infoRotationRunnerFixture{dir: dir, log: filepath.Join(dir, "operations.log"), stateDir: filepath.Join(dir, "state")}
	f.parameters = filepath.Join(dir, "parameters.json")
	f.operationctl = filepath.Join(dir, "operationctl")
	f.rotation = filepath.Join(dir, "rotation")
	f.publish = filepath.Join(dir, "publish")
	f.receipt = filepath.Join(dir, "receipt.json")
	require.NoError(t, os.Mkdir(f.stateDir, 0o700))
	credentials := make([]string, 4)
	hashes := make([]string, 4)
	for i, name := range []string{"old-ca", "old-cert", "new-ca", "new-cert"} {
		credentials[i] = filepath.Join(dir, name)
		data := []byte(name + "\n")
		require.NoError(t, os.WriteFile(credentials[i], data, 0o600))
		digest := sha256.Sum256(data)
		hashes[i] = fmt.Sprintf("%x", digest)
	}
	parameters := fmt.Sprintf(`{"state_dir":%q,"info_endpoint":"https://info.example:9090","info_server_name":"info.example","old_info_cacert":%q,"old_info_cert":%q,"new_info_cacert":%q,"new_info_cert":%q,"receipt_output":%q,"kubebrain_namespace":"kubebrain-system","pod_selector":"app=kubebrain","expected_replicas":3,"require_old_ca_rejection":true,"old_info_cacert_sha256":%q,"old_info_cert_sha256":%q,"new_info_cacert_sha256":%q,"new_info_cert_sha256":%q}`+"\n", f.stateDir, credentials[0], credentials[1], credentials[2], credentials[3], f.receipt, hashes[0], hashes[1], hashes[2], hashes[3])
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

	opctl := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$TEST_LOG"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$(sha256sum "$PARAMETERS_INPUT" | cut -d ' ' -f1)}"
  printf '{"namespace":"%s","name":"rotation-1","operation_id":"rotation-1","instance":"instance-a","type":"InfoCertificateRotation","requested_by":"platform:info-certificate-rotation","owner":"worker-a","attempt":1,"parameters_sha256":"%s"}\n' "${CLAIM_NAMESPACE:-tenant-a-operations}" "$digest"
elif [[ " $* " == *" --action heartbeat "* && "${FAIL_FINAL_HEARTBEAT:-false}" == true && -e "$RECEIPT_OUTPUT" ]]; then
  exit 1
fi
`
	rotation := `#!/usr/bin/env bash
set -euo pipefail
printf 'gate %s\n' "$ACTION" >>"$TEST_LOG"
[[ "${FAIL_STEP:-}" != "$ACTION" ]] || exit 9
state="$STATE_DIR/$ROTATION_ID.info.state"
if [[ "$ACTION" == begin ]]; then
  if [[ "${MALFORMED_STATE:-false}" == true ]]; then printf 'bad\n' >"$state"; else
    printf 'kubebrain.info-certificate-rotation.state.v1\t%s\t%s\t%s\t%064d\t%064d\npod-0\tuid-0\t0\ttrue\npod-1\tuid-1\t0\ttrue\npod-2\tuid-2\t0\ttrue\n' "$INSTANCE" "$ROTATION_ID" "$INFO_ENDPOINT" 1 2 >"$state"
  fi
elif [[ "$ACTION" == complete ]]; then
  if [[ "${INVALID_RECEIPT:-false}" == true ]]; then printf '{"format":"wrong"}\n' >"$RECEIPT_OUTPUT"; else
    printf '{"completed_at_unix":123,"format":"kubebrain.info-certificate-rotation.receipt.v1","info_endpoint":"%s","instance":"%s","new_certificate_sha256":"%064d","old_ca_rejected":true,"old_ca_rejection_required":true,"old_certificate_sha256":"%064d","pods_unchanged":true,"replicas":3,"rotation_id":"%s"}\n' "$INFO_ENDPOINT" "$INSTANCE" 2 1 "$ROTATION_ID" >"$RECEIPT_OUTPUT"
  fi
fi
`
	publish := `#!/usr/bin/env bash
set -euo pipefail
printf 'hook publish\n' >>"$TEST_LOG"
[[ "${FAIL_STEP:-}" != publish ]]
`
	require.NoError(t, os.WriteFile(f.operationctl, []byte(opctl), 0o755))
	require.NoError(t, os.WriteFile(f.rotation, []byte(rotation), 0o755))
	require.NoError(t, os.WriteFile(f.publish, []byte(publish), 0o755))
	return f
}

func (f *infoRotationRunnerFixture) run(t *testing.T, success bool, extra ...string) {
	t.Helper()
	digest := sha256.Sum256(mustRead(t, f.parameters))
	env := []string{"WORKER_ID=worker-a", "OPERATION_NAMESPACE=ops", "PARAMETERS_INPUT=" + f.parameters, "OPERATIONCTL=" + f.operationctl, "ROTATION_COMMAND=" + f.rotation, "PUBLISH_COMMAND=" + f.publish, "TEST_LOG=" + f.log, "RECEIPT_OUTPUT=" + f.receipt, "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=5", "CLAIM_DIGEST=" + fmt.Sprintf("%x", digest)}
	var wanted string
	for _, value := range extra {
		if strings.Contains(value, "=") {
			env = append(env, value)
		} else {
			wanted = value
		}
	}
	out, err := runProductionRunnerCommand(t, "run-info-certificate-rotation-operation.sh", env)
	if success {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err)
		require.Contains(t, string(out), wanted)
	}
}

func (f *infoRotationRunnerFixture) publishEvidence(t *testing.T, kind string) {
	t.Helper()
	state := "kubebrain.info-certificate-rotation.state.v1\tinstance-a\trotation-1\thttps://info.example:9090\t" + strings.Repeat("0", 63) + "1\t" + strings.Repeat("0", 63) + "2\n" +
		"pod-0\tuid-0\t0\ttrue\npod-1\tuid-1\t0\ttrue\npod-2\tuid-2\t0\ttrue\n"
	require.NoError(t, os.WriteFile(filepath.Join(f.stateDir, "rotation-1.info.state"), []byte(state), 0o600))
	if kind == "receipt" {
		receipt := `{"completed_at_unix":123,"format":"kubebrain.info-certificate-rotation.receipt.v1","info_endpoint":"https://info.example:9090","instance":"instance-a","new_certificate_sha256":"0000000000000000000000000000000000000000000000000000000000000002","old_ca_rejected":true,"old_ca_rejection_required":true,"old_certificate_sha256":"0000000000000000000000000000000000000000000000000000000000000001","pods_unchanged":true,"replicas":3,"rotation_id":"rotation-1"}` + "\n"
		require.NoError(t, os.WriteFile(f.receipt, []byte(receipt), 0o600))
	}
}
