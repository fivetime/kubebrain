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
	requireOrdered(t, log, "gate begin", "hook publish", "gate complete", "scrape complete", "--action succeed")
	require.Contains(t, log, "--receipt-sha256 ")
	scrapeDigest := sha256.Sum256(mustRead(t, f.scrapeReceipt))
	require.Contains(t, log, "--receipt-sha256 "+fmt.Sprintf("%x", scrapeDigest))
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
}

func TestInfoCertificateRotationOperationResumesFromDurableEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, evidence   string
		wanted, unwanted []string
	}{
		{"state", "state", []string{"hook publish", "gate complete", "--action succeed"}, []string{"gate begin"}},
		{"receipt", "receipt", []string{"gate verify", "scrape complete", "--action succeed"}, []string{"gate begin", "hook publish", "gate complete"}},
		{"scrape receipt", "scrape", []string{"gate verify", "scrape verify", "--action succeed"}, []string{"gate begin", "hook publish", "gate complete", "scrape complete"}},
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
	for _, step := range []string{"begin", "publish", "complete", "scrape"} {
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

func TestInfoCertificateRotationOperationRejectsInvalidScrapeReceipt(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, false, "INVALID_SCRAPE_RECEIPT=true", "scrape recovery receipt invalid")
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestInfoCertificateRotationOperationRejectsTLSReceiptMutationByScrapeStep(t *testing.T) {
	f := newInfoRotationRunnerFixture(t)
	f.run(t, false, "TAMPER_TLS_RECEIPT=true", "receipt changed during scrape recovery")
	require.NotContains(t, string(mustRead(t, f.log)), "--action succeed")
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

func TestInfoCertificateRotationOperationRejectsCredentialPathEscape(t *testing.T) {
	for _, tc := range []struct{ name, wanted string }{
		{"URL", "prometheus_url must use the trusted deployment endpoint"},
		{"CA", "prometheus_ca_file must use the dedicated Prometheus credential mount"},
		{"token", "prometheus_bearer_token_file must use the dedicated Prometheus credential mount"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInfoRotationRunnerFixture(t)
			parameters := string(mustRead(t, f.parameters))
			if tc.name == "URL" {
				parameters = strings.Replace(parameters, `"prometheus_url":"https://prometheus.example"`, `"prometheus_url":"https://collector.attacker.example"`, 1)
			} else if tc.name == "CA" {
				old := fmt.Sprintf(`"prometheus_ca_file":%q`, filepath.Join(f.dir, "prometheus-ca"))
				parameters = strings.Replace(parameters, old, `"prometheus_ca_file":"/var/run/secrets/kubebrain-parameter/token"`, 1)
			} else {
				old := `"recovery_timeout_seconds":`
				replacement := `"prometheus_bearer_token_file":"/var/run/secrets/kubebrain-parameter/token","prometheus_bearer_token_sha256":"` + strings.Repeat("0", 64) + `","recovery_timeout_seconds":`
				parameters = strings.Replace(parameters, old, replacement, 1)
			}
			require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))
			f.run(t, false, tc.wanted)
			require.NotContains(t, string(mustRead(t, f.log)), "gate ")
		})
	}
}

type infoRotationRunnerFixture struct {
	dir, parameters, operationctl, rotation, publish, scrape, log, stateDir, receipt, scrapeReceipt string
}

func newInfoRotationRunnerFixture(t *testing.T) *infoRotationRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	f := &infoRotationRunnerFixture{dir: dir, log: filepath.Join(dir, "operations.log"), stateDir: filepath.Join(dir, "state")}
	f.parameters = filepath.Join(dir, "parameters.json")
	f.operationctl = filepath.Join(dir, "operationctl")
	f.rotation = filepath.Join(dir, "rotation")
	f.publish = filepath.Join(dir, "publish")
	f.scrape = filepath.Join(dir, "scrape")
	f.receipt = filepath.Join(dir, "receipt.json")
	f.scrapeReceipt = filepath.Join(dir, "scrape-receipt.json")
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
	prometheusCA := filepath.Join(dir, "prometheus-ca")
	prometheusCAData := []byte("prometheus-ca\n")
	require.NoError(t, os.WriteFile(prometheusCA, prometheusCAData, 0o600))
	prometheusCAHash := sha256.Sum256(prometheusCAData)
	parameters := fmt.Sprintf(`{"state_dir":%q,"info_endpoint":"https://info.example:9090","info_server_name":"info.example","old_info_cacert":%q,"old_info_cert":%q,"new_info_cacert":%q,"new_info_cert":%q,"receipt_output":%q,"scrape_receipt_output":%q,"kubebrain_namespace":"kubebrain-system","pod_selector":"app=kubebrain","kubebrain_service":"kubebrain-peer","expected_replicas":3,"require_old_ca_rejection":true,"old_info_cacert_sha256":%q,"old_info_cert_sha256":%q,"new_info_cacert_sha256":%q,"new_info_cert_sha256":%q,"prometheus_url":"https://prometheus.example","prometheus_ca_file":%q,"prometheus_ca_sha256":"%x","recovery_timeout_seconds":120,"poll_interval_seconds":5,"query_timeout_seconds":10,"max_clock_skew_seconds":5,"max_sample_age_seconds":60}`+"\n", f.stateDir, credentials[0], credentials[1], credentials[2], credentials[3], f.receipt, f.scrapeReceipt, hashes[0], hashes[1], hashes[2], hashes[3], prometheusCA, prometheusCAHash)
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
	scrape := `#!/usr/bin/env bash
set -euo pipefail
printf 'scrape %s\n' "$ACTION" >>"$TEST_LOG"
[[ "${FAIL_STEP:-}" != scrape ]] || exit 9
if [[ "$ACTION" == complete ]]; then
  tls_digest="$(sha256sum "$TLS_RECEIPT_INPUT" | cut -d ' ' -f1)"
  if [[ "${INVALID_SCRAPE_RECEIPT:-false}" == true ]]; then printf '{"format":"wrong"}\n' >"$SCRAPE_RECEIPT_OUTPUT"; else
    jq -cnS --arg digest "$tls_digest" --arg certificate "$(printf '%064d' 2)" '{all_targets_up:true,format:"kubebrain.info-scrape-recovery.receipt.v1",info_endpoint:"https://info.example:9090",instance:"instance-a",namespace:"kubebrain-system",new_certificate_sha256:$certificate,observed_at_unix:124,oldest_sample_unix:124,prometheus_query:"up{namespace=\"kubebrain-system\",service=\"kubebrain-peer\"}",replicas:3,rotation_id:"rotation-1",service:"kubebrain-peer",targets:[{instance:"i0",pod:"pod-0",sample_unix:124},{instance:"i1",pod:"pod-1",sample_unix:124},{instance:"i2",pod:"pod-2",sample_unix:124}],tls_rotation_completed_at_unix:123,tls_rotation_receipt_sha256:$digest}' >"$SCRAPE_RECEIPT_OUTPUT"
  fi
  [[ "${TAMPER_TLS_RECEIPT:-false}" != true ]] || printf changed >"$TLS_RECEIPT_INPUT"
fi
`
	require.NoError(t, os.WriteFile(f.operationctl, []byte(opctl), 0o755))
	require.NoError(t, os.WriteFile(f.rotation, []byte(rotation), 0o755))
	require.NoError(t, os.WriteFile(f.publish, []byte(publish), 0o755))
	require.NoError(t, os.WriteFile(f.scrape, []byte(scrape), 0o755))
	return f
}

func (f *infoRotationRunnerFixture) run(t *testing.T, success bool, extra ...string) {
	t.Helper()
	digest := sha256.Sum256(mustRead(t, f.parameters))
	env := []string{"WORKER_ID=worker-a", "OPERATION_NAMESPACE=ops", "PARAMETERS_INPUT=" + f.parameters, "OPERATIONCTL=" + f.operationctl, "ROTATION_COMMAND=" + f.rotation, "SCRAPE_COMMAND=" + f.scrape, "PUBLISH_COMMAND=" + f.publish, "EXPECTED_PROMETHEUS_URL=https://prometheus.example", "PROMETHEUS_CA_SOURCE=" + filepath.Join(f.dir, "prometheus-ca"), "PROMETHEUS_TOKEN_SOURCE=" + filepath.Join(f.dir, "prometheus-token"), "TEST_LOG=" + f.log, "RECEIPT_OUTPUT=" + f.receipt, "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=5", "CLAIM_DIGEST=" + fmt.Sprintf("%x", digest)}
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
	if kind == "receipt" || kind == "scrape" {
		receipt := `{"completed_at_unix":123,"format":"kubebrain.info-certificate-rotation.receipt.v1","info_endpoint":"https://info.example:9090","instance":"instance-a","new_certificate_sha256":"0000000000000000000000000000000000000000000000000000000000000002","old_ca_rejected":true,"old_ca_rejection_required":true,"old_certificate_sha256":"0000000000000000000000000000000000000000000000000000000000000001","pods_unchanged":true,"replicas":3,"rotation_id":"rotation-1"}` + "\n"
		require.NoError(t, os.WriteFile(f.receipt, []byte(receipt), 0o600))
		if kind == "scrape" {
			digest := sha256.Sum256([]byte(receipt))
			scrapeReceipt := fmt.Sprintf(`{"all_targets_up":true,"format":"kubebrain.info-scrape-recovery.receipt.v1","info_endpoint":"https://info.example:9090","instance":"instance-a","namespace":"kubebrain-system","new_certificate_sha256":"0000000000000000000000000000000000000000000000000000000000000002","observed_at_unix":124,"oldest_sample_unix":124,"prometheus_query":"up{namespace=\"kubebrain-system\",service=\"kubebrain-peer\"}","replicas":3,"rotation_id":"rotation-1","service":"kubebrain-peer","targets":[{"instance":"i0","pod":"pod-0","sample_unix":124},{"instance":"i1","pod":"pod-1","sample_unix":124},{"instance":"i2","pod":"pod-2","sample_unix":124}],"tls_rotation_completed_at_unix":123,"tls_rotation_receipt_sha256":"%x"}`+"\n", digest)
			require.NoError(t, os.WriteFile(f.scrapeReceipt, []byte(scrapeReceipt), 0o600))
		}
	}
}
