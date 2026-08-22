package production_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateInfoScrapeRecoveryBindsPostRotationSamples(t *testing.T) {
	f := newInfoScrapeFixture(t)
	out, err := runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", append(f.env,
		"FAKE_STALE_ONCE=true", "RECOVERY_TIMEOUT_SECONDS=2", "POLL_INTERVAL_SECONDS=0"))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "info scrape recovery gate passed")
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, f.output), &receipt))
	require.Equal(t, "kubebrain.info-scrape-recovery.receipt.v1", receipt["format"])
	require.Equal(t, true, receipt["all_targets_up"])
	require.Equal(t, float64(3), receipt["replicas"])
	require.GreaterOrEqual(t, receipt["oldest_sample_unix"].(float64), float64(100))
	digest := sha256.Sum256(mustRead(t, f.tlsReceipt))
	require.Equal(t, fmt.Sprintf("%x", digest), receipt["tls_rotation_receipt_sha256"])
	require.Len(t, receipt["targets"], 3)
	require.Equal(t, 2, strings.Count(string(mustRead(t, f.calls)), "query="))
	out, err = runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", append(f.env, "ACTION=verify"))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "receipt verification passed")

	out, err = runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", f.env)
	require.Error(t, err)
	require.Contains(t, string(out), "must not already exist")
	tampered := strings.Replace(string(mustRead(t, f.output)), `"service":"kubebrain-peer"`, `"service":"other"`, 1)
	require.NoError(t, os.WriteFile(f.output, []byte(tampered), 0o600))
	out, err = runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", append(f.env, "ACTION=verify"))
	require.Error(t, err)
	require.Contains(t, string(out), "does not match verified evidence")
}

func TestValidateInfoScrapeRecoveryRejectsPreRotationAndInvalidVectors(t *testing.T) {
	for _, tc := range []struct{ name, env string }{
		{"pre rotation", "FAKE_ALWAYS_STALE=true"},
		{"target down", "FAKE_DOWN=true"},
		{"duplicate pod", "FAKE_DUPLICATE_POD=true"},
		{"future sample", "FAKE_FUTURE=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInfoScrapeFixture(t)
			out, err := runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", append(f.env,
				tc.env, "RECOVERY_TIMEOUT_SECONDS=0", "POLL_INTERVAL_SECONDS=0"))
			require.Error(t, err)
			require.Contains(t, string(out), "info scrape recovery timed out")
			require.NoFileExists(t, f.output)
		})
	}
}

func TestValidateInfoScrapeRecoveryRejectsUnsafeInputsBeforeQuery(t *testing.T) {
	f := newInfoScrapeFixture(t)
	for _, tc := range []struct{ env, want string }{
		{"EXPECTED_REPLICAS=2147483648", "canonical positive int32"},
		{"PROMETHEUS_URL=http://prometheus.example", "absolute HTTPS"},
		{"KUBEBRAIN_SERVICE=peer/service", "DNS label"},
		{"QUERY_TIMEOUT_SECONDS=301", "no greater than 300"},
	} {
		out, err := runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", append(f.env, tc.env))
		require.Error(t, err)
		require.Contains(t, string(out), tc.want)
		require.NoFileExists(t, f.calls)
	}
}

func TestValidateInfoScrapeRecoveryRejectsOversizedFileInputsBeforeQuery(t *testing.T) {
	for _, tc := range []struct{ name, wanted string }{
		{"TLS receipt", "TLS_RECEIPT_INPUT must contain 1..1048576 bytes"},
		{"Prometheus CA", "PROMETHEUS_CA_FILE must contain 1..1048576 bytes"},
		{"Prometheus token", "PROMETHEUS_BEARER_TOKEN_FILE must contain 1..16384 bytes"},
		{"scrape receipt", "SCRAPE_RECEIPT_OUTPUT must contain 1..2097152 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInfoScrapeFixture(t)
			env := append([]string{}, f.env...)
			switch tc.name {
			case "TLS receipt":
				require.NoError(t, os.Truncate(f.tlsReceipt, 1048577))
			case "Prometheus CA":
				require.NoError(t, os.Truncate(f.ca, 1048577))
			case "Prometheus token":
				token := filepath.Join(f.dir, "token")
				require.NoError(t, os.WriteFile(token, []byte("x"), 0o600))
				require.NoError(t, os.Truncate(token, 16385))
				env = append(env, "PROMETHEUS_BEARER_TOKEN_FILE="+token)
			case "scrape receipt":
				require.NoError(t, os.WriteFile(f.output, []byte("x"), 0o600))
				require.NoError(t, os.Truncate(f.output, 2097153))
				env = append(env, "ACTION=verify")
			}
			out, err := runProductionScriptCommand(t, "validate-info-scrape-recovery.sh", env)
			require.Error(t, err)
			require.Contains(t, string(out), tc.wanted)
			require.NoFileExists(t, f.calls)
		})
	}
}

type infoScrapeFixture struct {
	dir, tlsReceipt, output, calls, ca string
	env                                []string
}

func newInfoScrapeFixture(t *testing.T) *infoScrapeFixture {
	t.Helper()
	dir := t.TempDir()
	f := &infoScrapeFixture{dir: dir, tlsReceipt: filepath.Join(dir, "tls.json"), output: filepath.Join(dir, "scrape.json"), calls: filepath.Join(dir, "calls")}
	tls := `{"completed_at_unix":100,"format":"kubebrain.info-certificate-rotation.receipt.v1","info_endpoint":"https://info.example:8080","instance":"instance-a","new_certificate_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","old_ca_rejected":true,"old_ca_rejection_required":true,"old_certificate_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pods_unchanged":true,"replicas":3,"rotation_id":"rotation-1"}` + "\n"
	require.NoError(t, os.WriteFile(f.tlsReceipt, []byte(tls), 0o600))
	f.ca = filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(f.ca, []byte("ca"), 0o600))
	curl := filepath.Join(dir, "curl")
	writeExecutable(t, curl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_CALLS"
count="$(wc -l <"$FAKE_CALLS")"
timestamp="$(date +%s)"
[[ "${FAKE_ALWAYS_STALE:-false}" != true ]] || timestamp=99
[[ "${FAKE_STALE_ONCE:-false}" != true || "$count" != 1 ]] || timestamp=99
[[ "${FAKE_FUTURE:-false}" != true ]] || timestamp=$((timestamp + 1000))
printf '{"status":"success","data":{"resultType":"vector","result":['
for i in 0 1 2; do
  ((i == 0)) || printf ','
  pod="kubebrain-$i"
  [[ "${FAKE_DUPLICATE_POD:-false}" != true || "$i" != 2 ]] || pod=kubebrain-1
  value=1
  [[ "${FAKE_DOWN:-false}" != true || "$i" != 2 ]] || value=0
  printf '{"metric":{"namespace":"kubebrain-system","service":"kubebrain-peer","pod":"%s","instance":"10.0.0.%d:8080"},"value":[%s,"%s"]}' "$pod" "$i" "$timestamp" "$value"
done
printf ']}}\n'
`)
	f.env = []string{
		"TLS_RECEIPT_INPUT=" + f.tlsReceipt, "SCRAPE_RECEIPT_OUTPUT=" + f.output,
		"PROMETHEUS_URL=https://prometheus.example", "PROMETHEUS_CA_FILE=" + f.ca,
		"KUBEBRAIN_NAMESPACE=kubebrain-system", "KUBEBRAIN_SERVICE=kubebrain-peer", "EXPECTED_REPLICAS=3",
		"CURL=" + curl, "JQ=jq", "FAKE_CALLS=" + f.calls, "MAX_CLOCK_SKEW_SECONDS=300",
	}
	return f
}
