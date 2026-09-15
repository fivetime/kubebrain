package production_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCaptureRolloutTLSMetricsIdentity(t *testing.T) {
	for _, mode := range []string{"stable", "restart", "wrong-probe", "wrong-namespace"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, content string) string {
				path := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(path, []byte(content), 0700))
				return path
			}
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			require.NoError(t, err)
			cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
			der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
			require.NoError(t, err)
			ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
			write("ca.crt", ca)
			cm, err := json.Marshal(map[string]any{"metadata": map[string]any{"uid": "ca-uid"}, "immutable": true, "data": map[string]string{"ca.crt": ca}})
			require.NoError(t, err)
			write("ca.json", string(cm))
			digest := "sha256:" + strings.Repeat("a", 64)
			image := "registry/test@" + digest
			phase := write("phase.json", `{"format":"kubebrain.rollout-diagnostic-phase.v1","phase":"stable","probe_uid":"probe-uid","statefulset_uid":"sts-uid","image":"`+image+`"}`)
			mock := write("kubectl", `#!/bin/bash
set -euo pipefail
while [[ "$1" != get && "$1" != logs && "$1" != exec ]]; do shift; done
case "$1/$2" in
  get/namespace)
    uid=ns-uid; [[ "$MODE" != wrong-namespace ]] || uid=other
    if [[ "$*" == *jsonpath* ]]; then printf '%s' "$uid"; else jq -cn --arg uid "$uid" '{metadata:{uid:$uid}}'; fi ;;
  get/configmap) cat "$FIXTURE/ca.json" ;;
  get/pod)
    pod=$3; uid="$pod-uid"; count=0
    [[ ! -f "$FIXTURE/$pod.count" ]] || count=$(<"$FIXTURE/$pod.count")
    echo $((count+1)) > "$FIXTURE/$pod.count"
    restart=0
    if [[ "$MODE" == restart && "$pod" == brain-0 && "$count" -gt 0 ]]; then restart=1; fi
    if [[ "$MODE" == wrong-probe && "$pod" == probe ]]; then uid=other; fi
    jq -cn --arg uid "$uid" --arg image "$IMAGE" --arg digest "$DIGEST" --argjson restart "$restart" '
      {metadata:{uid:$uid,ownerReferences:[{uid:"sts-uid",controller:true}]},spec:{containers:[{image:$image}]},
       status:{phase:"Running",podIP:"127.0.0.1",containerStatuses:[{name:"main",ready:true,containerID:"container",imageID:$digest,restartCount:$restart,state:{running:{startedAt:"start"}}}]}}' ;;
  logs/*) printf 'PROBE_PROGRESS scope=diagnostic_only\nPROBE_WATCH_DELIVERY scope=diagnostic_only\n' ;;
  exec/*)
    [[ "$*" == *'--cacert /dev/stdin'* && "$*" == *'--connect-to service.test:8080:127.0.0.1:8080'* && "$*" == *'https://service.test:8080/metrics'* ]]
    incoming=$(cat)
    [[ "$incoming" == "$(<"$FIXTURE/ca.crt")" ]]
    for family in async_commit_txn_counter commit_txn_counter one_pc_txn_counter; do printf 'tikv_client_go_%s{type="ok"} 1\n' "$family"; done ;;
  *) exit 90 ;;
esac
`)
			cmd := exec.Command("bash", "capture-rollout-tls-metrics.sh", dir, phase)
			cmd.Env = append(os.Environ(), "FIXTURE="+dir, "MODE="+mode, "IMAGE="+image, "DIGEST="+digest,
				"KUBECONFIG="+write("config", "unused"), "KUBECTL_CONTEXT=test", "KUBEBRAIN_NAMESPACE=test", "KUBEBRAIN_STATEFULSET=brain", "PROBE_POD=probe",
				"PROBE_INFO_CA_CONFIGMAP=ca", "PROBE_INFO_TLS_SERVER_NAME=service.test", "TARGET_RUNTIME_DIGESTS="+digest,
				"EXPECTED_REPLICAS=1", "EXPECTED_INFO_PORT=8080", "DIAGNOSTIC_NAMESPACE_UID=ns-uid", "DIAGNOSTIC_INFO_CA_UID=ca-uid", "DIAGNOSTIC_KUBECTL_BIN="+mock)
			out, err := cmd.CombinedOutput()
			if mode == "stable" {
				require.NoError(t, err, string(out))
				require.FileExists(t, filepath.Join(dir, "capture", "ended-at"))
			} else {
				require.Error(t, err, string(out))
				require.NoFileExists(t, filepath.Join(dir, "capture", "ended-at"))
			}
		})
	}
}
