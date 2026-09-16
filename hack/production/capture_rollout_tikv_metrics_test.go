package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCaptureRolloutTiKVMetricsPinnedIdentity(t *testing.T) {
	testCaptureRolloutTiKVMetricsPinnedIdentity(t, false)
}

func TestCaptureRolloutBackendMetricsPinnedIdentity(t *testing.T) {
	testCaptureRolloutTiKVMetricsPinnedIdentity(t, true)
}

func testCaptureRolloutTiKVMetricsPinnedIdentity(t *testing.T, independent bool) {
	for _, allowUnready := range []string{"", "false", "true", "invalid"} {
		for _, mode := range []string{"stable", "missing-scheduler", "wrong-scheduler-type", "scheduler-without-type", "wrong-namespace", "wrong-owner", "wrong-pod", "not-running", "wrong-start", "restart", "wrong-image", "wrong-container", "wrong-count", "unready", "sts-unready", "readiness-change", "scope-change", "empty", "oversized", "missing-family", "curl-error"} {
			t.Run(allowUnready+"/"+mode, func(t *testing.T) {
				dir := t.TempDir()
				write := func(name, contents string) string {
					path := filepath.Join(dir, name)
					require.NoError(t, os.WriteFile(path, []byte(contents), 0700))
					return path
				}
				digest := "sha256:" + strings.Repeat("a", 64)
				receipt := write("receipt.json", `{"format":"kubebrain.tikv-metrics-receipt.v1","namespace":"test","namespace_uid":"ns-uid","statefulset":{"name":"kb-tikv","uid":"sts-uid"},"pods":[{"name":"kb-tikv-0","uid":"pod-uid","container":"tikv","image":"tikv:v8.5.3","imageID":"`+digest+`","containerID":"container","restartCount":0,"startedAt":"start"}]}`)
				mock := write("kubectl", `#!/bin/bash
set -euo pipefail
while [[ "$1" != get && "$1" != exec ]]; do shift; done
case "$1/$2" in
  get/namespace)
    uid=ns-uid; [[ "$MODE" != wrong-namespace ]] || uid=other
    jq -cn --arg uid "$uid" '{metadata:{uid:$uid}}' ;;
  get/statefulset)
    gen=1; if [[ "$MODE" == scope-change && -f "$FIXTURE/scope-seen" ]]; then gen=2; fi
    touch "$FIXTURE/scope-seen"
    replicas=1; [[ "$MODE" != wrong-count ]] || replicas=2
    ready=$replicas; [[ "$MODE" != sts-unready ]] || ready=0
    jq -cn --argjson gen "$gen" --argjson replicas "$replicas" --argjson ready "$ready" '{metadata:{uid:"sts-uid",generation:$gen},spec:{replicas:$replicas},status:{readyReplicas:$ready,observedGeneration:$gen}}' ;;
  get/pod)
    owner=sts-uid; [[ "$MODE" != wrong-owner ]] || owner=other
    restart=0; if [[ "$MODE" == restart && -f "$FIXTURE/pod-seen" ]]; then restart=1; fi
    changed=false; [[ ! -f "$FIXTURE/pod-seen" ]] || changed=true
    touch "$FIXTURE/pod-seen"
    image="$DIGEST"; [[ "$MODE" != wrong-image ]] || image=other
    container=container; [[ "$MODE" != wrong-container ]] || container=other
    ready=true; [[ "$MODE" != unready ]] || ready=false
    if [[ "$MODE" == readiness-change && "$changed" == true ]]; then ready=false; fi
    uid=pod-uid; [[ "$MODE" != wrong-pod ]] || uid=other
    phase=Running; [[ "$MODE" != not-running ]] || phase=Pending
    started=start; [[ "$MODE" != wrong-start ]] || started=other
    jq -cn --arg uid "$uid" --arg phase "$phase" --arg started "$started" --arg owner "$owner" --arg image "$image" --arg container "$container" --argjson restart "$restart" --argjson ready "$ready" '
    {metadata:{uid:$uid,ownerReferences:[{uid:$owner,controller:true}]},spec:{containers:[{name:"tikv",image:"tikv:v8.5.3"}]},status:{phase:$phase,containerStatuses:[{name:"tikv",ready:$ready,containerID:$container,imageID:$image,restartCount:$restart,state:{running:{startedAt:$started}}}]}}' ;;
  exec/*)
    [[ "$*" == 'exec kb-tikv-0 -c tikv -- curl --fail --silent --show-error --max-time 10 http://127.0.0.1:20180/metrics' ]]
    case "$MODE" in
      curl-error) exit 28 ;;
      empty) exit 0 ;;
      oversized) head -c 4194305 /dev/zero; exit 0 ;;
      missing-family) printf '# TYPE raft_engine_sync_log_duration_seconds histogram\n'; exit 0 ;;
    esac
    for family in raft_engine_sync_log_duration_seconds tikv_grpc_msg_duration_seconds tikv_scheduler_command_duration_seconds; do
      if [[ "$family" == tikv_scheduler_command_duration_seconds ]]; then
        case "$MODE" in
          missing-scheduler) continue ;;
          wrong-scheduler-type) printf '# TYPE %s gauge\n%s 1\n' "$family" "$family"; continue ;;
          scheduler-without-type) printf '%s_count 1\n' "$family"; continue ;;
        esac
      fi
      printf '# TYPE %s histogram\n%s_count 1\n' "$family" "$family"
    done ;;
  *) exit 90 ;;
esac
`)
				script, input := "capture-rollout-tikv-metrics.sh", receipt
				if independent {
					script = "capture-rollout-backend-metrics.sh"
					// Application phase is deliberately unknown; backend identity
					// still must be validated and capture must remain possible.
					input = write("phase.json", `{"phase":"unknown"}`)
				}
				cmd := exec.Command("bash", script, dir, input)
				cmd.Env = append(os.Environ(), "MODE="+mode, "FIXTURE="+dir, "DIGEST="+digest,
					"KUBECONFIG="+write("config", "unused"), "KUBECTL_CONTEXT=test", "KUBEBRAIN_NAMESPACE=test",
					"DIAGNOSTIC_NAMESPACE_UID=ns-uid", "DIAGNOSTIC_KUBECTL_BIN="+mock,
					"DIAGNOSTIC_TIKV_ALLOW_UNREADY="+allowUnready, "DIAGNOSTIC_TIKV_RECEIPT="+receipt)
				out, err := cmd.CombinedOutput()
				if allowUnready != "invalid" && (mode == "stable" || mode == "missing-scheduler" || allowUnready == "true" && (mode == "unready" || mode == "sts-unready")) {
					require.NoError(t, err, string(out))
					setting, err := os.ReadFile(filepath.Join(dir, "tikv", "allow-unready"))
					require.NoError(t, err)
					expectedSetting := allowUnready
					if expectedSetting == "" {
						expectedSetting = "false"
					}
					require.Equal(t, expectedSetting+"\n", string(setting))
					require.FileExists(t, filepath.Join(dir, "tikv", "ended-at"))
					availability, err := os.ReadFile(filepath.Join(dir, "tikv", "kb-tikv-0-metric-availability.json"))
					require.NoError(t, err)
					expectedAvailability := "present"
					if mode == "missing-scheduler" {
						expectedAvailability = "absent"
					}
					require.JSONEq(t, `{"tikv_scheduler_command_duration_seconds":"`+expectedAvailability+`"}`, string(availability))
					retry := exec.Command("bash", script, dir, input)
					retry.Env = cmd.Env
					out, err = retry.CombinedOutput()
					require.Error(t, err, "must reject overwriting an existing capture: %s", out)
					require.Contains(t, string(out), "File exists")
				} else {
					require.Error(t, err, string(out))
					require.NoFileExists(t, filepath.Join(dir, "tikv", "ended-at"))
				}
			})
		}
	}
}
