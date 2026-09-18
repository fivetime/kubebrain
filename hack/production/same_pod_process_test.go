package production_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSamePodProcessSeparatesReadinessFromIdentity(t *testing.T) {
	const pod = `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test","uid":"pod-uid"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":"image@sha256:fixed"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","containerID":"cri-o://process","imageID":"image@sha256:fixed","restartCount":0,"ready":true,"started":true,"state":{"running":{"startedAt":"2026-09-18T22:49:32Z"}}}]}}`
	for _, tc := range []struct {
		name, change string
		accept       bool
	}{
		{"unchanged", ".", true},
		{"isolated not ready", ".current.status.containerStatuses[0].ready=false", true},
		{"startup health flag", ".current.status.containerStatuses[0].started=false", true},
		{"status update", `.current.metadata.resourceVersion="new" | .current.status.conditions=[{type:"Ready",status:"False"}]`, true},
		{"replacement pod", `.current.metadata.uid="replacement"`, false},
		{"different name", `.current.metadata.name="brain-1"`, false},
		{"different namespace", `.current.metadata.namespace="other"`, false},
		{"deleting", `.current.metadata.deletionTimestamp="now"`, false},
		{"old deleting", `.expected.metadata.deletionTimestamp="before"`, false},
		{"different node", `.current.spec.nodeName="worker2"`, false},
		{"different spec", `.current.spec.containers[0].image="other"`, false},
		{"different IP", `.current.status.podIP="10.0.0.2"`, false},
		{"restart count", ".current.status.containerStatuses[0].restartCount=1", false},
		{"process changed", `.current.status.containerStatuses[0].containerID="cri-o://new"`, false},
		{"runtime image changed", `.current.status.containerStatuses[0].imageID="other"`, false},
		{"start time changed", `.current.status.containerStatuses[0].state.running.startedAt="later"`, false},
		{"waiting", `.current.status.containerStatuses[0].state={waiting:{reason:"CrashLoopBackOff"}}`, false},
		{"missing process", "del(.current.status.containerStatuses[0].containerID)", false},
		{"missing both identities", "del(.current.metadata.uid,.expected.metadata.uid)", false},
		{"empty both statuses", ".current.status.containerStatuses=[] | .expected.status.containerStatuses=[]", false},
		{"duplicate status", ".current.status.containerStatuses += .current.status.containerStatuses", false},
		{"fractional restarts", ".current.status.containerStatuses[0].restartCount=0.5", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := exec.Command("jq", "-n", "--argjson", "pod", pod, "{expected:$pod,current:$pod} | "+tc.change)
			input, err := fixture.CombinedOutput()
			require.NoError(t, err, string(input))
			cmd := exec.Command("jq", "-e", "-f", "same-pod-process.jq")
			cmd.Stdin = strings.NewReader(string(input))
			output, err := cmd.CombinedOutput()
			if tc.accept {
				require.NoError(t, err, string(output))
				require.Equal(t, "true", strings.TrimSpace(string(output)))
			} else {
				require.Error(t, err, string(output))
			}
		})
	}
}
