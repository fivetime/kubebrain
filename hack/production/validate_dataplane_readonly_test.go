package production_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDataplaneReadonlyProbe(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		podsJSON                string
		postPodsJSON            string
		finalPodsJSON           string
		readyz                  string
		readyzVerbose           string
		readyzExcludeData       string
		livez                   string
		livezVerbose            string
		livezExclude            string
		livezNamedVerbose       string
		healthJSON              string
		healthMethodCode        string
		healthMethodAllow       string
		httpHeaderType          string
		httpHeaderNosniff       string
		versionHeaderType       string
		serialHealthJSON        string
		count                   string
		statusJSON              string
		secondStatusJSON        string
		gatewayJSON             string
		directStatusJSON        string
		directAuthJSON          string
		authJSON                string
		directAlarmJSON         string
		alarmJSON               string
		versionJSON             string
		infoVersionJSON         string
		baselineInfoMetrics     string
		baselineInfoMetricsPod1 string
		baselineInfoMetricsPod2 string
		infoMetrics             string
		infoMetricsPod1         string
		infoMetricsPod2         string
		localStatusPod0         string
		localStatusPod1         string
		localStatusPod2         string
		clientMetrics           string
		infoDebugVars           string
		clientDebugVars         string
		debugVarsHeader         string
		infoPprof               string
		clientPprof             string
		extraEnv                []string
		wantTimeout             []string
		wantNoCommands          bool
		wantOK                  bool
		wantOutput              string
		wantNotOutput           []string
	}{
		{
			name:           "rejects ready Pod count above int32 before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"EXPECTED_READY_PODS=2147483648"},
			wantNoCommands: true,
			wantOutput:     "EXPECTED_READY_PODS must be a canonical positive int32",
		},
		{
			name:           "rejects invalid KubeBrain container name before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"KUBEBRAIN_CONTAINER_NAME=sidecar/default"},
			wantNoCommands: true,
			wantOutput:     "KUBEBRAIN_CONTAINER_NAME must be a lowercase DNS label of at most 63 characters",
		},
		{
			name:           "rejects invalid expected KubeBrain image digest before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"EXPECTED_KUBEBRAIN_IMAGE_DIGEST=sha256:ABC"},
			wantNoCommands: true,
			wantOutput:     "EXPECTED_KUBEBRAIN_IMAGE_DIGEST must be empty or a canonical sha256 digest",
		},
		{
			name:           "rejects unsafe expected KubeBrain StatefulSet UID before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{`EXPECTED_KUBEBRAIN_STATEFULSET_UID=unsafe\uid`},
			wantNoCommands: true,
			wantOutput:     "EXPECTED_KUBEBRAIN_STATEFULSET_UID contains unsupported characters",
		},
		{
			name: "rejects Ready Pods owned by another StatefulSet UID",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","ownerReferences":[{"apiVersion":"apps/v1","kind":"StatefulSet","name":"kubebrain","uid":"other-uid","controller":true}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","ownerReferences":[{"apiVersion":"apps/v1","kind":"StatefulSet","name":"kubebrain","uid":"other-uid","controller":true}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","ownerReferences":[{"apiVersion":"apps/v1","kind":"StatefulSet","name":"kubebrain","uid":"other-uid","controller":true}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_KUBEBRAIN_STATEFULSET_UID=expected-uid"},
			wantOutput: "KubeBrain Ready Pod StatefulSet ownership mismatch",
		},
		{
			name:           "rejects prefix count overflow before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"EXPECTED_PREFIX_COUNT=9223372036854775808"},
			wantNoCommands: true,
			wantOutput:     "EXPECTED_PREFIX_COUNT must be empty or a canonical non-negative int64",
		},
		{
			name:           "rejects cluster ID overflow before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=18446744073709551616"},
			wantNoCommands: true,
			wantOutput:     "EXPECTED_STATUS_CLUSTER_ID must be empty or a canonical positive uint64",
		},
		{
			name:           "rejects HashKV hash overflow before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=1", "EXPECTED_HASHKV_HASH=4294967296"},
			wantNoCommands: true,
			wantOutput:     "EXPECTED_HASHKV_HASH must be empty or a canonical non-negative uint32",
		},
		{
			name:           "rejects probe duration overflow before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"PROBE_TIMEOUT=2562048h"},
			wantNoCommands: true,
			wantOutput:     "PROBE_TIMEOUT must be a positive duration within Go time.Duration",
		},
		{
			name: "passes read only dataplane gate",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain-statefulset"},
			wantTimeout: []string{
				"10s kubectl",
				"10s curl",
				"10s go",
			},
			wantOK:     true,
			wantOutput: "kubebrain_statefulset_uid=uid-kubebrain-statefulset, kubebrain_image_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			name: "rejects duplicate Ready conditions",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"Ready","status":"False"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:ready-condition-count=2",
		},
		{
			name: "rejects Ready true with ContainersReady false",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","generation":1},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":1},{"type":"ContainersReady","status":"False","observedGeneration":1}]}},
				{"metadata":{"name":"kubebrain-1","generation":1},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":1},{"type":"ContainersReady","status":"True","observedGeneration":1}]}},
				{"metadata":{"name":"kubebrain-2","generation":1},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":1},{"type":"ContainersReady","status":"True","observedGeneration":1}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:containers-ready-status=\"False\"",
		},
		{
			name: "rejects Ready true without ContainersReady",
			podsJSON: `{"items":[
				{"preserveMissingContainersReady":true,"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:containers-ready-condition-count=0",
		},
		{
			name: "rejects duplicate ContainersReady conditions",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ContainersReady","status":"True"},{"type":"ContainersReady","status":"False"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:containers-ready-condition-count=2",
		},
		{
			name: "rejects invalid ContainersReady condition status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ContainersReady","status":7}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:containers-ready-status=7",
		},
		{
			name: "rejects stale ContainersReady condition generation",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2},{"type":"ContainersReady","status":"True","observedGeneration":1}]}},
				{"metadata":{"name":"kubebrain-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2},{"type":"ContainersReady","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2},{"type":"ContainersReady","status":"True","observedGeneration":2}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:containers-ready-observed-generation=1,current-generation=2",
		},
		{
			name: "rejects stale Ready condition generation",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":1}]}},
				{"metadata":{"name":"kubebrain-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:ready-observed-generation=1,current-generation=2",
		},
		{
			name: "rejects non-numeric Ready condition generation",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":"2"}]}},
				{"metadata":{"name":"kubebrain-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:ready-observed-generation=\"2\"",
		},
		{
			name: "rejects invalid current Pod generation for observed Ready",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","generation":"2"},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:current-generation=\"2\"",
		},
		{
			name: "rejects non-running Pod with Ready true",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"phase":"Failed","conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod lifecycle mismatch: kubebrain-0:phase=\"Failed\"",
		},
		{
			name: "rejects non-string Pod phase",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"phase":7,"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod lifecycle mismatch: kubebrain-0:phase=7",
		},
		{
			name: "rejects non-array Pod conditions",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":{"type":"Ready","status":"True"}}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:conditions-not-array",
		},
		{
			name: "rejects invalid Ready condition status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"ready"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Pod Ready condition mismatch: kubebrain-0:ready-status=\"ready\"",
		},
		{
			name: "rejects target container runtime image digest mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_KUBEBRAIN_IMAGE_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			wantOutput: "KubeBrain target container image digest mismatch: expected sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb, got sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			name: "rejects mixed target container runtime image digests",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","imageID":"docker.io/library/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","imageID":"docker.io/library/kubebrain@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","imageID":"docker.io/library/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container image digest mismatch: mixed-digests=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		{
			name: "rejects missing target container runtime image identity",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"sidecar","containerID":"containerd://sidecar-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container image digest mismatch: kubebrain-0:target-count=0",
		},
		{
			name: "rejects target container that is not ready",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":false}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container image digest mismatch: kubebrain-0:target-not-ready",
		},
		{
			name: "rejects ready target container that is waiting",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"waiting":{"reason":"ContainerCreating"}}}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container runtime state mismatch: kubebrain-0:target-state=waiting",
		},
		{
			name: "rejects ready target container that has not started",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":false,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container runtime state mismatch: kubebrain-0:target-started=false",
		},
		{
			name: "rejects ready target container that is terminated",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"terminated":{"exitCode":0}}}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container runtime state mismatch: kubebrain-0:target-state=terminated",
		},
		{
			name: "rejects ready target container without running start time",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"running":{}}}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container runtime state mismatch: kubebrain-0:target-started-at=null",
		},
		{
			name: "rejects target container image ID without canonical digest",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","imageID":"docker.io/library/kubebrain:latest","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain target container image digest mismatch: kubebrain-0:imageID-without-canonical-digest",
		},
		{
			name: "passes probe timeout to etcdctl internal timeouts",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"PROBE_TIMEOUT=60s",
				"FAKE_REQUIRE_ETCDCTL_COMMAND_TIMEOUT=1",
			},
			wantOK:     true,
			wantOutput: "status_cluster_id=123",
		},
		{
			name: "reports timed out probe without exposing arguments",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"FAKE_TIMEOUT_EXIT_CODE=124",
				"ETCDCTL_USER=root:do-not-print",
			},
			wantOutput:    "dataplane readonly probe timed out: command=kubectl timeout=10s",
			wantNotOutput: []string{"do-not-print", "kind-kubebrain-dbaas", "app.kubernetes.io/name=kubebrain"},
		},
		{
			name: "reports non-timeout probe exit without exposing arguments",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"FAKE_TIMEOUT_EXIT_CODE=42",
				"ETCDCTL_USER=root:another-secret",
			},
			wantOutput:    "dataplane readonly probe failed: command=kubectl exit_code=42",
			wantNotOutput: []string{"another-secret", "kind-kubebrain-dbaas", "app.kubernetes.io/name=kubebrain"},
		},
		{
			name: "reports prefix tool timeout through shared probe wrapper",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"FAKE_TIMEOUT_EXIT_CODE=124",
				"FAKE_TIMEOUT_MATCH=prefix-tool",
			},
			wantOutput: "dataplane readonly probe timed out: command=go timeout=10s",
		},
		{
			name: "passes authenticated gateway and client probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"ETCDCTL_USER=root:secret:with:colons",
				"FAKE_REQUIRE_GATEWAY_AUTH=1",
			},
			wantOK:     true,
			wantOutput: "gateway_auth_enabled=false",
		},
		{
			name: "passes expected etcd client certificate gateway rejection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION=1",
			},
			wantOK:     true,
			wantOutput: "gateway_client_cert_auth=expected-rejection",
		},
		{
			name:       "rejects wrong client certificate gateway status",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION=1",
				"FAKE_GATEWAY_CERT_REJECTION_RESPONSE=HTTP/1.1 401 Unauthorized",
			},
			wantOutput: "gateway client certificate auth mismatch: expected HTTP 400",
		},
		{
			name:       "rejects wrong client certificate gateway body",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION=1",
				"FAKE_GATEWAY_CERT_REJECTION_RESPONSE=HTTP/1.1 400 Bad Request",
			},
			wantOutput: "gateway client certificate auth mismatch: expected etcd CommonName rejection body",
		},
		{
			name: "reports legacy health endpoints in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOK:     true,
			wantOutput: "health=true, serializable_health=true",
		},
		{
			name: "reports livez endpoints in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_READYZ_NAMED_CHECKS=1", "EXPECTED_LIVEZ_NAMED_CHECKS=1", "EXPECTED_HEALTH_EXCLUDE_CHECKS=1", "EXPECTED_HEALTH_METHOD_CHECKS=1", "EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOK:     true,
			wantOutput: "readyz_verbose=ok, readyz_data_corruption=ok, readyz_serializable_read=ok, readyz_linearizable_read=ok, readyz_non_learner=ok, readyz_named_checks=ok, health_exclude_checks=ok, livez=ok, livez_serializable_read=ok, livez_named_checks=ok, health_method_checks=ok, http_header_checks=ok",
		},
		{
			name: "rejects unhealthy livez endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			livez:      "not ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "livez mismatch",
		},
		{
			name: "rejects malformed livez exclude endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:       "ok",
			livezExclude: "[+]serializable_read ok\nok",
			count:        "4",
			statusJSON:   `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:     []string{"EXPECTED_HEALTH_EXCLUDE_CHECKS=1"},
			wantOutput:   "livez exclude mismatch",
		},
		{
			name: "rejects malformed livez named endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			livezNamedVerbose: "ok",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_LIVEZ_NAMED_CHECKS=1"},
			wantOutput:        "livez serializable_read mismatch",
		},
		{
			name: "rejects malformed readyz verbose endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:        "ok",
			readyzVerbose: "[+]data_corruption ok\n[+]serializable_read ok\nok",
			count:         "4",
			statusJSON:    `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:      []string{"EXPECTED_READYZ_NAMED_CHECKS=1"},
			wantOutput:    "readyz verbose mismatch: expected linearizable_read ok",
		},
		{
			name: "rejects malformed readyz exclude endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			readyzExcludeData: "[+]data_corruption ok\n[+]serializable_read ok\nok",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_HEALTH_EXCLUDE_CHECKS=1"},
			wantOutput:        "readyz exclude mismatch: expected data_corruption to be excluded",
		},
		{
			name: "rejects unhealthy legacy health endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			healthJSON: `{"health":"false","reason":"RAFT NO LEADER"}`,
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "health mismatch",
		},
		{
			name: "rejects malformed health method endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			healthMethodCode: "200",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:         []string{"EXPECTED_HEALTH_METHOD_CHECKS=1"},
			wantOutput:       "livez method mismatch: expected HTTP 405",
		},
		{
			name: "rejects malformed health method allow header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			healthMethodAllow: "POST",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_HEALTH_METHOD_CHECKS=1"},
			wantOutput:        "livez method mismatch: expected Allow: GET",
		},
		{
			name: "rejects malformed health content type header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			httpHeaderType: "application/json",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:       []string{"EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOutput:     "livez header mismatch: expected Content-Type text/plain; charset=utf-8",
		},
		{
			name: "rejects missing health nosniff header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			httpHeaderNosniff: "missing",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOutput:        "livez header mismatch: expected X-Content-Type-Options nosniff",
		},
		{
			name: "rejects malformed version content type header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			versionHeaderType: "text/plain; charset=utf-8",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv:          []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOutput:        "client version header mismatch: expected Content-Type application/json",
		},
		{
			name: "reports info metrics boundary in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOK:     true,
			wantOutput: "info_metrics=ok, client_metrics=404, server_identity_metrics=ok, grpc_metrics=ok, client_request_metrics=ok, network_metrics=ok, server_stream_metrics=ok, mvcc_operation_metrics=ok, range_duration_metrics=ok, apply_duration_metrics=optional-ok, runtime_metrics=ok, fd_metrics=ok, server_state_metrics=ok, snapshot_apply_metrics=ok, raft_heartbeat_metrics=ok, slow_apply_metrics=ok, raft_proposal_metrics=ok, read_index_metrics=ok, wal_metrics=ok, raft_snapshot_file_metrics=ok, backend_commit_metrics=ok, backend_bbolt_commit_phase_metrics=ok, backend_snapshot_metrics=ok, backend_defrag_metrics=ok, health_metrics=ok, auth_metrics=ok, quota_metrics=ok, mvcc_db_size_metrics=ok, mvcc_key_metrics=ok, mvcc_put_size_metrics=ok, mvcc_pending_event_metrics=ok, mvcc_revision_metrics=ok, mvcc_compaction_metrics=ok, mvcc_watch_metrics=ok, range_stream_outcome_metrics=ok, range_stream_spill_metrics=ok, lease_metrics=ok, promhttp_metrics=ok",
		},
		{
			name: "selects leader info metrics endpoint from status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leaderId":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"memberId":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leaderId":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_server_range_duration_seconds_count{cluster=\"default\",success=\"true\"} 1\n",
				"",
				1,
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					defaultInfoMetrics(""),
					`server_id="e3f"`,
					`server_id="315"`,
					1,
				),
			},
			wantOK:     true,
			wantOutput: "info_metrics_endpoint=http://172.18.0.4:8080/metrics",
		},
		{
			name: "rejects leader term change across selected info metrics scrape",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			secondStatusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":9},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":9,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":9},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":9,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					defaultInfoMetrics(""),
					`server_id="e3f"`,
					`server_id="315"`,
					1,
				),
			},
			wantOutput: "INFO_ENDPOINTS status fence changed across info metrics scrape",
		},
		{
			name: "rejects selected leader info server identity mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + defaultInfoMetrics(""),
			},
			wantOutput: "info metrics server identity mismatch: expected exactly one etcd_server_id server_id=315 value=1",
		},
		{
			name: "rejects duplicate selected leader info server identity",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					defaultInfoMetrics(""),
					`etcd_server_id{cluster="default",server_id="e3f"} 1`,
					"etcd_server_id{cluster=\"default\",server_id=\"315\"} 1\n"+
						`etcd_server_id{cluster="default",server_id="315"} 1`,
					1,
				),
			},
			wantOutput: "info metrics server identity mismatch: expected exactly one etcd_server_id server_id=315 value=1",
		},
		{
			name: "rejects selected leader info without leader state",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					strings.Replace(defaultInfoMetrics(""), `server_id="e3f"`, `server_id="315"`, 1),
					"etcd_server_has_leader{cluster=\"default\"} 1\n",
					"etcd_server_has_leader{cluster=\"default\"} 0\n",
					1,
				),
			},
			wantOutput: "info metrics leader state mismatch: expected exactly has_leader=1,is_leader=1,is_learner=0",
		},
		{
			name: "rejects selected info endpoint that no longer reports leader",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					strings.Replace(
						defaultInfoMetrics(""),
						`server_id="e3f"`,
						`server_id="315"`,
						1,
					),
					"etcd_server_is_leader{cluster=\"default\"} 1\n",
					"etcd_server_is_leader{cluster=\"default\"} 0\n",
					1,
				),
			},
			wantOutput: "info metrics leader state mismatch: expected exactly has_leader=1,is_leader=1,is_learner=0",
		},
		{
			name: "rejects selected leader info reporting learner state",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					strings.Replace(defaultInfoMetrics(""), `server_id="e3f"`, `server_id="315"`, 1),
					"etcd_server_is_learner{cluster=\"default\"} 0\n",
					"etcd_server_is_learner{cluster=\"default\"} 1\n",
					1,
				),
			},
			wantOutput: "info metrics leader state mismatch: expected exactly has_leader=1,is_leader=1,is_learner=0",
		},
		{
			name: "rejects duplicate selected leader state metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":789,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
			]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"789","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
				"FAKE_LEADER_INFO_METRICS_URL=http://172.18.0.4:8080/metrics",
				"FAKE_LEADER_INFO_METRICS=" + strings.Replace(
					strings.Replace(defaultInfoMetrics(""), `server_id="e3f"`, `server_id="315"`, 1),
					`etcd_server_is_leader{cluster="default"} 1`,
					"etcd_server_is_leader{cluster=\"default\"} 1\n"+
						`etcd_server_is_leader{cluster="default"} 1`,
					1,
				),
			},
			wantOutput: "info metrics leader state mismatch: expected exactly has_leader=1,is_leader=1,is_learner=0",
		},
		{
			name: "rejects incomplete status leader reports for info endpoint selection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"leader":789}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"version":"3.7.0","dbSize":99}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
			},
			wantOutput: "INFO_ENDPOINTS leader selection requires every Status response to report a leader: expected 2, got 1",
		},
		{
			name: "rejects divergent status leaders for info endpoint selection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"leader":456}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"version":"3.7.0","dbSize":99,"leader":789}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
			},
			wantOutput: "INFO_ENDPOINTS leader selection requires one shared positive Status leader ID, got 456,789",
		},
		{
			name: "rejects status leader outside info endpoint member set",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"leader":999}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"version":"3.7.0","dbSize":99,"leader":999}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.4:8080",
			},
			wantOutput: "INFO_ENDPOINTS leader selection could not map Status leader 999 to exactly one member endpoint",
		},
		{
			name: "rejects missing info server version metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: "etcd_cluster_version{cluster=\"default\",cluster_version=\"3.7\"} 1\ngrpc_server_handled_total{grpc_code=\"OK\"} 1\n",
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput:  "info metrics mismatch: expected etcd_server_version server_version=3.7.0",
		},
		{
			name: "rejects missing info network known peers metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_network_known_peers{Local=\"abc\",Remote=\"abc\"} 1\n",
				"etcd_network_client_grpc_received_bytes_total{cluster=\"default\"} 0\n"+
					"etcd_network_client_grpc_sent_bytes_total{cluster=\"default\"} 0\n",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_network_known_peers",
		},
		{
			name: "rejects missing info client grpc received bytes metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_network_client_grpc_received_bytes_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_network_client_grpc_received_bytes_total",
		},
		{
			name: "rejects missing info client grpc sent bytes metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_network_client_grpc_sent_bytes_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_network_client_grpc_sent_bytes_total",
		},
		{
			name: "rejects missing info client request metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_server_client_requests_total{client_api_version=\"unknown\",cluster=\"default\",type=\"unary\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_client_requests_total",
		},
		{
			name: "rejects missing info mvcc operation metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_mvcc_put_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_mvcc_put_total",
		},
		{
			name:       "rejects missing info range duration metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_range_duration_seconds_count{cluster=\"default\",success=\"true\"} 1\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_range_duration_seconds_count",
		},
		{
			name:       "accepts absent optional apply duration metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_apply_duration_seconds_count{cluster=\"default\",op=\"Put\",success=\"true\",version=\"v3\"} 1\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOK:     true,
			wantOutput: "apply_duration_metrics=optional-ok",
		},
		{
			name:       "rejects malformed info apply duration metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_apply_duration_seconds_count{cluster=\"default\",op=\"Put\",success=\"true\",version=\"v3\"} 1\n",
				"etcd_server_apply_duration_seconds_count{cluster=\"default\",op=\"Range\",success=\"true\",version=\"v2\"} 1\n", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: invalid etcd_server_apply_duration_seconds_count labels or value",
		},
		{
			name: "rejects missing info server stream failure metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.NewReplacer(
				"etcd_network_server_stream_failures_total{API=\"watch\",Type=\"receive\",cluster=\"default\"} 0\n", "",
				"etcd_network_server_stream_failures_total{API=\"watch\",Type=\"send\",cluster=\"default\"} 0\n", "",
				"etcd_network_server_stream_failures_total{API=\"lease-keepalive\",Type=\"receive\",cluster=\"default\"} 0\n", "",
				"etcd_network_server_stream_failures_total{API=\"lease-keepalive\",Type=\"send\",cluster=\"default\"} 0\n", "",
			).Replace(defaultInfoMetrics("")),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_network_server_stream_failures_total",
		},
		{
			name: "rejects missing info go runtime metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_keys_total{cluster="default"} 4`,
				`etcd_debugging_mvcc_total_put_size_in_bytes{cluster="default"} 0`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_db_compaction_last{cluster="default"} 0`,
				`etcd_debugging_mvcc_db_compaction_keys_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_events_total{cluster="default"} 0`,
				`watch_range_prefilter_dropped{cluster="default"} 0`,
				`backend_list_by_stream_failed{cluster="default"} 0`,
				`backend_list_by_stream_canceled{cluster="default"} 0`,
				`backend_list_by_stream_limit_satisfied{cluster="default"} 0`,
				`backend_range_stream_spill_active{cluster="default"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="latest_metadata"} 0`,
				`backend_range_stream_spill_wait_seconds_count{cluster="default",path="decoded"} 0`,
				`backend_range_stream_spill_wait_seconds_count{cluster="default",path="latest_metadata"} 0`,
				`etcd_debugging_mvcc_pending_events_total{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected go_info",
		},
		{
			name: "rejects missing info server identity metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_keys_total{cluster="default"} 4`,
				`etcd_debugging_mvcc_total_put_size_in_bytes{cluster="default"} 0`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_db_compaction_last{cluster="default"} 0`,
				`etcd_debugging_mvcc_db_compaction_keys_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_events_total{cluster="default"} 0`,
				`watch_range_prefilter_dropped{cluster="default"} 0`,
				`backend_range_stream_spill_active{cluster="default"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="latest_metadata"} 0`,
				`backend_range_stream_spill_wait_seconds_count{cluster="default",path="decoded"} 0`,
				`backend_range_stream_spill_wait_seconds_count{cluster="default",path="latest_metadata"} 0`,
				`etcd_debugging_mvcc_pending_events_total{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_go_version server_go_version=go*",
		},
		{
			name: "rejects exposed client metrics endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:        "ok",
			count:         "4",
			statusJSON:    `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			clientMetrics: "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nmetrics\n",
			extraEnv:      []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput:    "client metrics mismatch: expected HTTP 404",
		},
		{
			name: "rejects missing info go scheduler metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected go_sched_gomaxprocs_threads",
		},
		{
			name: "rejects missing info go gc metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected go_gc_gogc_percent",
		},
		{
			name: "rejects missing info grpc started metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected grpc_server_started_total",
		},
		{
			name: "rejects missing info promhttp metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_keys_total{cluster="default"} 4`,
				`etcd_debugging_mvcc_total_put_size_in_bytes{cluster="default"} 0`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_db_compaction_last{cluster="default"} 0`,
				`etcd_debugging_mvcc_db_compaction_keys_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_events_total{cluster="default"} 0`,
				`watch_range_prefilter_dropped{cluster="default"} 0`,
				`backend_range_stream_spill_active{cluster="default"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="decoded"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="latest_metadata"} 0`,
				`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="latest_metadata"} 0`,
				`backend_range_stream_spill_wait_seconds_count{cluster="default",path="decoded"} 0`,
				`backend_range_stream_spill_wait_seconds_count{cluster="default",path="latest_metadata"} 0`,
				`etcd_debugging_mvcc_pending_events_total{cluster="default"} 0`,
				`etcd_debugging_server_lease_expired_total{cluster="default"} 0`,
				`etcd_debugging_lease_granted_total{cluster="default"} 0`,
				`etcd_debugging_lease_revoked_total{cluster="default"} 0`,
				`etcd_debugging_lease_renewed_total{cluster="default"} 0`,
				`backend_list_by_stream_failed{cluster="default"} 0`,
				`backend_list_by_stream_canceled{cluster="default"} 0`,
				`backend_list_by_stream_limit_satisfied{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected promhttp_metric_handler_requests_total",
		},
		{
			name: "rejects missing info fd metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected os_fd_used",
		},
		{
			name: "rejects missing info server state metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_is_leader",
		},
		{
			name: "rejects missing info leader changes metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_server_leader_changes_seen_total{cluster=\"default\"} 1\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_leader_changes_seen_total",
		},
		{
			name:       "rejects missing info learner promote successes metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_learner_promote_successes{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_learner_promote_successes",
		},
		{
			name: "rejects missing info snapshot apply metric",
			podsJSON: `{"items":[
					{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
					{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
					{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
				]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_server_snapshot_apply_in_progress_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_snapshot_apply_in_progress_total",
		},
		{
			name: "rejects missing info raft heartbeat metric",
			podsJSON: `{"items":[
					{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
					{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
					{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
				]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_server_heartbeat_send_failures_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_heartbeat_send_failures_total",
		},
		{
			name:       "rejects missing info slow apply metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_slow_apply_total{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_slow_apply_total to remain 0",
		},
		{
			name:       "rejects nonzero info slow apply metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_slow_apply_total{cluster=\"default\"} 0", "etcd_server_slow_apply_total{cluster=\"default\"} 1", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_slow_apply_total to remain 0",
		},
		{
			name:       "rejects missing info WAL fsync metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_disk_wal_fsync_duration_seconds_count{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_disk_wal_fsync_duration_seconds_count to remain 0",
		},
		{
			name:       "rejects nonzero info WAL bytes metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_disk_wal_write_bytes_total{cluster=\"default\"} 0", "etcd_disk_wal_write_bytes_total{cluster=\"default\"} 1", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_disk_wal_write_bytes_total to remain 0",
		},
		{
			name:       "rejects missing info raft snapshot marshalling metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_debugging_snap_save_marshalling_duration_seconds_count{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_snap_save_marshalling_duration_seconds_count to remain 0",
		},
		{
			name:       "rejects nonzero info raft snapshot db fsync metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_snap_db_fsync_duration_seconds_count{cluster=\"default\"} 0", "etcd_snap_db_fsync_duration_seconds_count{cluster=\"default\"} 1", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_snap_db_fsync_duration_seconds_count to remain 0",
		},
		{
			name:       "rejects missing info raft proposals committed metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_proposals_committed_total{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_proposals_committed_total",
		},
		{
			name:       "rejects missing info raft proposals applied metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_proposals_applied_total{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_proposals_applied_total",
		},
		{
			name:       "rejects missing info raft proposals pending metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_proposals_pending{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_proposals_pending",
		},
		{
			name:       "rejects missing info raft proposals failed metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_proposals_failed_total{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_proposals_failed_total",
		},
		{
			name:       "rejects missing info read index metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_server_read_indexes_failed_total{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_read_indexes_failed_total",
		},
		{
			name:       "rejects missing info backend commit metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_disk_backend_commit_duration_seconds_count{cluster=\"default\"} 1\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_disk_backend_commit_duration_seconds_count",
		},
		{
			name:       "rejects missing info bbolt commit phase metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_disk_backend_commit_spill_duration_seconds_count to remain 0",
		},
		{
			name:       "rejects nonzero info bbolt commit phase metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster=\"default\"} 0\n",
				"etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster=\"default\"} 1\n", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_disk_backend_commit_write_duration_seconds_count to remain 0",
		},
		{
			name:       "rejects missing info backend snapshot metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_disk_backend_snapshot_duration_seconds_count{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_disk_backend_snapshot_duration_seconds_count",
		},
		{
			name:       "rejects missing info backend defrag duration metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_disk_backend_defrag_duration_seconds_count{cluster=\"default\"} 0\n", "", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_disk_backend_defrag_duration_seconds_count",
		},
		{
			name:       "rejects nonzero info defrag inflight metric",
			podsJSON:   `{"items":[{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(defaultInfoMetrics(""),
				"etcd_disk_defrag_inflight{cluster=\"default\"} 0\n",
				"etcd_disk_defrag_inflight{cluster=\"default\"} 1\n", 1),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_disk_defrag_inflight to remain 0",
		},
		{
			name: "rejects missing info health metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_health_failures",
		},
		{
			name: "rejects missing info auth revision metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_auth_revision",
		},
		{
			name: "rejects missing info quota metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_quota_backend_bytes",
		},
		{
			name: "rejects missing info mvcc db size metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_mvcc_db_total_size_in_use_in_bytes",
		},
		{
			name: "rejects missing info mvcc current revision metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_current_revision",
		},
		{
			name: "rejects missing info mvcc open read transaction metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_mvcc_db_open_read_transactions{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_mvcc_db_open_read_transactions",
		},
		{
			name: "rejects missing info mvcc compact revision metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_keys_total{cluster="default"} 4`,
				`etcd_debugging_mvcc_total_put_size_in_bytes{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_compact_revision",
		},
		{
			name: "rejects missing info mvcc watch stream metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
				`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
				`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
				`etcd_mvcc_range_total{cluster="default"} 1`,
				`etcd_mvcc_put_total{cluster="default"} 0`,
				`etcd_mvcc_delete_total{cluster="default"} 0`,
				`etcd_mvcc_txn_total{cluster="default"} 0`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_learner_promote_successes{cluster="default"} 0`,
				`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
				`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
				`etcd_server_proposals_committed_total{cluster="default"} 0`,
				`etcd_server_proposals_applied_total{cluster="default"} 0`,
				`etcd_server_proposals_pending{cluster="default"} 0`,
				`etcd_server_proposals_failed_total{cluster="default"} 0`,
				`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
				`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
				`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
				`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
				`etcd_disk_defrag_inflight{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
				`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_keys_total{cluster="default"} 4`,
				`etcd_debugging_mvcc_total_put_size_in_bytes{cluster="default"} 0`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_db_compaction_last{cluster="default"} 0`,
				`etcd_debugging_mvcc_db_compaction_keys_total{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_watch_stream_total",
		},
		{
			name: "rejects missing info mvcc db compaction last metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_mvcc_db_compaction_last{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_db_compaction_last",
		},
		{
			name: "rejects missing info mvcc db compaction keys metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_mvcc_db_compaction_keys_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_db_compaction_keys_total",
		},
		{
			name: "rejects missing info mvcc keys metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_mvcc_keys_total{cluster=\"default\"} 4\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_keys_total",
		},
		{
			name: "rejects missing info mvcc put size metric",
			podsJSON: `{"items":[
					{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
					{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
					{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
				]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_mvcc_total_put_size_in_bytes{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_total_put_size_in_bytes",
		},
		{
			name: "rejects missing info mvcc watch event metric",
			podsJSON: `{"items":[
					{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_mvcc_events_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_events_total",
		},
		{
			name: "rejects missing info watch range prefilter metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"watch_range_prefilter_dropped{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected watch_range_prefilter_dropped",
		},
		{
			name: "rejects missing info range stream limit outcome metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"backend_list_by_stream_limit_satisfied{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: backend list stream outcomes must contain failed, canceled, and limit_satisfied",
		},
		{
			name: "rejects duplicate info range stream limit outcome metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: defaultInfoMetrics("") +
				"backend_list_by_stream_limit_satisfied{cluster=\"duplicate\"} 0\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: backend list stream outcomes must contain failed, canceled, and limit_satisfied",
		},
		{
			name: "rejects negative info range stream outcome metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"backend_list_by_stream_canceled{cluster=\"default\"} 0\n",
				"backend_list_by_stream_canceled{cluster=\"default\"} -1\n",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: backend list stream outcomes must contain failed, canceled, and limit_satisfied",
		},
		{
			name: "rejects missing info range stream spill active metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"backend_range_stream_spill_active{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected backend_range_stream_spill_active",
		},
		{
			name: "rejects invalid info range stream spill active metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"backend_range_stream_spill_active{cluster=\"default\"} 0\n",
				"backend_range_stream_spill_active{cluster=\"default\"} 2\n",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: backend_range_stream_spill_active must be exactly one 0 or 1 sample, got 2",
		},
		{
			name: "rejects missing info range stream spill outcome",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"backend_range_stream_spill_outcome{cluster=\"default\",outcome=\"failed\",path=\"latest_metadata\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: backend_range_stream_spill_outcome must contain exactly the two paths and four bounded outcomes",
		},
		{
			name: "rejects missing info range stream spill wait path",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"backend_range_stream_spill_wait_seconds_count{cluster=\"default\",path=\"latest_metadata\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: backend_range_stream_spill_wait_seconds_count must contain exactly decoded and latest_metadata paths",
		},
		{
			name: "rejects missing info mvcc pending events metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_mvcc_pending_events_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_pending_events_total",
		},
		{
			name: "rejects missing info lease expired metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_server_lease_expired_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_server_lease_expired_total",
		},
		{
			name: "rejects missing info lease granted metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_debugging_lease_granted_total{cluster=\"default\"} 0\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_lease_granted_total",
		},
		{
			name: "reports info debug vars boundary in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOK:     true,
			wantOutput: "info_debug_vars=ok, client_debug_vars=404",
		},
		{
			name: "rejects malformed info debug vars JSON",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:        "ok",
			count:         "4",
			statusJSON:    `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			infoDebugVars: `{"cmdline":"kubebrain","memstats":{}}`,
			extraEnv:      []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:    "info debug vars mismatch: expected cmdline array",
		},
		{
			name: "rejects exposed client debug vars endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			clientDebugVars: "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n\r\n{\"cmdline\":[],\"memstats\":{}}\n",
			extraEnv:        []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:      "client debug vars mismatch: expected HTTP 404",
		},
		{
			name: "rejects malformed info debug vars content type",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			debugVarsHeader: "application/json",
			extraEnv:        []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:      "info debug vars header mismatch: expected Content-Type application/json; charset=utf-8",
		},
		{
			name: "rejects malformed info debug vars method guard",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			healthMethodCode: "200",
			extraEnv:         []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:       "info debug vars method mismatch: expected HTTP 405",
		},
		{
			name: "reports pprof disabled boundary in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=1"},
			wantOK:     true,
			wantOutput: "client_pprof=404, info_pprof=404",
		},
		{
			name: "rejects exposed info pprof endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			infoPprof:  "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<html>pprof</html>\n",
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=1"},
			wantOutput: "info pprof mismatch: expected HTTP 404",
		},
		{
			name: "rejects exposed client pprof endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			clientPprof: "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n" +
				"<html>pprof</html>\n",
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=1"},
			wantOutput: "client pprof mismatch: expected HTTP 404",
		},
		{
			name: "rejects unhealthy serializable health endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			serialHealthJSON: `{"health":"false","reason":"ALARM NOSPACE"}`,
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput:       "serializable health mismatch",
		},
		{
			name: "passes camelcase diagnostic envelopes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"clusterId":123,"memberId":456,"revision":7},"db_size":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"clusterId":123,"memberId":456,"revision":7},"hash":111,"compactRevision":3}}]`,
			},
			wantOK:     true,
			wantOutput: "dataplane readonly gate passed",
		},
		{
			name: "reports pinned status version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
			},
			wantOK:     true,
			wantOutput: "status_version=3.7.0",
		},
		{
			name: "reports status storage version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","storageVersion":"3.6.0","dbSize":99}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.6.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.6.0", 2147483648, false, ""),
			versionJSON:      `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			infoVersionJSON:  `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:           true,
			wantOutput:       "status_storage_versions=3.6.0",
		},
		{
			name: "reports snakecase status storage version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","storage_version":"3.6.0","dbSize":99}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.6.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.6.0", 2147483648, false, ""),
			versionJSON:      `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			infoVersionJSON:  `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:           true,
			wantOutput:       "status_storage_versions=3.6.0",
		},
		{
			name: "reports full status storage version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","storageVersion":"3.7.0","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_storage_versions=3.7.0",
		},
		{
			name: "reports status db size quota in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"dbSizeQuota":2147483648}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size_quota=2147483648",
		},
		{
			name: "reports snakecase status db size quota in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"db_size_quota":2147483648}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size_quota=2147483648",
		},
		{
			name: "accepts disabled status db size quota sentinel",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"dbSizeQuota":-1}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"-1","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.7.0", -1, false, ""),
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:           true,
			wantOutput:       "min_status_db_size_quota=-1",
		},
		{
			name: "reports status learner envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"isLearner":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_is_learners=false",
		},
		{
			name: "reports snakecase status learner envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"is_learner":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_is_learners=false",
		},
		{
			name: "reports status downgrade info in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgradeInfo":{"enabled":true,"targetVersion":"3.6.0"}}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{"enabled":true,"targetVersion":"3.6.0"}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.7.0", 2147483648, true, "3.6.0"),
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:           true,
			wantOutput:       "status_downgrade_enableds=true, status_downgrade_target_versions=3.6.0",
		},
		{
			name: "reports snakecase status downgrade info in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgrade_info":{"enabled":true,"target_version":"3.6.0"}}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{"enabled":true,"targetVersion":"3.6.0"}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.7.0", 2147483648, true, "3.6.0"),
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:           true,
			wantOutput:       "status_downgrade_enableds=true, status_downgrade_target_versions=3.6.0",
		},
		{
			name: "allows disabled status downgrade info without target",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgradeInfo":{"enabled":false}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_downgrade_enableds=false",
		},
		{
			name: "allows idle protojson status downgrade info object",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgradeInfo":{}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_errors=empty",
		},
		{
			name: "reports gateway status envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
			},
			wantOK:     true,
			wantOutput: "version_etcdserver=3.7.0, version_etcdcluster=3.7, version_storage=3.7.0, info_version_storage=3.7.0, gateway_db_size_in_use=88, gateway_db_size_quota=2147483648, gateway_is_learner=false, gateway_leader_id=456, gateway_raft_term=8, gateway_raft_index=7, gateway_raft_applied_index=7, gateway_raft_indexes_sampled=true, gateway_downgrade_info=object, gateway_status_body_match=true, gateway_status_errors=empty, direct_status_endpoint_identity_match=true, direct_status_is_learner_match=true, direct_status_v36_fields_match=true, direct_status_errors=empty",
		},
		{
			name: "rejects malformed version storage envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			versionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":false}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:  "/version storage must be a storage semver string",
		},
		{
			name: "rejects version storage drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			versionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:  "/version storage mismatch",
		},
		{
			name: "rejects info version storage drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			infoVersionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:      "info /version storage mismatch",
		},
		{
			name: "reports gateway auth status envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":"5"}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "gateway_auth_enabled=false, gateway_auth_revision=5, gateway_auth_status_match=true",
		},
		{
			name: "normalizes omitted auth status scalars on both paths",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAuthJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8}}`,
			authJSON:       `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"}}`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:         true,
			wantOutput:     "gateway_auth_enabled=false, gateway_auth_status_match=true",
		},
		{
			name: "rejects gateway auth enabled mismatch with direct endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAuthJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"enabled":true,"authRevision":5}`,
			authJSON:       `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":"5"}`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:     "gateway auth status enabled mismatch with direct endpoint",
		},
		{
			name: "rejects gateway auth revision mismatch with direct endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAuthJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"authRevision":5}`,
			authJSON:       `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":"6"}`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:     "gateway auth status authRevision mismatch with direct endpoint",
		},
		{
			name: "rejects malformed direct auth enabled envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAuthJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"enabled":"false","authRevision":5}`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:     "direct auth status enabled must be boolean",
		},
		{
			name: "rejects direct auth revision uint64 overflow",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAuthJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"authRevision":"18446744073709551616"}`,
			extraEnv:       []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:     "direct auth status authRevision must be non-negative uint64",
		},
		{
			name: "reports gateway alarm envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "direct_alarms=empty, gateway_alarms=empty, gateway_alarm_match=true",
		},
		{
			name: "rejects non empty direct alarm list",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAlarmJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"alarms":[{"memberID":456,"alarm":"NOSPACE"}]}`,
			alarmJSON:       `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"}}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:      "direct alarm list must be empty",
		},
		{
			name: "rejects malformed direct alarm envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAlarmJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"alarms":false}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:      "direct alarm alarms must be an array",
		},
		{
			name: "rejects direct alarm serving member mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			directAlarmJSON: `{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8}}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:      "direct alarm serving member mismatch",
		},
		{
			name: "rejects malformed gateway alarm envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"alarms":false}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway alarm alarms must be an array",
		},
		{
			name: "rejects non empty gateway alarm list",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"alarms":[{"memberID":"456","alarm":"NOSPACE"}]}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway alarm list must be empty",
		},
		{
			name: "rejects malformed gateway auth enabled envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"enabled":"false","authRevision":"5"}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway auth status enabled must be boolean",
		},
		{
			name: "rejects malformed gateway auth revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":false}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway auth status authRevision must be non-negative",
		},
		{
			name: "rejects malformed gateway status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":0,"isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status dbSizeQuota must be a non-zero integer",
		},
		{
			name: "accepts disabled gateway status db size quota sentinel",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"-1","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.7.0", -1, false, ""),
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:           true,
			wantOutput:       "gateway_db_size_quota=-1",
		},
		{
			name: "rejects malformed gateway status learner envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":"false","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status isLearner must be boolean",
		},
		{
			name: "accepts independently sampled gateway db size inversion",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"100","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      true,
			wantOutput:  "gateway_db_size_in_use=100",
		},
		{
			name: "accepts independently sampled gateway raft index inversion",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"8","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      true,
			wantOutput:  "gateway_raft_applied_index=8, gateway_raft_indexes_sampled=true",
		},
		{
			name: "rejects malformed gateway status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":false,"dbSize":"99","dbSizeInUse":"88","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status storageVersion must be a storage semver string",
		},
		{
			name: "rejects noncanonical storage version on every status surface",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.1","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.1","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("3.7.0", "3.7.1", 2147483648, false, ""),
			versionJSON:      `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.7.1"}`,
			infoVersionJSON:  `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.7.1"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:       "status storageVersion envelope invalid",
		},
		{
			name: "rejects versioned status fields before etcd 3.4",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.3.0","dbSize":99,"dbSizeInUse":88,"isLearner":false,"errors":[],"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.3.0","dbSize":"99","dbSizeInUse":"88","isLearner":false,"errors":[],"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7"}`,
			directStatusJSON: statusProbeJSON("3.3.0", "", 0, false, ""),
			versionJSON:      `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			infoVersionJSON:  `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.3.0"},
			wantOutput:       "status 3.4 fields are unavailable before etcd 3.4",
		},
		{
			name: "rejects versioned gateway status fields before etcd 3.4",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.3.0","dbSize":99,"leader":456,"raftTerm":8,"raftIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.3.0","dbSize":"99","dbSizeInUse":"88","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7"}`,
			directStatusJSON: pre34StatusProbeJSON("3.3.0"),
			versionJSON:      `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			infoVersionJSON:  `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.3.0"},
			wantOutput:       "gateway status 3.4 fields are unavailable before etcd 3.4",
		},
		{
			name: "rejects versioned raw status fields before etcd 3.4",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.3.0","dbSize":99,"leader":456,"raftTerm":8,"raftIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.3.0","dbSize":"99","leader":"456","raftTerm":"8","raftIndex":"7"}`,
			directStatusJSON: statusProbeJSON("3.3.0", "", 0, false, ""),
			versionJSON:      `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			infoVersionJSON:  `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.3.0"},
			wantOutput:       "raw status versioned fields are unavailable before etcd 3.4",
		},
		{
			name: "rejects versioned status fields before etcd 3.6",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.5.0","dbSize":99,"storageVersion":"3.5.0","dbSizeInUse":88,"dbSizeQuota":2147483648,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.5.0","storageVersion":"3.5.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("3.5.0", "3.5.0", 2147483648, false, ""),
			versionJSON:      `{"etcdserver":"3.5.0","etcdcluster":"3.5","storage":"3.5.0"}`,
			infoVersionJSON:  `{"etcdserver":"3.5.0","etcdcluster":"3.5","storage":"3.5.0"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.5.0"},
			wantOutput:       "status 3.6 fields are unavailable before etcd 3.6",
		},
		{
			name: "rejects malformed gateway status downgrade info envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":false}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status downgradeInfo envelope invalid",
		},
		{
			name: "rejects gateway status quota drift hidden by old etcdctl",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"123","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				`FAKE_DIRECT_STATUS_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":7,"errors":[],"db_size_in_use":88,"is_learner":false,"storage_version":"3.7.0","db_size_quota":2147483648,"downgrade_info":{"enabled":false,"target_version":""}}`,
			},
			wantOutput: "raw/gateway status dbSizeQuota mismatch",
		},
		{
			name: "rejects gateway status learner drift hidden by old etcdctl",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":7,"errors":[],"db_size_in_use":88,"is_learner":true,"storage_version":"3.7.0","db_size_quota":2147483648,"downgrade_info":{"enabled":false,"target_version":""}}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:       "raw/gateway status isLearner mismatch",
		},
		{
			name: "rejects non empty raw status errors",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{},"errors":[]}`,
			directStatusJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":7,"errors":["alarm:NOSPACE"],"db_size_in_use":88,"is_learner":false,"storage_version":"3.7.0","db_size_quota":2147483648,"downgrade_info":{"enabled":false,"target_version":""}}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:       "raw status errors must be empty",
		},
		{
			name: "accepts omitted false gateway status learner for etcd 3.4",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.4.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.4.0","dbSize":"99","dbSizeInUse":"88","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7"}`,
			directStatusJSON: statusProbeJSON("3.4.0", "", 0, false, ""),
			versionJSON:      `{"etcdserver":"3.4.0","etcdcluster":"3.4","storage":"unknown"}`,
			infoVersionJSON:  `{"etcdserver":"3.4.0","etcdcluster":"3.4","storage":"unknown"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.4.0"},
			wantOK:           true,
			wantOutput:       "direct_status_is_learner_match=true",
		},
		{
			name: "allows missing gateway status learner before etcd 3.4",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.3.0","dbSize":99,"leader":456,"raftTerm":8,"raftIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.3.0","dbSize":"99","leader":"456","raftTerm":"8","raftIndex":"7"}`,
			directStatusJSON: pre34StatusProbeJSON("3.3.0"),
			versionJSON:      `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			infoVersionJSON:  `{"etcdserver":"3.3.0","etcdcluster":"3.3","storage":"unknown"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.3.0"},
			wantOK:           true,
			wantOutput:       "direct_status_is_learner_match=not-required",
		},
		{
			name: "rejects gateway status downgrade drift hidden by old etcdctl",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{"enabled":true,"targetVersion":"3.6.0"}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				`FAKE_DIRECT_STATUS_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":7,"errors":[],"db_size_in_use":88,"is_learner":false,"storage_version":"3.7.0","db_size_quota":2147483648,"downgrade_info":{"enabled":false,"target_version":""}}`,
			},
			wantOutput: "raw/gateway status downgradeInfo mismatch",
		},
		{
			name: "rejects gateway status storage drift hidden by old etcdctl",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:     `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.6.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			versionJSON:     `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			infoVersionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				`FAKE_DIRECT_STATUS_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":7,"errors":[],"db_size_in_use":88,"is_learner":false,"storage_version":"3.7.0","db_size_quota":2147483648,"downgrade_info":{"enabled":false,"target_version":""}}`,
			},
			wantOutput: "raw/gateway status storageVersion mismatch",
		},
		{
			name: "rejects status quota drift for oversized compatible version",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"9223372036854775808.0.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"9223372036854775808.0.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"123","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("9223372036854775808.0.0", "3.7.0", 2147483648, false, ""),
			versionJSON:      `{"etcdserver":"9223372036854775808.0.0","etcdcluster":"9223372036854775808.0","storage":"3.7.0"}`,
			infoVersionJSON:  `{"etcdserver":"9223372036854775808.0.0","etcdcluster":"9223372036854775808.0","storage":"3.7.0"}`,
			extraEnv:         []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:       "raw/gateway status dbSizeQuota mismatch",
		},
		{
			name: "reports status leader and raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"leader":456,"raftTerm":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_leader_ids=456, status_raft_terms=8",
		},
		{
			name: "reports snakecase status raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raftTerm":8},"dbSize":99,"leader_id":456,"raft_term":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_leader_ids=456, status_raft_terms=8",
		},
		{
			name: "reports camelcase status leader in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"leaderId":456,"raftTerm":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_leader_ids=456, status_raft_terms=8",
		},
		{
			name: "reports status raft indexes in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":9},"dbSize":99,"raftIndex":9,"raftAppliedIndex":9}}]`,
			gatewayJSON:     `{"header":{"cluster_id":"123","member_id":"456","revision":"9","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"9","raftAppliedIndex":"9","downgradeInfo":{}}`,
			directAuthJSON:  `{"header":{"cluster_id":123,"member_id":456,"revision":9,"raft_term":8},"authRevision":5}`,
			authJSON:        `{"header":{"cluster_id":"123","member_id":"456","revision":"9","raft_term":"8"},"authRevision":"5"}`,
			directAlarmJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":9,"raft_term":8}}`,
			alarmJSON:       `{"header":{"cluster_id":"123","member_id":"456","revision":"9","raft_term":"8"}}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:          true,
			wantOutput:      "min_status_raft_index=9, min_status_raft_applied_index=9, raft_indexes_sampled=true",
		},
		{
			name: "reports snakecase status raft indexes in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":9},"dbSize":99,"raft_index":9,"raft_applied_index":9}}]`,
			gatewayJSON:     `{"header":{"cluster_id":"123","member_id":"456","revision":"9","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"9","raftAppliedIndex":"9","downgradeInfo":{}}`,
			directAuthJSON:  `{"header":{"cluster_id":123,"member_id":456,"revision":9,"raft_term":8},"authRevision":5}`,
			authJSON:        `{"header":{"cluster_id":"123","member_id":"456","revision":"9","raft_term":"8"},"authRevision":"5"}`,
			directAlarmJSON: `{"header":{"cluster_id":123,"member_id":456,"revision":9,"raft_term":8}}`,
			alarmJSON:       `{"header":{"cluster_id":"123","member_id":"456","revision":"9","raft_term":"8"}}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:          true,
			wantOutput:      "min_status_raft_index=9, min_status_raft_applied_index=9, raft_indexes_sampled=true",
		},
		{
			name: "reports status db size in use in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":88}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size=99, min_status_db_size_in_use=88",
		},
		{
			name: "reports snakecase status db size in use in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"db_size":99,"db_size_in_use":88}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size=99, min_status_db_size_in_use=88",
		},
		{
			name: "reports empty status errors in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_errors=empty",
		},
		{
			name: "reports uppercase empty status errors in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"Errors":[]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_errors=empty",
		},
		{
			name: "reports hashkv raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
			},
			wantOK:     true,
			wantOutput: "revisions_match=true, hashkv_raft_terms=8, raft_terms_match=true",
		},
		{
			name: "reports camelcase hashkv raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raftTerm":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raftTerm":8},"hash":111,"compact_revision":3}}]`,
			},
			wantOK:     true,
			wantOutput: "hashkv_raft_terms=8, raft_terms_match=true",
		},
		{
			name: "rejects missing hashkv raft term when status reports term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv raft term must be present on all endpoints when Status reports raft term",
		},
		{
			name: "rejects partial hashkv raft term coverage",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"dbSize":99}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv raft term must be present on all endpoints when present",
		},
		{
			name: "reports gateway hash and hashkv envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":222}`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"111","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOK:     true,
			wantOutput: "direct_hash=222, gateway_hash=222, gateway_hash_match=true, gateway_hashkv_hash=111, gateway_hashkv_hash_revision=7, gateway_hashkv_compact_revision=3, gateway_hashkv_revisions_match=true, gateway_hashkv_body_match=true",
		},
		{
			name: "rejects gateway hashkv compact revision from another endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":5,"hash_revision":7}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}
				]`,
				`FAKE_DIRECT_HASHKV_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":5,"hash_revision":7}`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"111","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv compact revision mismatch with direct endpoint",
		},
		{
			name: "rejects gateway hash mismatch with direct endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_DIRECT_HASH_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111}`,
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":222}`,
			},
			wantOutput: "gateway hash mismatch with direct endpoint",
		},
		{
			name: "rejects gateway hash uint32 overflow",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"4294967296"}`,
			},
			wantOutput: "gateway hash must be a non-negative uint32",
		},
		{
			name: "reports post hash mvcc metrics in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain-statefulset",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOK:     true,
			wantOutput: "mvcc_hash_metrics=ok, mvcc_hash_count_delta=3, mvcc_hash_rev_count_delta=3, hashkv_cache_metrics=ok, hashkv_cache_hit_delta=3, hashkv_cache_miss_delta=3, hashkv_cache_process_identity=stable",
		},
		{
			name: "rejects missing post hashkv completed cache hit metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"# TYPE backend_hashkv_completed_cache_hit counter\nbackend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: expected backend_hashkv_completed_cache_hit counter",
		},
		{
			name: "rejects non counter post hashkv completed cache metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"# TYPE backend_hashkv_completed_cache_hit counter\n",
				"# TYPE backend_hashkv_completed_cache_hit gauge\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: expected backend_hashkv_completed_cache_hit counter",
		},
		{
			name: "rejects negative post hashkv completed cache metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} -1\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: backend_hashkv_completed_cache_miss must contain exactly one cluster-labeled canonical non-negative safe integer sample",
		},
		{
			name: "rejects inactive post hashkv completed cache metrics",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.NewReplacer(
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 0\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 0\n",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: HashKV completed cache hit counter must increase during hashkv probes",
		},
		{
			name: "allows inactive post hashkv completed cache metrics on one ready pod",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetricsPod1: strings.NewReplacer(
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 0\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 0\n",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOK:     true,
			wantOutput: "hashkv_cache_metrics=ok",
		},
		{
			name: "rejects StatefulSet ownership drift during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"replacement-uid-0","ownerReferences":[{"apiVersion":"apps/v1","kind":"StatefulSet","name":"kubebrain","uid":"replacement-statefulset-uid","controller":true}]},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://new-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain-statefulset",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Ready Pod StatefulSet ownership mismatch: expected UID uid-kubebrain-statefulset; kubebrain-0:controller=apps/v1/StatefulSet/kubebrain/replacement-statefulset-uid",
		},
		{
			name: "rejects target container state change during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"running":{"startedAt":"2026-09-07T00:00:00Z"}},"lastState":{}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"terminated":{"exitCode":1,"finishedAt":"2026-09-07T00:00:01Z"}},"lastState":{}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain target container runtime state mismatch after HashKV cache probes: kubebrain-0:target-state=terminated",
		},
		{
			name: "rejects target container last state change during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"running":{"startedAt":"2026-09-07T00:00:00Z"}},"lastState":{}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"running":{"startedAt":"2026-09-07T00:00:00Z"}},"lastState":{"terminated":{"exitCode":1,"finishedAt":"2026-09-06T23:59:59Z"}}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Pod runtime identity changed during HashKV probes for pod kubebrain-0",
		},
		{
			name: "rejects target container started change during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{"running":{"startedAt":"2026-09-07T00:00:00Z"}},"lastState":{}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":false,"state":{"running":{"startedAt":"2026-09-07T00:00:00Z"}},"lastState":{}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain target container runtime state mismatch after HashKV cache probes: kubebrain-0:target-started=false",
		},
		{
			name: "rejects incomplete target container state at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true,"started":true,"state":{},"lastState":{}}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain target container runtime state mismatch: kubebrain-0:target-state=<empty>",
		},
		{
			name: "rejects duplicate scoped runtime status names at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true},{"name":"sidecar","containerID":"containerd://sidecar-a","restartCount":0,"ready":true},{"name":"sidecar","containerID":"containerd://sidecar-b","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: complete Pod runtime identity is required for HashKV cache baseline pod kubebrain-0",
		},
		{
			name: "rejects duplicate ready Pod names at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0a"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0a","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-0","uid":"uid-0b"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0b","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: complete Pod runtime identity is required for HashKV cache baseline pod kubebrain-0",
		},
		{
			name: "rejects duplicate runtime container IDs at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://shared-0","restartCount":0,"ready":true},{"name":"sidecar","containerID":"containerd://shared-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: complete Pod runtime identity is required for HashKV cache baseline pod kubebrain-0",
		},
		{
			name: "rejects duplicate ready Pod UIDs at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-shared"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-shared"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: complete Pod runtime identity is required for HashKV cache baseline pod kubebrain-0",
		},
		{
			name: "rejects non-string ready Pod UID at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":7},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: complete Pod runtime identity is required for HashKV cache baseline pod kubebrain-0",
		},
		{
			name: "rejects non-string ready Pod name at hashkv baseline",
			podsJSON: `{"items":[
				{"metadata":{"name":7,"uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Ready Pod name is required for HashKV cache baseline",
		},
		{
			name: "rejects duplicate Ready conditions after hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"Ready","status":"Unknown"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod Ready condition mismatch after HashKV cache probes: kubebrain-0:ready-condition-count=2",
		},
		{
			name: "rejects ContainersReady drift after hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ContainersReady","status":"False"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod Ready condition mismatch after HashKV cache probes: kubebrain-1:containers-ready-status=\"False\"",
		},
		{
			name: "rejects target runtime state drift after hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true,"started":true,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain target container runtime state mismatch after HashKV cache probes: kubebrain-1:target-state=waiting",
		},
		{
			name: "rejects Pod phase drift after hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"phase":"Pending","conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod lifecycle mismatch after HashKV cache probes: kubebrain-0:phase=\"Pending\"",
		},
		{
			name: "rejects Ready condition generation drift after hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0","generation":3},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod Ready condition mismatch after HashKV cache probes: kubebrain-0:ready-observed-generation=2,current-generation=3",
		},
		{
			name: "rejects duplicate Ready conditions after post hashkv evidence collection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			finalPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"Ready","status":"False"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod Ready condition mismatch after post HashKV evidence collection: kubebrain-1:ready-condition-count=2",
		},
		{
			name: "rejects ContainersReady drift after post hashkv evidence collection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			finalPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ContainersReady","status":"Unknown"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod Ready condition mismatch after post HashKV evidence collection: kubebrain-0:containers-ready-status=\"Unknown\"",
		},
		{
			name: "rejects target runtime state drift after post hashkv evidence collection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			finalPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true,"started":false,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain target container runtime state mismatch after post HashKV evidence collection: kubebrain-2:target-started=false",
		},
		{
			name: "rejects Pod phase drift after post hashkv evidence collection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			finalPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"phase":"Succeeded","conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod lifecycle mismatch after post HashKV evidence collection: kubebrain-1:phase=\"Succeeded\"",
		},
		{
			name: "rejects Ready condition generation drift after post hashkv evidence collection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			finalPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1","generation":3},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2","generation":2},"status":{"conditions":[{"type":"Ready","status":"True","observedGeneration":2}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HASHKV_HASH=111", "EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "KubeBrain Pod Ready condition mismatch after post HashKV evidence collection: kubebrain-1:ready-observed-generation=2,current-generation=3",
		},
		{
			name: "rejects native sidecar restart during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}],"initContainerStatuses":[{"name":"mesh","containerID":"containerd://mesh-old","imageID":"docker.io/library/mesh@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}],"initContainerStatuses":[{"name":"mesh","containerID":"containerd://mesh-new","imageID":"docker.io/library/mesh@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","restartCount":1,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Pod runtime identity changed during HashKV probes for pod kubebrain-0",
		},
		{
			name: "rejects target container readiness loss during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":false}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Pod runtime identity changed during HashKV probes for pod kubebrain-0",
		},
		{
			name: "rejects container restart during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://new-0","restartCount":1,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Pod runtime identity changed during HashKV probes for pod kubebrain-0",
		},
		{
			name: "rejects Pod restart during post HashKV evidence collection",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-0","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			finalPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://new-0","restartCount":1,"ready":true}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://old-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Pod runtime identity changed during post HashKV evidence collection for pod kubebrain-0",
		},
		{
			name: "allows stable multi-container identity in different API order",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"sidecar","containerID":"containerd://sidecar-0","restartCount":2,"ready":true},{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true}],"initContainerStatuses":[{"name":"sidecar","containerID":"containerd://mesh-0","imageID":"docker.io/library/mesh@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","restartCount":0,"ready":true}],"ephemeralContainerStatuses":[{"name":"sidecar","containerID":"containerd://debugger-0","imageID":"docker.io/library/debugger@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","restartCount":0,"ready":false}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			postPodsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-0","restartCount":0,"ready":true},{"name":"sidecar","containerID":"containerd://sidecar-0","restartCount":2,"ready":true}],"initContainerStatuses":[{"name":"sidecar","containerID":"containerd://mesh-0","imageID":"docker.io/library/mesh@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","restartCount":0,"ready":true}],"ephemeralContainerStatuses":[{"name":"sidecar","containerID":"containerd://debugger-0","imageID":"docker.io/library/debugger@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","restartCount":0,"ready":false}]}},
				{"metadata":{"name":"kubebrain-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-1","restartCount":0,"ready":true}]}},
				{"metadata":{"name":"kubebrain-2","uid":"uid-2"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"kubebrain","containerID":"containerd://app-2","restartCount":0,"ready":true}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOK:     true,
			wantOutput: "hashkv_cache_metrics=ok",
		},
		{
			name: "rejects process restart invisible to Pod status during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.ReplaceAll(
				defaultBaselineInfoMetrics(""),
				"process_start_time_seconds 200\n",
				"process_start_time_seconds 100\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: process start time changed during HashKV probes for pod kubebrain-0",
		},
		{
			name: "rejects missing process start time during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.NewReplacer(
				"# TYPE process_start_time_seconds gauge\n", "",
				"process_start_time_seconds 200\n", "",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: expected process_start_time_seconds gauge",
		},
		{
			name: "rejects labeled process start time during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"process_start_time_seconds 200\n",
				"process_start_time_seconds{cluster=\"default\"} 200\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: process_start_time_seconds must contain exactly one finite positive unlabeled sample",
		},
		{
			name: "rejects mixed unlabeled and labeled process start time during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"process_start_time_seconds 200\n",
				"process_start_time_seconds 200\nprocess_start_time_seconds{cluster=\"default\"} 200\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: process_start_time_seconds must contain exactly one finite positive unlabeled sample",
		},
		{
			name: "rejects overflowing process start time during hashkv cache probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.ReplaceAll(
				defaultBaselineInfoMetrics(""),
				"process_start_time_seconds 200\n",
				"process_start_time_seconds 1e9999\n",
			),
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"process_start_time_seconds 200\n",
				"process_start_time_seconds 1e9999\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: process_start_time_seconds must contain exactly one finite positive unlabeled sample",
		},
		{
			name: "rejects fractional hashkv completed cache counters",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.NewReplacer(
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 0\n", "backend_hashkv_completed_cache_hit{cluster=\"default\"} 0.5\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 0\n", "backend_hashkv_completed_cache_miss{cluster=\"default\"} 0.5\n",
			).Replace(defaultBaselineInfoMetrics("")),
			infoMetrics: strings.NewReplacer(
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n", "backend_hashkv_completed_cache_hit{cluster=\"default\"} 1.5\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n", "backend_hashkv_completed_cache_miss{cluster=\"default\"} 1.5\n",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "must contain exactly one cluster-labeled canonical non-negative safe integer sample",
		},
		{
			name: "rejects hashkv completed cache counter above safe integer range",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 9007199254740992\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "must contain exactly one cluster-labeled canonical non-negative safe integer sample",
		},
		{
			name: "rejects aggregate hashkv completed cache delta above safe integer range",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 4000000000000000\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: aggregate HashKV completed cache hit delta exceeds safe integer range",
		},
		{
			name: "rejects no post hashkv completed cache hits",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 0\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: HashKV completed cache hit counter must increase during hashkv probes",
		},
		{
			name: "rejects stale post hashkv completed cache counters without current probe deltas",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:              "ok",
			count:               "4",
			statusJSON:          `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: defaultInfoMetrics(""),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: HashKV completed cache hit counter must increase during hashkv probes",
		},
		{
			name: "rejects hashkv completed cache counter reset during probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.NewReplacer(
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 2\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 2\n",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: HashKV completed cache hit counter decreased for pod kubebrain-0",
		},
		{
			name: "rejects no post hashkv completed cache misses",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 0\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: HashKV completed cache miss counter must increase during hashkv probes",
		},
		{
			name: "rejects missing post hash mvcc metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"etcd_mvcc_hash_rev_duration_seconds_count{cluster=\"default\"} 1\n",
				"",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_mvcc_hash_rev_duration_seconds_count must contain exactly one cluster-labeled canonical non-negative safe integer sample",
		},
		{
			name: "rejects stale post hash mvcc histogram counts without current probe deltas",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.NewReplacer(
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_hit{cluster=\"default\"} 0\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
				"backend_hashkv_completed_cache_miss{cluster=\"default\"} 0\n",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: MVCC hash histogram count must increase during hash probes",
		},
		{
			name: "rejects non histogram post hash mvcc metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: defaultInfoMetrics("") +
				"# TYPE etcd_mvcc_hash_duration_seconds gauge\n",
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: expected etcd_mvcc_hash_duration_seconds histogram",
		},
		{
			name: "rejects prefixed post hash mvcc count spoof",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"etcd_mvcc_hash_rev_duration_seconds_count{cluster=\"default\"} 1\n",
				"spoof_etcd_mvcc_hash_rev_duration_seconds_count{cluster=\"default\"} 1\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_mvcc_hash_rev_duration_seconds_count must contain exactly one cluster-labeled canonical non-negative safe integer sample",
		},
		{
			name: "rejects incomplete post hash mvcc histogram buckets",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"etcd_mvcc_hash_duration_seconds_bucket{cluster=\"default\",le=\"0.01\"} 1\n",
				"",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_mvcc_hash_duration_seconds histogram family is incomplete or inconsistent",
		},
		{
			name: "rejects nonmonotonic post hash mvcc histogram buckets",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"etcd_mvcc_hash_duration_seconds_bucket{cluster=\"default\",le=\"0.02\"} 1\n",
				"etcd_mvcc_hash_duration_seconds_bucket{cluster=\"default\",le=\"0.02\"} 0\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_mvcc_hash_duration_seconds histogram family is incomplete or inconsistent",
		},
		{
			name: "rejects overflowing post hash mvcc histogram sum",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				"etcd_mvcc_hash_duration_seconds_sum{cluster=\"default\"} 0.005\n",
				"etcd_mvcc_hash_duration_seconds_sum{cluster=\"default\"} 1e9999\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_mvcc_hash_duration_seconds histogram family is incomplete or inconsistent",
		},
		{
			name: "rejects baseline hash observability family cluster mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.ReplaceAll(
				defaultBaselineInfoMetrics(""),
				"backend_hashkv_completed_cache_hit{cluster=\"default\"}",
				"backend_hashkv_completed_cache_hit{cluster=\"other\"}",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Hash observability metric clusters disagree for pod kubebrain-0",
		},
		{
			name: "rejects post hash observability family cluster mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.NewReplacer(
				"etcd_mvcc_hash_rev_duration_seconds_bucket{cluster=\"default\"", "etcd_mvcc_hash_rev_duration_seconds_bucket{cluster=\"other\"",
				"etcd_mvcc_hash_rev_duration_seconds_sum{cluster=\"default\"}", "etcd_mvcc_hash_rev_duration_seconds_sum{cluster=\"other\"}",
				"etcd_mvcc_hash_rev_duration_seconds_count{cluster=\"default\"}", "etcd_mvcc_hash_rev_duration_seconds_count{cluster=\"other\"}",
			).Replace(defaultInfoMetrics("")),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Hash observability metric clusters disagree for pod kubebrain-0",
		},
		{
			name: "rejects hash observability cluster drift during probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				`cluster="default"`,
				`cluster="other"`,
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Hash observability metric cluster changed during probes for pod kubebrain-0",
		},
		{
			name: "rejects hash observability cluster detached from server identity",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:              "ok",
			count:               "4",
			statusJSON:          `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: withHashObservabilityCluster(defaultBaselineInfoMetrics(""), "other"),
			infoMetrics:         withHashObservabilityCluster(defaultInfoMetrics(""), "other"),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: Hash observability metric cluster disagrees with server identity for pod kubebrain-0",
		},
		{
			name: "rejects server identity drift during hash probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.ReplaceAll(
				defaultInfoMetrics(""),
				`server_id="e3f"`,
				`server_id="abc"`,
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: server identity changed during HashKV probes for pod kubebrain-0",
		},
		{
			name: "rejects duplicate baseline server identity during hash probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: defaultBaselineInfoMetrics("") +
				"etcd_server_id{cluster=\"default\",server_id=\"abc\"} 1\n",
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_server_id must contain exactly one cluster/server_id-labeled value=1 sample",
		},
		{
			name: "rejects noncanonical baseline server identity value during hash probes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			baselineInfoMetrics: strings.ReplaceAll(
				defaultBaselineInfoMetrics(""),
				"etcd_server_id{cluster=\"default\",server_id=\"e3f\"} 1\n",
				"etcd_server_id{cluster=\"default\",server_id=\"e3f\"} 1.0\n",
			),
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOutput: "info metrics mismatch: etcd_server_id must contain exactly one cluster/server_id-labeled value=1 sample",
		},
		{
			name: "allows Ready Pod server identity set matching fully enumerated Status members",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "315"), "0"),
			baselineInfoMetricsPod2: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "abc"), "0"),
			infoMetrics:             withServerID(defaultInfoMetrics(""), "1c8"),
			infoMetricsPod1:         withServerRole(withServerID(defaultInfoMetrics(""), "315"), "0"),
			infoMetricsPod2:         withServerRole(withServerID(defaultInfoMetrics(""), "abc"), "0"),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"FAKE_REQUIRE_EXEC_CONTAINER=kubebrain",
				"FAKE_HASHKV_JSON=" + threeMemberHashKVJSON(),
			},
			wantOK:     true,
			wantOutput: "hashkv_server_identity_members_match=true, hashkv_server_identity_local_status_match=true, hashkv_server_role_local_status_match=true, hashkv_server_role_process_identity=stable",
		},
		{
			name: "rejects post HashKV Ready Pod server role mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "315"), "0"),
			baselineInfoMetricsPod2: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "abc"), "0"),
			infoMetrics:             withServerRole(withServerID(defaultInfoMetrics(""), "1c8"), "0"),
			infoMetricsPod1:         withServerRole(withServerID(defaultInfoMetrics(""), "315"), "0"),
			infoMetricsPod2:         withServerRole(withServerID(defaultInfoMetrics(""), "abc"), "0"),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"FAKE_HASHKV_JSON=" + threeMemberHashKVJSON(),
			},
			wantOutput: "info metrics mismatch: Ready Pod server roles do not match local Status for pod kubebrain-0",
		},
		{
			name: "rejects Ready Pod server role mismatch with local Status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerRole(withServerID(defaultBaselineInfoMetrics(""), "1c8"), "0"),
			baselineInfoMetricsPod1: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "315"), "0"),
			baselineInfoMetricsPod2: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "abc"), "0"),
			infoMetrics:             withServerRole(withServerID(defaultInfoMetrics(""), "1c8"), "0"),
			infoMetricsPod1:         withServerRole(withServerID(defaultInfoMetrics(""), "315"), "0"),
			infoMetricsPod2:         withServerRole(withServerID(defaultInfoMetrics(""), "abc"), "0"),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"FAKE_HASHKV_JSON=" + threeMemberHashKVJSON(),
			},
			wantOutput: "info metrics mismatch: Ready Pod server roles do not match local Status for pod kubebrain-0",
		},
		{
			name: "rejects noncanonical Ready Pod server role metric sample",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: threeMemberStatusJSON(),
			baselineInfoMetrics: strings.ReplaceAll(
				withServerID(defaultBaselineInfoMetrics(""), "1c8"),
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 2`,
			),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: etcd_server_is_leader must contain exactly one cluster-labeled canonical 0/1 sample",
		},
		{
			name: "rejects numeric local Status leader identity for fully enumerated Ready Pods",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:              "ok",
			count:               "4",
			statusJSON:          threeMemberStatusJSON(),
			baselineInfoMetrics: withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			localStatusPod0:     `{"header":{"cluster_id":"123","member_id":"456"},"leader":456}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: local Status leader ID must be a canonical positive uint64 JSON string for pod kubebrain-0",
		},
		{
			name: "rejects local Status leader disagreement with fully enumerated Status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:              "ok",
			count:               "4",
			statusJSON:          threeMemberStatusJSON(),
			baselineInfoMetrics: withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			localStatusPod0:     `{"header":{"cluster_id":"123","member_id":"456"},"leader":"789"}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: local Status leader differs from fully enumerated Status leader for pod kubebrain-0",
		},
		{
			name: "rejects nonboolean local Status learner role",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:              "ok",
			count:               "4",
			statusJSON:          threeMemberStatusJSON(),
			baselineInfoMetrics: withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			localStatusPod0:     `{"header":{"cluster_id":"123","member_id":"456"},"leader":"456","isLearner":"false"}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: local Status isLearner must be a JSON boolean for pod kubebrain-0",
		},
		{
			name: "rejects Ready Pod learner metric mismatch with local Status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:              "ok",
			count:               "4",
			statusJSON:          threeMemberStatusJSON(),
			baselineInfoMetrics: withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			localStatusPod0:     `{"header":{"cluster_id":"123","member_id":"456"},"leader":"456","isLearner":true}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: Ready Pod server roles do not match local Status for pod kubebrain-0",
		},
		{
			name: "rejects duplicate Ready Pod server identities with fully enumerated Status members",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "315"), "0"),
			baselineInfoMetricsPod2: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "315"), "0"),
			infoMetrics:             withServerID(defaultInfoMetrics(""), "1c8"),
			infoMetricsPod1:         withServerRole(withServerID(defaultInfoMetrics(""), "315"), "0"),
			infoMetricsPod2:         withServerID(defaultInfoMetrics(""), "315"),
			localStatusPod2:         defaultLocalStatusJSON("", "789"),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"FAKE_HASHKV_JSON=" + threeMemberHashKVJSON(),
			},
			wantOutput: "info metrics mismatch: Ready Pod server IDs must be unique",
		},
		{
			name: "rejects Ready Pod server identity outside fully enumerated Status members",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "315"), "0"),
			baselineInfoMetricsPod2: withServerRole(withServerID(defaultBaselineInfoMetrics(""), "def"), "0"),
			infoMetrics:             withServerID(defaultInfoMetrics(""), "1c8"),
			infoMetricsPod1:         withServerRole(withServerID(defaultInfoMetrics(""), "315"), "0"),
			infoMetricsPod2:         withServerRole(withServerID(defaultInfoMetrics(""), "def"), "0"),
			localStatusPod2:         defaultLocalStatusJSON("", "3567"),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"FAKE_HASHKV_JSON=" + threeMemberHashKVJSON(),
			},
			wantOutput: "info metrics mismatch: Ready Pod server ID set must match Status member ID set",
		},
		{
			name: "rejects swapped Ready Pod server identities despite matching Status member set",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "315"),
			baselineInfoMetricsPod1: withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod2: withServerID(defaultBaselineInfoMetrics(""), "abc"),
			infoMetrics:             withServerID(defaultInfoMetrics(""), "315"),
			infoMetricsPod1:         withServerID(defaultInfoMetrics(""), "1c8"),
			infoMetricsPod2:         withServerID(defaultInfoMetrics(""), "abc"),
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"FAKE_HASHKV_JSON=" + threeMemberHashKVJSON(),
			},
			wantOutput: "info metrics mismatch: Ready Pod server identity does not match local Status member",
		},
		{
			name: "rejects numeric local Status identity for fully enumerated Ready Pods",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerID(defaultBaselineInfoMetrics(""), "315"),
			baselineInfoMetricsPod2: withServerID(defaultBaselineInfoMetrics(""), "abc"),
			localStatusPod0:         `{"header":{"cluster_id":123,"member_id":456}}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: local Status cluster/member IDs must be JSON strings for pod kubebrain-0",
		},
		{
			name: "rejects local Status cluster mismatch for fully enumerated Ready Pods",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerID(defaultBaselineInfoMetrics(""), "315"),
			baselineInfoMetricsPod2: withServerID(defaultBaselineInfoMetrics(""), "abc"),
			localStatusPod0:         `{"header":{"cluster_id":"124","member_id":"456"},"leader":"456"}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: local Status cluster ID differs from Status endpoint cluster for pod kubebrain-0",
		},
		{
			name: "rejects local Status errors for fully enumerated Ready Pods",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:                  "ok",
			count:                   "4",
			statusJSON:              threeMemberStatusJSON(),
			baselineInfoMetrics:     withServerID(defaultBaselineInfoMetrics(""), "1c8"),
			baselineInfoMetricsPod1: withServerID(defaultBaselineInfoMetrics(""), "315"),
			baselineInfoMetricsPod2: withServerID(defaultBaselineInfoMetrics(""), "abc"),
			localStatusPod0:         `{"header":{"cluster_id":"123","member_id":"456"},"errors":["backend unavailable"]}`,
			extraEnv: []string{
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				"EXPECTED_INFO_METRICS_CHECKS=1",
			},
			wantOutput: "info metrics mismatch: local Status error must be empty for pod kubebrain-0",
		},
		{
			name: "rejects gateway hashkv hash drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"222","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv hash mismatch",
		},
		{
			name: "rejects gateway hashkv serving member mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"789","revision":"7"},"hash":"111","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv serving member mismatch",
		},
		{
			name: "rejects gateway status serving member mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"789","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status serving member mismatch",
		},
		{
			name: "rejects gateway auth status serving member mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"789","revision":"7","raft_term":"8"},"authRevision":"5"}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway auth status serving member mismatch",
		},
		{
			name: "rejects gateway alarm serving member mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"789","revision":"7","raft_term":"8"}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway alarm serving member mismatch",
		},
		{
			name: "rejects gateway hash serving member mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"789","revision":"7","raft_term":"8"},"hash":222}`,
			},
			wantOutput: "gateway hash serving member mismatch",
		},
		{
			name: "rejects gateway status raft term mismatch with direct status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"9"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"9","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "status/gateway status raft term mismatch",
		},
		{
			name: "rejects missing gateway hashkv raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7"},"hash":"111","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv raft term must be positive",
		},
		{
			name: "rejects missing gateway status header raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status header raft term must be positive",
		},
		{
			name: "rejects missing gateway auth status raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"authRevision":"5"}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway auth status raft term must be positive",
		},
		{
			name: "rejects missing gateway alarm raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7"}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway alarm raft term must be positive",
		},
		{
			name: "rejects missing gateway hash raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7"},"hash":222}`,
			},
			wantOutput: "gateway hash raft term must be positive",
		},
		{
			name: "rejects status hashkv endpoint revision mapping mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":99}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":8},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "status/hashkv endpoint revision mapping mismatch",
		},
		{
			name: "rejects gateway status serving revision mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"8","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status serving revision mismatch",
		},
		{
			name: "rejects gateway status db size mismatch with direct endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"100","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status dbSize mismatch with direct endpoint",
		},
		{
			name: "rejects gateway status raft index mismatch with direct endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"8","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status raftIndex mismatch with direct endpoint",
		},
		{
			name: "rejects non empty gateway status errors",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[]}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{},"errors":["alarm:NOSPACE"]}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status errors must be empty",
		},
		{
			name: "rejects malformed gateway status errors envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[]}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{},"errors":false}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status errors envelope invalid",
		},
		{
			name: "rejects malformed gateway status errors element",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[]}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{},"errors":[false]}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput:  "gateway status errors envelope invalid",
		},
		{
			name: "rejects gateway hashkv compact revision beyond hash revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"111","compact_revision":"8","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects gateway hashkv compact revision below sentinel",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"111","compact_revision":"-2","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv compact revision must be at least -1",
		},
		{
			name: "rejects non ready replica",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"False"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Ready pod count mismatch",
		},
		{
			name: "rejects readyz failure",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "starting",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "readyz mismatch",
		},
		{
			name: "rejects count drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "5",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_PREFIX_COUNT=4"},
			wantOutput: "prefix count mismatch",
		},
		{
			name: "rejects count drift on additional status endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_PREFIX_COUNT=4",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"FAKE_PREFIX_COUNTS=http://127.0.0.1:2379=4\nhttp://127.0.0.2:2379=5",
			},
			wantOutput: "prefix count mismatch for http://127.0.0.2:2379",
		},
		{
			name: "rejects count drift on bootstrap endpoint when status endpoints override",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"ENDPOINT=http://127.0.0.9:2379",
				"EXPECTED_PREFIX_COUNT=4",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"FAKE_PREFIX_COUNTS=http://127.0.0.9:2379=5\nhttp://127.0.0.1:2379=4\nhttp://127.0.0.2:2379=4",
			},
			wantOutput: "prefix count mismatch for http://127.0.0.9:2379",
		},
		{
			name: "rejects status cluster id drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":321,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantTimeout: []string{
				"10s etcdctl",
			},
			wantOutput: "status cluster ID mismatch",
		},
		{
			name: "rejects missing status cluster id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status cluster ID is required",
		},
		{
			name: "rejects string status numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"dbSize":"99"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON numbers",
		},
		{
			name: "rejects boolean status header identity envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":false,"clusterId":123,"member_id":false,"memberId":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON numbers",
		},
		{
			name: "rejects fractional status numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123.5,"member_id":456.5,"revision":7.5},"dbSize":99.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON integers",
		},
		{
			name: "rejects mismatched status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":9}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects status raft term missing top level pair",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects string status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":"8"},"dbSize":99,"raftTerm":"8"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects fractional status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8.5},"dbSize":99,"raftTerm":8.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects boolean status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":false},"dbSize":99,"raftTerm":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "accepts independently sampled status applied index inversion",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":8,"raftAppliedIndex":9}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"8","raftAppliedIndex":"9","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      true,
			wantOutput:  "min_status_raft_index=8, min_status_raft_applied_index=9, raft_indexes_sampled=true",
		},
		{
			name: "rejects status raft index missing applied pair",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":7}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects fractional status raft index envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":7.5,"raftAppliedIndex":7.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects boolean status raft index envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":false,"raftAppliedIndex":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "accepts independently sampled status raft indexes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":8,"raftAppliedIndex":8}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"8","raftAppliedIndex":"8","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      true,
			wantOutput:  "min_status_raft_index=8, min_status_raft_applied_index=8, raft_indexes_sampled=true",
		},
		{
			name: "accepts independently sampled status db size inversion",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":100}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"100","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      true,
			wantOutput:  "min_status_db_size_in_use=100",
		},
		{
			name: "rejects string status db size in use envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":"88"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects fractional status db size in use envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":88.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects boolean status db size in use envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects malformed status version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status version envelope invalid",
		},
		{
			name: "rejects boolean status version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status version envelope invalid",
		},
		{
			name: "rejects malformed status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"storageVersion":"3.x","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status storageVersion envelope invalid",
		},
		{
			name: "rejects boolean status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"storageVersion":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status storageVersion envelope invalid",
		},
		{
			name: "rejects snakecase boolean status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"storage_version":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status storageVersion envelope invalid",
		},
		{
			name: "rejects zero status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeQuota":0}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeQuota envelope invalid",
		},
		{
			name: "rejects boolean status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeQuota":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeQuota envelope invalid",
		},
		{
			name: "rejects snakecase boolean status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"db_size_quota":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeQuota envelope invalid",
		},
		{
			name: "rejects string status learner envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"isLearner":"false"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status isLearner envelope invalid",
		},
		{
			name: "rejects snakecase string status learner envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"is_learner":"false"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status isLearner envelope invalid",
		},
		{
			name: "rejects status downgrade enabled without target",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":{"enabled":true}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects boolean status downgrade target version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":{"enabled":true,"targetVersion":false}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects snakecase boolean status downgrade target version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgrade_info":{"enabled":true,"target_version":false}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects short status downgrade target version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":{"enabled":true,"targetVersion":"3.6"}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects boolean status downgrade info envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects snakecase boolean status downgrade info envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgrade_info":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects status version mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.6.0","dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
			},
			wantOutput: "status version mismatch",
		},
		{
			name: "rejects non positive status leader envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"leader":0,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status leader envelope invalid",
		},
		{
			name: "rejects boolean status leader envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"leader":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status leader envelope invalid",
		},
		{
			name: "rejects non empty status errors",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":["etcdserver: no leader"]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects uppercase non empty status errors",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"Errors":["etcdserver: no leader"]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects boolean status errors envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects boolean status errors element envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[false]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects uppercase boolean status errors element envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"Errors":[false]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects status response missing header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status header is required",
		},
		{
			name: "rejects status response missing status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379"}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status payload is required",
		},
		{
			name: "rejects hashkv hash drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":222,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv hash mismatch",
		},
		{
			name: "rejects hashkv cluster id drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":321,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv cluster ID mismatch",
		},
		{
			name: "rejects missing hashkv cluster id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv cluster ID is required",
		},
		{
			name: "rejects string hashkv numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"hash":"111","compact_revision":"3","hash_revision":"7"}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects boolean hashkv hash envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":false,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects boolean hashkv hash revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3,"hashRevision":false}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects partial hashkv hash revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3,"hash_revision":7}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv hash revision must be present on all endpoints when present",
		},
		{
			name: "rejects missing hashkv hash revision for etcd 3.7",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_DIRECT_HASHKV_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":0}`,
			},
			wantOutput: "direct hashkv hash revision must match header revision for etcd 3.7.0",
		},
		{
			name: "rejects missing hashkv hash revision for oversized compatible version",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"9223372036854775808.0.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"9223372036854775808.0.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			directStatusJSON: statusProbeJSON("9223372036854775808.0.0", "3.7.0", 2147483648, false, ""),
			versionJSON:      `{"etcdserver":"9223372036854775808.0.0","etcdcluster":"9223372036854775808.0","storage":"3.7.0"}`,
			infoVersionJSON:  `{"etcdserver":"9223372036854775808.0.0","etcdcluster":"9223372036854775808.0","storage":"3.7.0"}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=9223372036854775808.0.0",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_DIRECT_HASHKV_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":0}`,
			},
			wantOutput: "direct hashkv hash revision must match header revision for etcd 9223372036854775808.0.0",
		},
		{
			name: "reports required hashkv hash revisions for etcd 3.7",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}]`,
			},
			wantOK:     true,
			wantOutput: "direct_hashkv_hash_revision=7, direct_hashkv_hash_revision_match=true",
		},
		{
			name: "allows missing hashkv hash revision before etcd 3.6",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.5.0","dbSize":99,"dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7}}]`,
			gatewayJSON:      `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.5.0","dbSize":"99","dbSizeInUse":"88","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7"}`,
			directStatusJSON: statusProbeJSON("3.5.0", "", 0, false, ""),
			versionJSON:      `{"etcdserver":"3.5.0","etcdcluster":"3.5","storage":"unknown"}`,
			infoVersionJSON:  `{"etcdserver":"3.5.0","etcdcluster":"3.5","storage":"unknown"}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.5.0",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_DIRECT_HASHKV_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":0}`,
			},
			wantOK:     true,
			wantOutput: "direct_status_v36_fields_match=not-required",
		},
		{
			name: "rejects hashkv hash revision mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3,"hashRevision":6}}]`,
			},
			wantOutput: "hashkv hash revision must match header revision",
		},
		{
			name: "rejects boolean hashkv header identity envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":false,"clusterId":123,"member_id":false,"memberId":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects fractional hashkv numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123.5,"member_id":456.5,"revision":7.5},"hash":111.5,"compact_revision":3.5,"hash_revision":7.5}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON integers",
		},
		{
			name: "rejects hashkv response missing header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"hash":111,"compact_revision":0}}]`,
			},
			wantOutput: "hashkv header is required",
		},
		{
			name: "rejects hashkv response missing hashkv",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379"}]`,
			},
			wantOutput: "hashkv payload is required",
		},
		{
			name: "rejects hashkv response missing hash",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=0",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"compact_revision":0}}]`,
			},
			wantOutput: "hashkv hash is required",
		},
		{
			name: "rejects hashkv response missing compact revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111}}]`,
			},
			wantOutput: "hashkv compact revision is required",
		},
		{
			name: "rejects boolean hashkv compact revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":false,"compactRevision":3}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects hashkv endpoint member mapping mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"dbSize":99}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "status/hashkv endpoint member mapping mismatch",
		},
		{
			name: "rejects duplicate hashkv member ids across endpoints",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":8},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv member IDs must be unique",
		},
		{
			name: "rejects zero hashkv member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":0,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv member ID must be positive",
		},
		{
			name: "rejects missing hashkv member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv member ID is required",
		},
		{
			name: "rejects hashkv endpoint set mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.3:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":8},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv endpoint set mismatch",
		},
		{
			name: "rejects hashkv response missing endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv endpoint set mismatch",
		},
		{
			name: "rejects hashkv endpoint count mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv endpoint count mismatch",
		},
		{
			name: "rejects non array hashkv response",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON={"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}`,
			},
			wantOutput: "hashkv endpoint count mismatch",
		},
		{
			name: "rejects hashkv compact revision beyond hash revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":8}}]`,
			},
			wantOutput: "hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects hashkv compact revision beyond explicit hash revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":8,"hash_revision":7}}]`,
			},
			wantOutput: "hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects negative hashkv revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":-1},"hash":111,"compact_revision":-1}}]`,
			},
			wantOutput: "hashkv revision must be non-negative",
		},
		{
			name: "rejects missing hashkv revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456},"hash":111,"compact_revision":0}}]`,
			},
			wantOutput: "hashkv revision is required",
		},
		{
			name: "rejects non positive hashkv raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":0},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv raft term envelope invalid",
		},
		{
			name: "rejects boolean hashkv raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":false},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv raft term envelope invalid",
		},
		{
			name: "rejects status and hashkv raft term mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":9},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "status/hashkv raft term mismatch",
		},
		{
			name: "rejects status and hashkv revision mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":8},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "status/hashkv revision mismatch",
		},
		{
			name: "accepts uncompact hashkv revision sentinel",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":-1}}]`,
				`FAKE_DIRECT_HASHKV_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":-1,"hash_revision":7}`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"111","compact_revision":"-1","hash_revision":"7"}`,
			},
			wantOK:     true,
			wantOutput: "gateway_hashkv_compact_revision=-1, gateway_hashkv_revisions_match=true, gateway_hashkv_body_match=true",
		},
		{
			name: "rejects hashkv compact revision below sentinel",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":-2}}]`,
			},
			wantOutput: "hashkv compact revision must be at least -1",
		},
		{
			name: "rejects hashkv compact revision beyond hash revision on one endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":5},"hash":111,"compact_revision":1}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":8},"hash":111,"compact_revision":9}}
				]`,
			},
			wantOutput: "hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects zero status member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":0,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status member ID must be positive",
		},
		{
			name: "rejects missing status member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status member ID is required",
		},
		{
			name: "rejects negative status revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":-1},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status revision must be non-negative",
		},
		{
			name: "rejects missing status revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status revision is required",
		},
		{
			name: "rejects negative status db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":-1}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize must be positive",
		},
		{
			name: "rejects zero status db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":0}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize must be positive",
		},
		{
			name: "rejects missing status db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize is required",
		},
		{
			name: "rejects boolean status db size envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":false,"db_size":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON numbers",
		},
		{
			name: "rejects status db size in use without db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSizeInUse":88}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize is required",
		},
		{
			name: "rejects duplicate status member ids across endpoints",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
			},
			wantOutput: "status member IDs must be unique",
		},
		{
			name: "rejects status endpoint set mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.3:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
			},
			wantOutput: "status endpoint set mismatch",
		},
		{
			name: "rejects status response missing endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}
			]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status endpoint set mismatch",
		},
		{
			name: "rejects status endpoint count mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
			},
			wantOutput: "status endpoint count mismatch",
		},
		{
			name: "rejects non array status response",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status endpoint count mismatch",
		},
		{
			name: "rejects empty status endpoint entry before commands",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,",
			},
			wantOutput: "STATUS_ENDPOINTS contains an empty endpoint",
		},
		{
			name: "rejects duplicate status endpoints before commands",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.1:2379",
			},
			wantOutput: "STATUS_ENDPOINTS must not contain duplicate endpoints",
		},
		{
			name:       "rejects unsafe endpoint before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"ENDPOINT=http://127.0.0.1:2379\nbad"},
			wantOutput: "ENDPOINT contains unsupported characters",
		},
		{
			name:       "rejects invalid probe timeout before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"PROBE_TIMEOUT=forever"},
			wantOutput: "PROBE_TIMEOUT must be a positive duration",
		},
		{
			name:       "rejects hashkv hash without expected cluster id before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HASHKV_HASH=111"},
			wantOutput: "EXPECTED_HASHKV_HASH requires EXPECTED_STATUS_CLUSTER_ID",
		},
		{
			name:       "rejects malformed expected status version before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_VERSION=3.7"},
			wantOutput: "EXPECTED_STATUS_VERSION must be empty or a semver string",
		},
		{
			name:       "rejects expected status version without expected cluster id before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput: "EXPECTED_STATUS_VERSION requires EXPECTED_STATUS_CLUSTER_ID",
		},
		{
			name:       "rejects malformed expected livez named checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_LIVEZ_NAMED_CHECKS=true"},
			wantOutput: "EXPECTED_LIVEZ_NAMED_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected health exclude checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HEALTH_EXCLUDE_CHECKS=true"},
			wantOutput: "EXPECTED_HEALTH_EXCLUDE_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected health method checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HEALTH_METHOD_CHECKS=true"},
			wantOutput: "EXPECTED_HEALTH_METHOD_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected http header checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HTTP_HEADER_CHECKS=true"},
			wantOutput: "EXPECTED_HTTP_HEADER_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected info metrics checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_INFO_METRICS_CHECKS=true"},
			wantOutput: "EXPECTED_INFO_METRICS_CHECKS must be empty or 1",
		},
		{
			name:       "rejects info metrics checks without expected status version before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "EXPECTED_INFO_METRICS_CHECKS requires EXPECTED_STATUS_VERSION",
		},
		{
			name:           "rejects info endpoints without info metrics checks before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[]`,
			extraEnv:       []string{"INFO_ENDPOINTS=http://172.18.0.3:8080"},
			wantOutput:     "INFO_ENDPOINTS requires EXPECTED_INFO_METRICS_CHECKS=1",
			wantNoCommands: true,
		},
		{
			name:       "rejects info endpoint count mismatch before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080",
			},
			wantOutput:     "INFO_ENDPOINTS count must match STATUS_ENDPOINTS: expected 2, got 1",
			wantNoCommands: true,
		},
		{
			name:       "rejects duplicate normalized info endpoints before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"INFO_ENDPOINTS=http://172.18.0.3:8080,http://172.18.0.3:8080/",
			},
			wantOutput:     "INFO_ENDPOINTS must not contain duplicate endpoints",
			wantNoCommands: true,
		},
		{
			name:       "rejects info endpoint path before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
				"EXPECTED_INFO_METRICS_CHECKS=1",
				"INFO_ENDPOINTS=http://172.18.0.3:8080/private",
			},
			wantOutput:     "INFO_ENDPOINTS must contain absolute HTTP(S) base URLs without paths",
			wantNoCommands: true,
		},
		{
			name:       "rejects malformed expected debug vars checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_DEBUG_VARS_CHECKS=true"},
			wantOutput: "EXPECTED_DEBUG_VARS_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected pprof disabled checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=true"},
			wantOutput: "EXPECTED_PPROF_DISABLED_CHECKS must be empty or 1",
		},
		{
			name:           "rejects malformed expected gateway client certificate auth flag before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:       []string{"EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION=true"},
			wantOutput:     "EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION must be empty or 1",
			wantNoCommands: true,
		},
		{
			name:           "requires status contract for gateway client certificate rejection before commands",
			podsJSON:       `{"items":[]}`,
			readyz:         "ok",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:       []string{"EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION=1"},
			wantOutput:     "EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION requires EXPECTED_STATUS_CLUSTER_ID",
			wantNoCommands: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "kubectl"), `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" exec "* ]]; then
if [[ -n "${FAKE_REQUIRE_EXEC_CONTAINER:-}" && " $* " != *" -c ${FAKE_REQUIRE_EXEC_CONTAINER} -- "* ]]; then
	echo "kubectl exec did not target container ${FAKE_REQUIRE_EXEC_CONTAINER}" >&2
	exit 1
fi
if [[ " $* " == *"/v3/maintenance/status"* ]]; then
		if [[ " $* " == *" kubebrain-1 "* ]]; then
			printf '%s' "$FAKE_LOCAL_STATUS_POD_1"
			exit 0
		fi
		if [[ " $* " == *" kubebrain-2 "* ]]; then
			printf '%s' "$FAKE_LOCAL_STATUS_POD_2"
			exit 0
		fi
		printf '%s' "$FAKE_LOCAL_STATUS_POD_0"
		exit 0
	fi
	exec_calls="$(wc -l <"$FAKE_KUBECTL_EXEC_LOG" 2>/dev/null || true)"
	exec_calls="${exec_calls//[[:space:]]/}"
	printf 'exec\n' >>"$FAKE_KUBECTL_EXEC_LOG"
	if (( exec_calls < EXPECTED_READY_PODS )); then
		if [[ " $* " == *" kubebrain-1 "* && -n "${FAKE_BASELINE_INFO_METRICS_POD_1:-}" ]]; then
			printf '%s' "$FAKE_BASELINE_INFO_METRICS_POD_1"
			exit 0
		fi
		if [[ " $* " == *" kubebrain-2 "* && -n "${FAKE_BASELINE_INFO_METRICS_POD_2:-}" ]]; then
			printf '%s' "$FAKE_BASELINE_INFO_METRICS_POD_2"
			exit 0
		fi
		printf '%s' "$FAKE_BASELINE_INFO_METRICS"
		exit 0
	fi
	if [[ " $* " == *" kubebrain-1 "* && -n "${FAKE_INFO_METRICS_POD_1:-}" ]]; then
		printf '%s' "$FAKE_INFO_METRICS_POD_1"
		exit 0
	fi
	if [[ " $* " == *" kubebrain-2 "* && -n "${FAKE_INFO_METRICS_POD_2:-}" ]]; then
		printf '%s' "$FAKE_INFO_METRICS_POD_2"
		exit 0
	fi
  printf '%s' "$FAKE_INFO_METRICS"
  exit 0
fi
get_calls=0
if [[ -f "$FAKE_KUBECTL_GET_LOG" ]]; then
	get_calls="$(wc -l <"$FAKE_KUBECTL_GET_LOG")"
fi
printf 'get\n' >>"$FAKE_KUBECTL_GET_LOG"
if (( get_calls >= 2 )) && [[ -n "${FAKE_FINAL_PODS_JSON:-}" ]]; then
	printf '%s' "$FAKE_FINAL_PODS_JSON"
	exit 0
fi
if (( get_calls >= 1 )) && [[ -n "${FAKE_POST_PODS_JSON:-}" ]]; then
	printf '%s' "$FAKE_POST_PODS_JSON"
	exit 0
fi
printf '%s' "$FAKE_PODS_JSON"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "curl"), `#!/usr/bin/env bash
set -euo pipefail
target="${@: -1}"
if [[ "${EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION:-}" == "1" && "$target" == "${ENDPOINT%/}/v3/maintenance/status" ]]; then
  printf '%s' "${FAKE_GATEWAY_CERT_REJECTION_RESPONSE:-HTTP/1.1 400 Bad Request
Content-Type: text/plain; charset=utf-8

CommonName of client sending a request against gateway will be ignored and not used as expected
}"
  exit 0
fi
if [[ "$target" == "${ENDPOINT%/}/v3/auth/authenticate" ]]; then
  if [[ "$*" != *'"name":"root"'* || "$*" != *'"password":"secret:with:colons"'* ]]; then
    echo "authenticate payload mismatch" >&2
    exit 1
  fi
  printf '{"token":"fake-token"}'
  exit 0
fi
if [[ "${FAKE_REQUIRE_GATEWAY_AUTH:-}" == "1" && "$target" == */v3/* && "$*" != *"Authorization: Bearer fake-token"* ]]; then
  echo "gateway bearer token missing" >&2
  exit 1
fi
if [[ " $* " == *" -X POST "* ]]; then
  if [[ "$target" == "${READYZ_URL}" || "$target" == "${READYZ_URL%/readyz}/livez" || "$target" == "${READYZ_URL%/readyz}/debug/vars" || "$target" == "${ENDPOINT%/}/health" ]]; then
  code="${FAKE_HEALTH_METHOD_CODE:-405}"
  allow="${FAKE_HEALTH_METHOD_ALLOW:-GET}"
  printf 'HTTP/1.1 %s Method Not Allowed\r\nAllow: %s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nMethod Not Allowed\n' "$code" "$allow"
  exit 0
  fi
fi
if [[ " $* " == *" -D - "* ]]; then
  target="${@: -1}"
  if [[ "$target" == "${READYZ_URL}" || "$target" == "${READYZ_URL%/readyz}/livez" ]]; then
    printf 'HTTP/1.1 200 OK\r\nContent-Type: %s\r\n' "$FAKE_HTTP_HEADER_CONTENT_TYPE"
    if [[ "$FAKE_HTTP_HEADER_NOSNIFF" != "missing" ]]; then
      printf 'X-Content-Type-Options: %s\r\n' "$FAKE_HTTP_HEADER_NOSNIFF"
    fi
    printf '\r\n'
    exit 0
  fi
  if [[ "$target" == "${ENDPOINT%/}/version" || "$target" == "${READYZ_URL%/readyz}/version" ]]; then
    printf 'HTTP/1.1 200 OK\r\nContent-Type: %s\r\n\r\n' "$FAKE_VERSION_HEADER_CONTENT_TYPE"
    exit 0
  fi
  if [[ "$target" == "${READYZ_URL%/readyz}/debug/vars" ]]; then
    printf 'HTTP/1.1 200 OK\r\nContent-Type: %s\r\n\r\n' "$FAKE_DEBUG_VARS_HEADER_CONTENT_TYPE"
    exit 0
  fi
fi
if [[ " $* " == *" -i "* ]]; then
  target="${@: -1}"
  if [[ "$target" == "${ENDPOINT%/}/metrics" ]]; then
    printf '%s' "$FAKE_CLIENT_METRICS_RESPONSE"
    exit 0
  fi
  if [[ "$target" == "${ENDPOINT%/}/debug/vars" ]]; then
    printf '%s' "$FAKE_CLIENT_DEBUG_VARS_RESPONSE"
    exit 0
  fi
  if [[ "$target" == "${ENDPOINT%/}/debug/pprof/" ]]; then
    printf '%s' "$FAKE_CLIENT_PPROF_RESPONSE"
    exit 0
  fi
  if [[ "$target" == "${READYZ_URL%/readyz}/debug/pprof/" ]]; then
    printf '%s' "$FAKE_INFO_PPROF_RESPONSE"
    exit 0
  fi
fi
for arg in "$@"; do
	if [[ -n "${FAKE_LEADER_INFO_METRICS_URL:-}" && "$arg" == "$FAKE_LEADER_INFO_METRICS_URL" ]]; then
		printf '%s' "$FAKE_LEADER_INFO_METRICS"
		exit 0
	fi
	if [[ "$arg" == "${READYZ_URL}?verbose" ]]; then
    printf '%s' "$FAKE_READYZ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL}?verbose&exclude=data_corruption" ]]; then
    printf '%s' "$FAKE_READYZ_EXCLUDE_DATA"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL}?verbose&exclude=unknown" ]]; then
    printf '%s' "$FAKE_READYZ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/data_corruption?verbose" ]]; then
    printf '[+]data_corruption ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/serializable_read?verbose" ]]; then
    printf '[+]serializable_read ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/linearizable_read?verbose" ]]; then
    printf '[+]linearizable_read ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/non_learner?verbose" ]]; then
    printf '[+]non_learner ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez?verbose" ]]; then
    printf '%s' "$FAKE_LIVEZ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez?verbose&exclude=serializable_read" ]]; then
    printf '%s' "$FAKE_LIVEZ_EXCLUDE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez/serializable_read?verbose" ]]; then
    printf '%s' "$FAKE_LIVEZ_SERIALIZABLE_READ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez" ]]; then
    printf '%s' "$FAKE_LIVEZ"
    exit 0
  fi
  if [[ "$arg" == */health\?serializable=true ]]; then
    printf '%s' "$FAKE_SERIALIZABLE_HEALTH_JSON"
    exit 0
  fi
  if [[ "$arg" == */health ]]; then
    printf '%s' "$FAKE_HEALTH_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/status ]]; then
    printf '%s' "$FAKE_GATEWAY_STATUS_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/hashkv ]]; then
    printf '%s' "$FAKE_GATEWAY_HASHKV_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/hash ]]; then
    printf '%s' "$FAKE_GATEWAY_HASH_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/auth/status ]]; then
    printf '%s' "$FAKE_GATEWAY_AUTH_STATUS_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/alarm ]]; then
    printf '%s' "$FAKE_GATEWAY_ALARM_JSON"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/version" ]]; then
    printf '%s' "$FAKE_INFO_VERSION_JSON"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/metrics" ]]; then
    printf '%s' "$FAKE_INFO_METRICS"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/debug/vars" ]]; then
    printf '%s' "$FAKE_INFO_DEBUG_VARS"
    exit 0
  fi
  if [[ "$arg" == */version ]]; then
    printf '%s' "$FAKE_VERSION_JSON"
    exit 0
  fi
done
printf '%s' "$FAKE_READYZ"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "go"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"maintenance-status-probe"* ]]; then
  printf '%s\n' "$FAKE_DIRECT_STATUS_JSON"
  exit 0
fi
if [[ "$*" == *"maintenance-hashkv-probe"* ]]; then
  printf '%s\n' "$FAKE_DIRECT_HASHKV_JSON"
  exit 0
fi
if [[ "$*" == *"maintenance-hash-probe"* ]]; then
  printf '%s\n' "$FAKE_DIRECT_HASH_JSON"
  exit 0
fi
if [[ -n "${FAKE_PREFIX_COUNTS:-}" ]]; then
  while IFS= read -r line; do
    endpoint="${line%=*}"
    count="${line##*=}"
    if [[ "$endpoint" == "$ENDPOINT" ]]; then
      printf '%s\n' "$count"
      exit 0
    fi
  done <<<"$FAKE_PREFIX_COUNTS"
  echo "no fake prefix count for endpoint ${ENDPOINT}" >&2
  exit 1
fi
printf '%s\n' "$FAKE_PREFIX_COUNT"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "etcdctl"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "${FAKE_REQUIRE_ETCDCTL_COMMAND_TIMEOUT:-}" == "1" ]]; then
  if [[ " $* " != *" --dial-timeout=${PROBE_TIMEOUT} "* ]]; then
    echo "etcdctl dial timeout mismatch: expected --dial-timeout=${PROBE_TIMEOUT}, got $*" >&2
    exit 1
  fi
  if [[ " $* " != *" --command-timeout=${PROBE_TIMEOUT} "* ]]; then
    echo "etcdctl command timeout mismatch: expected --command-timeout=${PROBE_TIMEOUT}, got $*" >&2
    exit 1
  fi
fi
if [[ "$*" == *"endpoint hashkv"* ]]; then
  printf '%s\n' "$FAKE_HASHKV_JSON"
  exit 0
fi
if [[ "$*" == *"auth status"* ]]; then
  printf '%s\n' "$FAKE_DIRECT_AUTH_STATUS_JSON"
  exit 0
fi
if [[ "$*" == *"alarm list"* ]]; then
  printf '%s\n' "$FAKE_DIRECT_ALARM_JSON"
  exit 0
fi
if [[ "$*" == *"endpoint status"* && -n "${FAKE_SECOND_STATUS_JSON:-}" ]]; then
  status_calls=0
  if [[ -f "$FAKE_STATUS_CALL_LOG" ]]; then
    status_calls="$(wc -l <"$FAKE_STATUS_CALL_LOG")"
  fi
  printf 'status\n' >>"$FAKE_STATUS_CALL_LOG"
  if (( status_calls >= 1 )); then
    printf '%s\n' "$FAKE_SECOND_STATUS_JSON"
    exit 0
  fi
fi
printf '%s\n' "$FAKE_STATUS_JSON"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "timeout"), `#!/usr/bin/env bash
set -euo pipefail
duration="$1"
shift
printf '%s %s\n' "$duration" "$(basename "$1")" >>"$FAKE_TIMEOUT_LOG"
if [[ -n "${FAKE_TIMEOUT_EXIT_CODE:-}" && ( -z "${FAKE_TIMEOUT_MATCH:-}" || " $* " == *"${FAKE_TIMEOUT_MATCH}"* ) ]]; then
  exit "$FAKE_TIMEOUT_EXIT_CODE"
fi
exec "$@"
`)
			timeoutLog := filepath.Join(dir, "timeout.log")
			statusCallLog := filepath.Join(dir, "status-calls.log")
			kubectlExecLog := filepath.Join(dir, "kubectl-exec.log")
			kubectlGetLog := filepath.Join(dir, "kubectl-get.log")

			env := []string{
				"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"KUBECTL=kubectl",
				"CURL=curl",
				"GO=go",
				"ETCDCTL=etcdctl",
				"TIMEOUT_CMD=timeout",
				"JQ=jq",
				"KUBE_CONTEXT=kind-kubebrain-dbaas",
				"KUBEBRAIN_NAMESPACE=kubebrain-dev",
				"EXPECTED_READY_PODS=3",
				"ENDPOINT=http://127.0.0.1:2379",
				"READYZ_URL=http://172.18.0.3:32758/readyz",
				"PREFIX=/",
				"PROBE_TIMEOUT=10s",
				"FAKE_PODS_JSON=" + podJSONWithDefaultRuntimeIdentities(t, tc.podsJSON),
				"FAKE_POST_PODS_JSON=" + podJSONWithDefaultRuntimeIdentities(t, tc.postPodsJSON),
				"FAKE_FINAL_PODS_JSON=" + podJSONWithDefaultRuntimeIdentities(t, tc.finalPodsJSON),
				"FAKE_READYZ=" + tc.readyz,
				"FAKE_READYZ_VERBOSE=" + defaultReadyzVerbose(tc.readyzVerbose),
				"FAKE_READYZ_EXCLUDE_DATA=" + defaultReadyzExcludeData(tc.readyzExcludeData),
				"FAKE_LIVEZ=" + defaultLivez(tc.livez),
				"FAKE_LIVEZ_VERBOSE=" + defaultLivezVerbose(tc.livezVerbose),
				"FAKE_LIVEZ_EXCLUDE=" + defaultLivez(tc.livezExclude),
				"FAKE_LIVEZ_SERIALIZABLE_READ_VERBOSE=" + defaultLivezVerbose(tc.livezNamedVerbose),
				"FAKE_HEALTH_JSON=" + defaultHealthJSON(tc.healthJSON),
				"FAKE_HEALTH_METHOD_CODE=" + defaultHealthMethodCode(tc.healthMethodCode),
				"FAKE_HEALTH_METHOD_ALLOW=" + defaultHealthMethodAllow(tc.healthMethodAllow),
				"FAKE_HTTP_HEADER_CONTENT_TYPE=" + defaultHTTPHeaderContentType(tc.httpHeaderType),
				"FAKE_HTTP_HEADER_NOSNIFF=" + defaultHTTPHeaderNosniff(tc.httpHeaderNosniff),
				"FAKE_VERSION_HEADER_CONTENT_TYPE=" + defaultVersionHeaderContentType(tc.versionHeaderType),
				"FAKE_DEBUG_VARS_HEADER_CONTENT_TYPE=" + defaultDebugVarsHeaderContentType(tc.debugVarsHeader),
				"FAKE_SERIALIZABLE_HEALTH_JSON=" + defaultHealthJSON(tc.serialHealthJSON),
				"FAKE_PREFIX_COUNT=" + tc.count,
				"FAKE_STATUS_JSON=" + tc.statusJSON,
				"FAKE_SECOND_STATUS_JSON=" + tc.secondStatusJSON,
				"FAKE_STATUS_CALL_LOG=" + statusCallLog,
				"FAKE_GATEWAY_STATUS_JSON=" + defaultGatewayStatusJSON(tc.gatewayJSON),
				"FAKE_DIRECT_STATUS_JSON=" + defaultDirectStatusJSON(tc.directStatusJSON),
				"FAKE_DIRECT_AUTH_STATUS_JSON=" + defaultDirectAuthStatusJSON(tc.directAuthJSON),
				"FAKE_GATEWAY_AUTH_STATUS_JSON=" + defaultGatewayAuthStatusJSON(tc.authJSON),
				"FAKE_DIRECT_ALARM_JSON=" + defaultDirectAlarmJSON(tc.directAlarmJSON),
				"FAKE_GATEWAY_ALARM_JSON=" + defaultGatewayAlarmJSON(tc.alarmJSON),
				"FAKE_VERSION_JSON=" + defaultVersionJSON(tc.versionJSON),
				"FAKE_INFO_VERSION_JSON=" + defaultVersionJSON(tc.infoVersionJSON),
				"FAKE_BASELINE_INFO_METRICS=" + defaultBaselineInfoMetrics(tc.baselineInfoMetrics),
				"FAKE_BASELINE_INFO_METRICS_POD_1=" + tc.baselineInfoMetricsPod1,
				"FAKE_BASELINE_INFO_METRICS_POD_2=" + tc.baselineInfoMetricsPod2,
				"FAKE_INFO_METRICS=" + defaultInfoMetrics(tc.infoMetrics),
				"FAKE_INFO_METRICS_POD_1=" + tc.infoMetricsPod1,
				"FAKE_INFO_METRICS_POD_2=" + tc.infoMetricsPod2,
				"FAKE_LOCAL_STATUS_POD_0=" + defaultLocalStatusJSON(tc.localStatusPod0, "456"),
				"FAKE_LOCAL_STATUS_POD_1=" + defaultLocalStatusJSON(tc.localStatusPod1, "789"),
				"FAKE_LOCAL_STATUS_POD_2=" + defaultLocalStatusJSON(tc.localStatusPod2, "2748"),
				"FAKE_KUBECTL_EXEC_LOG=" + kubectlExecLog,
				"FAKE_KUBECTL_GET_LOG=" + kubectlGetLog,
				"FAKE_CLIENT_METRICS_RESPONSE=" + defaultClientMetricsResponse(tc.clientMetrics),
				"FAKE_INFO_DEBUG_VARS=" + defaultInfoDebugVars(tc.infoDebugVars),
				"FAKE_CLIENT_DEBUG_VARS_RESPONSE=" + defaultClientDebugVarsResponse(tc.clientDebugVars),
				"FAKE_INFO_PPROF_RESPONSE=" + defaultInfoPprofResponse(tc.infoPprof),
				"FAKE_CLIENT_PPROF_RESPONSE=" + defaultClientPprofResponse(tc.clientPprof),
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":222}`,
				`FAKE_DIRECT_HASH_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":222}`,
				`FAKE_DIRECT_HASHKV_JSON={"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":111,"compact_revision":"3","hash_revision":"7"}`,
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
				"FAKE_TIMEOUT_LOG=" + timeoutLog,
			}
			env = append(env, tc.extraEnv...)

			output, err := runProductionScriptCommand(t, "validate-dataplane-readonly.sh", env)
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, string(output), tc.wantOutput)
			for _, unwanted := range tc.wantNotOutput {
				require.NotContains(t, string(output), unwanted)
			}
			if len(tc.wantTimeout) > 0 {
				timeoutBytes, readErr := os.ReadFile(timeoutLog)
				require.NoError(t, readErr)
				for _, want := range tc.wantTimeout {
					require.Contains(t, string(timeoutBytes), want)
				}
			}
			if tc.wantNoCommands {
				require.NoFileExists(t, timeoutLog)
			}
		})
	}
}

func compactJSONString(value string) string {
	return strings.Join(strings.Fields(value), "")
}

func podJSONWithDefaultRuntimeIdentities(t *testing.T, value string) string {
	t.Helper()
	if value == "" {
		return ""
	}
	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(value), &document))
	items, ok := document["items"].([]any)
	require.True(t, ok)
	for _, itemValue := range items {
		item, ok := itemValue.(map[string]any)
		require.True(t, ok)
		preserveMissingContainersReady, _ := item["preserveMissingContainersReady"].(bool)
		delete(item, "preserveMissingContainersReady")
		metadata, ok := item["metadata"].(map[string]any)
		require.True(t, ok)
		name, _ := metadata["name"].(string)
		if _, exists := metadata["uid"]; !exists {
			metadata["uid"] = "uid-" + name
		}
		if _, exists := metadata["generation"]; !exists {
			metadata["generation"] = float64(1)
		}
		if _, exists := metadata["ownerReferences"]; !exists {
			metadata["ownerReferences"] = []any{map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "StatefulSet",
				"name":       "kubebrain",
				"uid":        "uid-kubebrain-statefulset",
				"controller": true,
			}}
		}
		status, ok := item["status"].(map[string]any)
		require.True(t, ok)
		if _, exists := status["phase"]; !exists {
			status["phase"] = "Running"
		}
		if conditions, ok := status["conditions"].([]any); ok {
			var readyCondition map[string]any
			hasContainersReady := false
			for _, conditionValue := range conditions {
				condition, ok := conditionValue.(map[string]any)
				if !ok {
					continue
				}
				conditionType, _ := condition["type"].(string)
				if conditionType == "ContainersReady" {
					hasContainersReady = true
				}
				if conditionType != "Ready" && conditionType != "ContainersReady" {
					continue
				}
				if conditionType == "Ready" && readyCondition == nil {
					readyCondition = condition
				}
				if _, exists := condition["observedGeneration"]; !exists {
					condition["observedGeneration"] = metadata["generation"]
				}
			}
			if readyCondition != nil && !hasContainersReady && !preserveMissingContainersReady {
				conditions = append(conditions, map[string]any{
					"type":               "ContainersReady",
					"status":             readyCondition["status"],
					"observedGeneration": metadata["generation"],
				})
				status["conditions"] = conditions
			}
		}
		if _, exists := status["containerStatuses"]; !exists {
			status["containerStatuses"] = []any{map[string]any{
				"name":         "kubebrain",
				"containerID":  "containerd://" + name,
				"imageID":      "docker.io/library/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"restartCount": 0,
				"ready":        true,
				"started":      true,
				"state": map[string]any{
					"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"},
				},
				"lastState": map[string]any{},
			}}
		}
		for _, statusesKey := range []string{"containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses"} {
			statusesValue, exists := status[statusesKey]
			if !exists {
				continue
			}
			containerStatuses, ok := statusesValue.([]any)
			require.True(t, ok)
			for _, containerStatusValue := range containerStatuses {
				containerStatus, ok := containerStatusValue.(map[string]any)
				require.True(t, ok)
				if _, exists := containerStatus["imageID"]; !exists {
					containerStatus["imageID"] = "docker.io/library/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				}
				if _, exists := containerStatus["started"]; !exists {
					containerStatus["started"] = true
				}
				if _, exists := containerStatus["state"]; !exists {
					containerStatus["state"] = map[string]any{
						"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"},
					}
				}
				if _, exists := containerStatus["lastState"]; !exists {
					containerStatus["lastState"] = map[string]any{}
				}
			}
		}
	}
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	return string(encoded)
}

func defaultReadyzVerbose(value string) string {
	if value != "" {
		return value
	}
	return "[+]data_corruption ok\n[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\nok"
}

func defaultReadyzExcludeData(value string) string {
	if value != "" {
		return value
	}
	return "[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\nok"
}

func defaultLivez(value string) string {
	if value != "" {
		return value
	}
	return "ok"
}

func defaultLivezVerbose(value string) string {
	if value != "" {
		return value
	}
	return "[+]serializable_read ok\nok"
}

func defaultHealthJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"health":"true","reason":""}`
}

func defaultHealthMethodCode(value string) string {
	if value != "" {
		return value
	}
	return "405"
}

func defaultHealthMethodAllow(value string) string {
	if value != "" {
		return value
	}
	return "GET"
}

func defaultHTTPHeaderContentType(value string) string {
	if value != "" {
		return value
	}
	return "text/plain; charset=utf-8"
}

func defaultHTTPHeaderNosniff(value string) string {
	if value != "" {
		return value
	}
	return "nosniff"
}

func defaultVersionHeaderContentType(value string) string {
	if value != "" {
		return value
	}
	return "application/json"
}

func defaultDebugVarsHeaderContentType(value string) string {
	if value != "" {
		return value
	}
	return "application/json; charset=utf-8"
}

func defaultGatewayStatusJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`
}

func defaultDirectStatusJSON(value string) string {
	if value != "" {
		return value
	}
	return statusProbeJSON("3.7.0", "3.7.0", 2147483648, false, "")
}

func statusProbeJSON(version, storageVersion string, dbSizeQuota int64, downgradeEnabled bool, downgradeTarget string) string {
	return fmt.Sprintf(`{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":%q,"db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":7,"errors":[],"db_size_in_use":88,"is_learner":false,"storage_version":%q,"db_size_quota":%d,"downgrade_info":{"enabled":%t,"target_version":%q}}`, version, storageVersion, dbSizeQuota, downgradeEnabled, downgradeTarget)
}

func pre34StatusProbeJSON(version string) string {
	return fmt.Sprintf(`{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":%q,"db_size":99,"leader":456,"raft_index":7,"raft_term":8,"raft_applied_index":0,"errors":[],"db_size_in_use":0,"is_learner":false,"storage_version":"","db_size_quota":0,"downgrade_info":{"enabled":false,"target_version":""}}`, version)
}

func defaultGatewayAuthStatusJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":"5"}`
}

func defaultDirectAuthStatusJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"authRevision":5}`
}

func defaultGatewayAlarmJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"}}`
}

func defaultDirectAlarmJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8}}`
}

func defaultHashHistogramMetrics(metric, count, sum string) string {
	levels := []string{
		"0.01", "0.02", "0.04", "0.08", "0.16", "0.32", "0.64", "1.28",
		"2.56", "5.12", "10.24", "20.48", "40.96", "81.92", "163.84", "+Inf",
	}
	lines := []string{fmt.Sprintf("# TYPE %s histogram", metric)}
	for _, level := range levels {
		lines = append(lines, fmt.Sprintf(`%s_bucket{cluster="default",le=%q} %s`, metric, level, count))
	}
	lines = append(lines,
		fmt.Sprintf(`%s_sum{cluster="default"} %s`, metric, sum),
		fmt.Sprintf(`%s_count{cluster="default"} %s`, metric, count),
	)
	return strings.Join(lines, "\n")
}

func withHashObservabilityCluster(metrics, cluster string) string {
	return strings.NewReplacer(
		`backend_hashkv_completed_cache_hit{cluster="default"}`, `backend_hashkv_completed_cache_hit{cluster="`+cluster+`"}`,
		`backend_hashkv_completed_cache_miss{cluster="default"}`, `backend_hashkv_completed_cache_miss{cluster="`+cluster+`"}`,
		`etcd_mvcc_hash_duration_seconds_bucket{cluster="default"`, `etcd_mvcc_hash_duration_seconds_bucket{cluster="`+cluster+`"`,
		`etcd_mvcc_hash_duration_seconds_sum{cluster="default"}`, `etcd_mvcc_hash_duration_seconds_sum{cluster="`+cluster+`"}`,
		`etcd_mvcc_hash_duration_seconds_count{cluster="default"}`, `etcd_mvcc_hash_duration_seconds_count{cluster="`+cluster+`"}`,
		`etcd_mvcc_hash_rev_duration_seconds_bucket{cluster="default"`, `etcd_mvcc_hash_rev_duration_seconds_bucket{cluster="`+cluster+`"`,
		`etcd_mvcc_hash_rev_duration_seconds_sum{cluster="default"}`, `etcd_mvcc_hash_rev_duration_seconds_sum{cluster="`+cluster+`"}`,
		`etcd_mvcc_hash_rev_duration_seconds_count{cluster="default"}`, `etcd_mvcc_hash_rev_duration_seconds_count{cluster="`+cluster+`"}`,
	).Replace(metrics)
}

func withServerID(metrics, serverID string) string {
	return strings.ReplaceAll(metrics, `server_id="e3f"`, `server_id="`+serverID+`"`)
}

func withServerRole(metrics, isLeader string) string {
	return strings.ReplaceAll(metrics, `etcd_server_is_leader{cluster="default"} 1`, `etcd_server_is_leader{cluster="default"} `+isLeader)
}

func threeMemberStatusJSON() string {
	return `[
		{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
		{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}},
		{"Endpoint":"http://127.0.0.3:2379","Status":{"header":{"cluster_id":123,"member_id":2748,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}
	]`
}

func threeMemberHashKVJSON() string {
	return `[
		{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}},
		{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}},
		{"Endpoint":"http://127.0.0.3:2379","HashKV":{"header":{"cluster_id":123,"member_id":2748,"revision":7,"raft_term":8},"hash":111,"compact_revision":3,"hash_revision":7}}
	]`
}

func defaultLocalStatusJSON(value, memberID string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"` + memberID + `","revision":"7","raft_term":"8"},"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`
}

func defaultVersionJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.7.0"}`
}

func defaultInfoMetrics(value string) string {
	if value != "" {
		return value
	}
	return strings.Join([]string{
		`# HELP etcd_server_version Which version is running. 1 for 'server_version' label with current version.`,
		`# TYPE etcd_server_version gauge`,
		`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
		`# HELP etcd_cluster_version Which version is running. 1 for 'cluster_version' label with current cluster version.`,
		`# TYPE etcd_cluster_version gauge`,
		`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
		`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
		`etcd_server_id{cluster="default",server_id="e3f"} 1`,
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`etcd_server_client_requests_total{client_api_version="unknown",cluster="default",type="unary"} 0`,
		`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
		`etcd_network_client_grpc_received_bytes_total{cluster="default"} 0`,
		`etcd_network_client_grpc_sent_bytes_total{cluster="default"} 0`,
		`etcd_network_server_stream_failures_total{API="watch",Type="receive",cluster="default"} 0`,
		`etcd_network_server_stream_failures_total{API="watch",Type="send",cluster="default"} 0`,
		`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="receive",cluster="default"} 0`,
		`etcd_network_server_stream_failures_total{API="lease-keepalive",Type="send",cluster="default"} 0`,
		`etcd_mvcc_range_total{cluster="default"} 1`,
		`etcd_mvcc_put_total{cluster="default"} 0`,
		`etcd_mvcc_delete_total{cluster="default"} 0`,
		`etcd_mvcc_txn_total{cluster="default"} 0`,
		`etcd_server_range_duration_seconds_count{cluster="default",success="true"} 1`,
		`etcd_server_apply_duration_seconds_count{cluster="default",op="Put",success="true",version="v3"} 1`,
		`go_info{version="go1.26.5"} 1`,
		`go_goroutines 12`,
		`go_threads 7`,
		`go_gc_gogc_percent 100`,
		`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
		`go_sched_gomaxprocs_threads 80`,
		`# TYPE process_start_time_seconds gauge`,
		`process_start_time_seconds 200`,
		`os_fd_used 64`,
		`os_fd_limit 1048576`,
		`# TYPE etcd_server_has_leader gauge`,
		`etcd_server_has_leader{cluster="default"} 1`,
		`# TYPE etcd_server_is_leader gauge`,
		`etcd_server_is_leader{cluster="default"} 1`,
		`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
		`# TYPE etcd_server_is_learner gauge`,
		`etcd_server_is_learner{cluster="default"} 0`,
		`etcd_server_learner_promote_successes{cluster="default"} 0`,
		`etcd_server_snapshot_apply_in_progress_total{cluster="default"} 0`,
		`etcd_server_heartbeat_send_failures_total{cluster="default"} 0`,
		`etcd_server_slow_apply_total{cluster="default"} 0`,
		`etcd_server_proposals_committed_total{cluster="default"} 0`,
		`etcd_server_proposals_applied_total{cluster="default"} 0`,
		`etcd_server_proposals_pending{cluster="default"} 0`,
		`etcd_server_proposals_failed_total{cluster="default"} 0`,
		`etcd_server_slow_read_indexes_total{cluster="default"} 0`,
		`etcd_server_read_indexes_failed_total{cluster="default"} 0`,
		`etcd_disk_wal_fsync_duration_seconds_count{cluster="default"} 0`,
		`etcd_disk_wal_write_duration_seconds_count{cluster="default"} 0`,
		`etcd_disk_wal_write_bytes_total{cluster="default"} 0`,
		`etcd_debugging_snap_save_marshalling_duration_seconds_count{cluster="default"} 0`,
		`etcd_debugging_snap_save_total_duration_seconds_count{cluster="default"} 0`,
		`etcd_snap_fsync_duration_seconds_count{cluster="default"} 0`,
		`etcd_snap_db_save_total_duration_seconds_count{cluster="default"} 0`,
		`etcd_snap_db_fsync_duration_seconds_count{cluster="default"} 0`,
		`etcd_disk_backend_commit_duration_seconds_count{cluster="default"} 1`,
		`etcd_disk_backend_snapshot_duration_seconds_count{cluster="default"} 0`,
		`etcd_disk_backend_defrag_duration_seconds_count{cluster="default"} 0`,
		`etcd_disk_defrag_inflight{cluster="default"} 0`,
		`etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count{cluster="default"} 0`,
		`etcd_debugging_disk_backend_commit_spill_duration_seconds_count{cluster="default"} 0`,
		`etcd_debugging_disk_backend_commit_write_duration_seconds_count{cluster="default"} 0`,
		`etcd_server_health_success{cluster="default"} 0`,
		`etcd_server_health_failures{cluster="default"} 0`,
		`etcd_debugging_auth_revision{cluster="default"} 1`,
		`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
		`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
		`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
		`etcd_mvcc_db_open_read_transactions{cluster="default"} 0`,
		`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
		`etcd_debugging_mvcc_keys_total{cluster="default"} 4`,
		`etcd_debugging_mvcc_total_put_size_in_bytes{cluster="default"} 0`,
		`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
		`etcd_debugging_mvcc_db_compaction_last{cluster="default"} 0`,
		`etcd_debugging_mvcc_db_compaction_keys_total{cluster="default"} 0`,
		`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
		`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
		`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
		`etcd_debugging_mvcc_events_total{cluster="default"} 0`,
		`watch_range_prefilter_dropped{cluster="default"} 0`,
		`backend_list_by_stream_failed{cluster="default"} 0`,
		`backend_list_by_stream_canceled{cluster="default"} 0`,
		`backend_list_by_stream_limit_satisfied{cluster="default"} 0`,
		`backend_range_stream_spill_active{cluster="default"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="decoded"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="decoded"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="decoded"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="decoded"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="completed",path="latest_metadata"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="quota_exhausted",path="latest_metadata"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="canceled",path="latest_metadata"} 0`,
		`backend_range_stream_spill_outcome{cluster="default",outcome="failed",path="latest_metadata"} 0`,
		`backend_range_stream_spill_wait_seconds_count{cluster="default",path="decoded"} 0`,
		`backend_range_stream_spill_wait_seconds_count{cluster="default",path="latest_metadata"} 0`,
		`etcd_debugging_mvcc_pending_events_total{cluster="default"} 0`,
		`etcd_debugging_server_lease_expired_total{cluster="default"} 0`,
		`etcd_debugging_lease_granted_total{cluster="default"} 0`,
		`etcd_debugging_lease_revoked_total{cluster="default"} 0`,
		`etcd_debugging_lease_renewed_total{cluster="default"} 0`,
		defaultHashHistogramMetrics("etcd_mvcc_hash_duration_seconds", "1", "0.005"),
		defaultHashHistogramMetrics("etcd_mvcc_hash_rev_duration_seconds", "1", "0.005"),
		`# TYPE backend_hashkv_completed_cache_hit counter`,
		`backend_hashkv_completed_cache_hit{cluster="default"} 1`,
		`# TYPE backend_hashkv_completed_cache_miss counter`,
		`backend_hashkv_completed_cache_miss{cluster="default"} 1`,
		`promhttp_metric_handler_requests_in_flight 1`,
		`promhttp_metric_handler_requests_total{code="200"} 1`,
	}, "\n") + "\n"
}

func defaultBaselineInfoMetrics(value string) string {
	if value != "" {
		return value
	}
	return strings.NewReplacer(
		defaultHashHistogramMetrics("etcd_mvcc_hash_duration_seconds", "1", "0.005"),
		defaultHashHistogramMetrics("etcd_mvcc_hash_duration_seconds", "0", "0"),
		defaultHashHistogramMetrics("etcd_mvcc_hash_rev_duration_seconds", "1", "0.005"),
		defaultHashHistogramMetrics("etcd_mvcc_hash_rev_duration_seconds", "0", "0"),
		"backend_hashkv_completed_cache_hit{cluster=\"default\"} 1\n",
		"backend_hashkv_completed_cache_hit{cluster=\"default\"} 0\n",
		"backend_hashkv_completed_cache_miss{cluster=\"default\"} 1\n",
		"backend_hashkv_completed_cache_miss{cluster=\"default\"} 0\n",
	).Replace(defaultInfoMetrics(""))
}

func defaultClientMetricsResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func defaultInfoDebugVars(value string) string {
	if value != "" {
		return value
	}
	return `{"cmdline":["kubebrain"],"memstats":{"Alloc":1}}`
}

func defaultClientDebugVarsResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func defaultInfoPprofResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func defaultClientPprofResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func TestProductionReadinessDataplaneReadonlyExampleIncludesReadonlyAuditFields(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "production_readiness_cn.md"))
	require.NoError(t, err)
	doc := string(data)

	end := strings.Index(doc, "hack/production/validate-dataplane-readonly.sh")
	require.NotEqual(t, -1, end, "dataplane readonly gate command is missing")
	start := strings.LastIndex(doc[:end], "```shell")
	require.NotEqual(t, -1, start, "dataplane readonly shell example is missing")
	example := doc[start:end]

	for _, required := range []string{
		"EXPECTED_STATUS_CLUSTER_ID=",
		"EXPECTED_STATUS_VERSION=",
		"EXPECTED_HASHKV_HASH=",
		"EXPECTED_READYZ_NAMED_CHECKS=1",
		"EXPECTED_LIVEZ_NAMED_CHECKS=1",
		"EXPECTED_HEALTH_EXCLUDE_CHECKS=1",
		"EXPECTED_HEALTH_METHOD_CHECKS=1",
		"EXPECTED_HTTP_HEADER_CHECKS=1",
		"EXPECTED_INFO_METRICS_CHECKS=1",
		"EXPECTED_DEBUG_VARS_CHECKS=1",
		"EXPECTED_PPROF_DISABLED_CHECKS=1",
		"INFO_ENDPOINTS=",
	} {
		require.Contains(t, example, required)
	}
	require.Contains(t, doc, "所有 Status version 唯一且等于期望 semver")
	require.Contains(t, doc, "同一次 scrape 中，这两个 cache counter 与两个 Hash histogram family 必须携带完全相同的")
	require.Contains(t, doc, "etcd_server_id{cluster=\"<cluster>\",server_id=\"<lower-hex>\"}")
	require.Contains(t, doc, "探针结束时任何 cluster 或 server ID 漂移都会 fail closed")
	for _, required := range []string{
		"readyz_verbose=ok",
		"readyz_data_corruption=ok",
		"readyz_serializable_read=ok",
		"readyz_linearizable_read=ok",
		"readyz_non_learner=ok",
		"readyz_named_checks=ok",
		"health_exclude_checks=ok",
		"livez=ok",
		"livez_serializable_read=ok",
		"livez_named_checks=ok",
		"health_method_checks=ok",
		"http_header_checks=ok",
		"info_metrics=ok",
		"info_metrics_endpoint=<url>/metrics",
		"info_metrics_server_id=<hex>",
		"info_metrics_status_fence=stable",
		"client_metrics=404",
		"server_identity_metrics=ok",
		"grpc_metrics=ok",
		"client_request_metrics=ok",
		"network_metrics=ok",
		"server_stream_metrics=ok",
		"mvcc_operation_metrics=ok",
		"range_duration_metrics=ok",
		"apply_duration_metrics=optional-ok",
		"runtime_metrics=ok",
		"fd_metrics=ok",
		"server_state_metrics=ok",
		"snapshot_apply_metrics=ok",
		"raft_heartbeat_metrics=ok",
		"slow_apply_metrics=ok",
		"raft_proposal_metrics=ok",
		"wal_metrics=ok",
		"raft_snapshot_file_metrics=ok",
		"backend_commit_metrics=ok",
		"backend_snapshot_metrics=ok",
		"backend_defrag_metrics=ok",
		"health_metrics=ok",
		"auth_metrics=ok",
		"quota_metrics=ok",
		"mvcc_db_size_metrics=ok",
		"mvcc_hash_metrics=ok",
		"hashkv_cache_metrics=ok",
		"mvcc_put_size_metrics=ok",
		"mvcc_pending_event_metrics=ok",
		"mvcc_revision_metrics=ok",
		"mvcc_compaction_metrics=ok",
		"mvcc_watch_metrics=ok",
		"lease_metrics=ok",
		"promhttp_metrics=ok",
		"info_debug_vars=ok",
		"client_debug_vars=404",
		"debug_vars_method_headers=ok",
		"client_pprof=404",
		"info_pprof=404",
		"health=true",
		"serializable_health=true",
		"status_errors=empty",
		"raft_indexes_sampled=true",
		"gateway_status_version=<semver>",
		"gateway_storage_version=<semver>",
		"version_etcdserver=<semver>",
		"version_storage=<semver>",
		"info_version_storage=<semver>",
		"gateway_auth_enabled=<bool>",
		"gateway_auth_status_match=true",
		"direct_alarms=empty",
		"gateway_alarms=empty",
		"gateway_alarm_match=true",
		"direct_hash=<n>",
		"gateway_hash_match=true",
		"gateway_endpoint_member_id=<id>",
		"gateway_endpoint_members_match=true",
		"gateway_endpoint_raft_term=<n>",
		"gateway_endpoint_raft_terms_match=true",
		"gateway_endpoint_revision=<n>",
		"gateway_endpoint_revisions_match=true",
		"gateway_status_body_match=true",
		"gateway_status_errors=empty",
		"direct_status_endpoint_identity_match=true",
		"direct_status_v36_fields_match=true",
		"direct_status_errors=empty",
		"gateway_hashkv_hash=<n>",
		"gateway_hashkv_revisions_match=true",
		"gateway_hashkv_body_match=true",
		"hashkv_endpoint_members_match=true",
		"hashkv_endpoint_revisions_match=true",
		"direct_hashkv_body_match=true",
		"direct_hashkv_hash_revision_match=true",
		"revisions_match=true",
		"hashkv_raft_terms=<unique>",
		"raft_terms_match=true",
		"HashKV raft term 一旦在任一 endpoint 返回就必须覆盖全部 endpoint",
		"同一个静态 MVCC/Raft 观察边界",
	} {
		require.Contains(t, doc, required)
	}
}

func writeDataplaneProbeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
}
