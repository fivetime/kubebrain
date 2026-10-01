package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

type targetedSuccessorConnection struct {
	*successorConnection
	target string
}

func (c *targetedSuccessorConnection) Target() string { return c.target }

func TestVerifiedCommandAdmissionAndJoin(t *testing.T) {
	for _, mode := range []string{"execution-refused", "wrong-member", "tools-changed-after-claim", "tools-changed-during-process", "preconfigured-recovery", "wrong-preparation-member", "missing-preparation", "missing-successor", "wrong-release", "missing-isolated-join-pin", "node-uid", "node-platform", "node-api", "node-post-admit", "endpoint-original", "endpoint-observer", "endpoint-dns", "endpoint-port", "endpoint-probe", "endpoint-no-target", "endpoint-shared-ip", "endpoint-host-network", "deployment-spec", "deployment-generation", "deployment-uid", "deployment-api"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			p.ProbeExecutable, p.StackExecutable = "/bin/bash", "/bin/bash"
			pod := &unstructured.Unstructured{}
			require.NoError(t, pod.UnmarshalJSON([]byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test-ns","uid":"pod-uid","resourceVersion":"1"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":"pinned"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","containerID":"containerd://one","imageID":"sha256:one","restartCount":0,"state":{"running":{"startedAt":"2026-09-20T00:00:00Z"}}}]}}`)))
			var err error
			pod.Object["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "brain", "uid": "sts-uid", "controller": true}}
			image, release := commandReleaseFixture(t)
			if mode == "wrong-release" {
				release.Index = append(release.Index, '\n')
			}
			p.Bindings.Image = image
			pod.Object["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = image
			pod.Object["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["imageID"] = release.Reviewed["linux/amd64"]
			p.Bindings.Network.PodBefore, err = pod.MarshalJSON()
			require.NoError(t, err)
			observer := pod.DeepCopy()
			observer.SetName("brain-1")
			observer.SetUID("observer-uid")
			require.NoError(t, unstructured.SetNestedField(observer.Object, "10.0.0.2", "status", "podIP"))
			if mode == "endpoint-shared-ip" {
				require.NoError(t, unstructured.SetNestedField(observer.Object, "10.0.0.1", "status", "podIP"))
			}
			if mode == "endpoint-host-network" {
				require.NoError(t, unstructured.SetNestedField(observer.Object, true, "spec", "hostNetwork"))
			}
			observerRaw, err := observer.MarshalJSON()
			require.NoError(t, err)
			ns := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "test-ns", "uid": "ns-uid", "resourceVersion": "1"}}}
			sts := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]any{"name": "brain", "namespace": "test-ns", "uid": "sts-uid", "resourceVersion": "1"}}}
			sts.SetGeneration(1)
			sts.Object["status"] = map[string]any{"observedGeneration": int64(1)}
			sts.Object["spec"] = map[string]any{"replicas": int64(3)}
			spec, err := json.Marshal(sts.Object["spec"])
			require.NoError(t, err)
			p.Bindings.Metrics[0].Binding.SpecSHA256 = planinput.SHA256(spec)
			switch mode {
			case "deployment-spec":
				sts.Object["spec"].(map[string]any)["replicas"] = int64(4)
			case "deployment-generation":
				sts.SetGeneration(2)
			case "deployment-uid":
				sts.SetUID("replaced-sts")
			}
			node := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "worker1", "uid": "node-uid"}, "status": map[string]any{"nodeInfo": map[string]any{"operatingSystem": "linux", "architecture": "amd64"}}}}
			if mode == "node-uid" {
				node.SetUID("replaced-node")
			}
			if mode == "node-platform" {
				require.NoError(t, unstructured.SetNestedField(node.Object, "arm64", "status", "nodeInfo", "architecture"))
			}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), pod, observer, ns, sts, node)
			toolsCalls := 0
			processRead := false
			var originalAdmissionTools []int
			client.PrependReactor("get", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
				processRead = true
				return false, nil, nil
			})
			deploymentGets := 0
			client.PrependReactor("get", "statefulsets", func(a ktesting.Action) (bool, runtime.Object, error) {
				deploymentGets++
				if mode == "deployment-api" {
					return true, nil, errors.New("deployment lookup refused")
				}
				return false, nil, nil
			})
			nodeGets := 0
			client.PrependReactor("get", "nodes", func(a ktesting.Action) (bool, runtime.Object, error) {
				nodeGets++
				if mode == "node-api" {
					return true, nil, errors.New("node lookup refused")
				}
				return false, nil, nil
			})
			creates := 0
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				creates++
				u := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				u.SetUID("claim-uid")
				u.SetResourceVersion("1")
				require.NoError(t, client.Tracker().Create(ownerResource, u, "test-ns"))
				return true, u, nil
			})
			connection := func(member uint64) *successorConnection {
				return &successorConnection{read: func(context.Context, int) (*pb.StatusResponse, error) {
					if mode == "wrong-member" {
						member = 0
					}
					return &pb.StatusResponse{Header: &pb.ResponseHeader{ClusterId: p.Bindings.Protocol.ClusterID, MemberId: member, RaftTerm: p.Bindings.InitialTerm}, Leader: p.Bindings.Protocol.AlarmMemberID}, nil
				}}
			}
			p.Endpoint = "10.0.0.1:2379"
			original := &targetedSuccessorConnection{connection(p.Bindings.Protocol.AlarmMemberID), "passthrough:///" + p.Endpoint}
			healthy := &targetedSuccessorConnection{connection(p.Bindings.ObserverMemberID), "passthrough:///10.0.0.2:2379"}
			check := func(context.Context) error { return nil }
			r := MeasuredNetworkFaultRuntime{Network: NetworkFaultRuntime{
				Lifecycle:       FaultLifecycle{Preparation: FaultPreparation{Directory: p.OwnerDirectory, StatefulSetName: "brain", Network: p.Bindings.Network, Protocol: p.Bindings.Protocol, Client: client, Connection: original, Own: func(context.Context) error { return errors.New("preparation deliberately refused") }}, OutcomeAdmit: check, RecoveryTimeout: time.Second},
				ScriptDirectory: "/bin", TargetsSHA256: strings.Repeat("a", 64), AdmitNetwork: check, SuccessorConnection: healthy, AdmitSuccessor: check, CaptureSeconds: 1,
			}, AdmitMetrics: check}
			switch mode {
			case "endpoint-original":
				original.target = healthy.target
			case "endpoint-observer":
				healthy.target = original.target
			case "endpoint-dns":
				healthy.target = "dns:///10.0.0.2:2379"
			case "endpoint-port":
				healthy.target = "passthrough:///10.0.0.2:0"
			case "endpoint-probe":
				p.Endpoint = "10.0.0.3:2379"
			case "endpoint-no-target":
				r.Network.SuccessorConnection = healthy.successorConnection
			}
			if mode == "preconfigured-recovery" {
				r.Network.Lifecycle.RecoveryConnection = healthy
			}
			if mode == "wrong-preparation-member" {
				r.Network.Lifecycle.Preparation.Connection = &targetedSuccessorConnection{connection(p.Bindings.ObserverMemberID), original.target}
			}
			if mode == "missing-preparation" {
				r.Network.Lifecycle.Preparation.Connection = nil
			}
			if mode == "missing-successor" {
				r.Network.SuccessorConnection = nil
			}
			h := ObservationHooks{AdmitOriginal: func(context.Context) error {
				originalAdmissionTools = append(originalAdmissionTools, toolsCalls)
				return nil
			}, AdmitStack: func(context.Context, string) error { return nil }}
			predicate, err := filepath.Abs("../../same-pod-process.jq")
			require.NoError(t, err)
			join := filepath.Join(t.TempDir(), "join.sh")
			if mode == "missing-isolated-join-pin" {
				join = filepath.Join(filepath.Dir(join), IsolatedJoinScript)
			}
			require.NoError(t, os.WriteFile(join, []byte("printf 'joined fixture\\n'\n"), 0600))
			// Exercise the public artifact-to-runtime path, not manually supplied
			// descriptors or precreated worker directories from a fixture.
			p.ProbeLog, p.BeforeLog, p.AfterLog = nil, nil, nil
			artifacts, err := p.PrepareArtifacts("/bin/bash", []MetricCommandTarget{{PodName: p.Bindings.Network.PodName, PodUID: p.Bindings.Network.PodUID, InfoPort: 18600, AnonymousPort: 18601}})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, artifacts.Close()) })
			p = artifacts.Plan
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			changed := errors.New("approved tools changed after claim")
			toolsAdmit := func(context.Context) error {
				toolsCalls++
				if mode == "tools-changed-during-process" && processRead {
					return changed
				}
				if mode == "node-post-admit" && nodeGets > 0 {
					return changed
				}
				if mode == "tools-changed-after-claim" && creates != 0 {
					return changed
				}
				return nil
			}
			result, err := p.RunVerified(ctx, r, h, VerifiedCommandInputs{Release: release, Processes: CommandProcessInputs{JQ: "/usr/bin/jq", Predicate: predicate, Observer: observerRaw, Metrics: []json.RawMessage{p.Bindings.Network.PodBefore}}, MetricExecutable: "/bin/bash", JoinScript: join, Targets: artifacts.Targets, AdmitTools: toolsAdmit})
			require.Error(t, err)
			if mode == "execution-refused" {
				require.GreaterOrEqual(t, len(originalAdmissionTools), 2)
				require.Equal(t, []int{2, 2}, originalAdmissionTools[:2], "one source guard brackets the whole original process check, rather than each internal member check")
			}
			if mode == "tools-changed-during-process" {
				require.ErrorIs(t, err, changed, "post-process source guard must reject tools changed during the live read")
			}
			joins, globErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "recovery-join.*.json"))
			require.NoError(t, globErr)
			staticFailure := mode == "preconfigured-recovery" || mode == "missing-preparation" || mode == "missing-successor" || mode == "wrong-release" || mode == "missing-isolated-join-pin" || strings.HasPrefix(mode, "endpoint-")
			if mode == "wrong-member" || mode == "wrong-preparation-member" || mode == "tools-changed-during-process" || staticFailure || strings.HasPrefix(mode, "node-") || strings.HasPrefix(mode, "deployment-") {
				require.Zero(t, creates)
				require.Nil(t, result.Owner)
				require.Empty(t, joins)
				if staticFailure {
					require.Empty(t, client.Actions(), "reject conflicting runtime before cluster access")
				}
			} else {
				require.Equal(t, 1, creates)
				require.NotNil(t, result.Owner)
				require.True(t, result.Lifecycle.RecoveryAttempted)
				if mode == "tools-changed-after-claim" {
					require.ErrorIs(t, result.Lifecycle.ExecutionError, changed)
					require.Empty(t, joins, "never execute a changed Join script")
				} else {
					require.Len(t, joins, 1)
					require.Contains(t, err.Error(), "preparation deliberately refused")
				}
			}
			nodeEvidence, nodeErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-node-platform.*.json"))
			require.NoError(t, nodeErr)
			if staticFailure || mode == "tools-changed-during-process" || strings.HasPrefix(mode, "deployment-") {
				require.Zero(t, nodeGets)
				require.Empty(t, nodeEvidence)
			} else {
				require.Equal(t, 1, nodeGets, "one GET for all snapshots on the same Node; no retry")
				require.Len(t, nodeEvidence, 1)
			}
			deploymentEvidence, globErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-deployment.*.json"))
			require.NoError(t, globErr)
			if staticFailure || mode == "tools-changed-during-process" {
				require.Empty(t, deploymentEvidence)
				require.Zero(t, deploymentGets)
			} else {
				require.Len(t, deploymentEvidence, 1)
				if strings.HasPrefix(mode, "deployment-") {
					require.Equal(t, 1, deploymentGets, "one failing GET without retry or claim")
				}
			}
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb())
			}
			// All cases stop before native children start. Closing the parent
			// resources must preserve the evidence and never permit attempt reuse.
			require.NoError(t, artifacts.Close())
			require.FileExists(t, filepath.Join(p.OwnerDirectory, "probe.stderr"))
			for _, dir := range []string{p.BeforeDirectory, p.AfterDirectory} {
				entries, readErr := os.ReadDir(dir)
				require.NoError(t, readErr)
				require.Empty(t, entries)
			}
		})
	}
}
