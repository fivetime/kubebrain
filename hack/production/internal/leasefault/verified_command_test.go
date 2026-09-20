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

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestVerifiedCommandAdmissionAndJoin(t *testing.T) {
	for _, mode := range []string{"execution-refused", "wrong-member"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			p.ProbeExecutable, p.StackExecutable = "/bin/bash", "/bin/bash"
			pod := &unstructured.Unstructured{}
			require.NoError(t, pod.UnmarshalJSON([]byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test-ns","uid":"pod-uid","resourceVersion":"1"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":"pinned"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","containerID":"containerd://one","imageID":"sha256:one","restartCount":0,"state":{"running":{"startedAt":"2026-09-20T00:00:00Z"}}}]}}`)))
			var err error
			p.Bindings.Network.PodBefore, err = pod.MarshalJSON()
			require.NoError(t, err)
			observer := pod.DeepCopy()
			observer.SetName("brain-1")
			observer.SetUID("observer-uid")
			observerRaw, err := observer.MarshalJSON()
			require.NoError(t, err)
			ns := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "test-ns", "uid": "ns-uid", "resourceVersion": "1"}}}
			sts := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]any{"name": "brain", "namespace": "test-ns", "uid": "sts-uid", "resourceVersion": "1"}}}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), pod, observer, ns, sts)
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
			original, healthy := connection(p.Bindings.Protocol.AlarmMemberID), connection(p.Bindings.ObserverMemberID)
			check := func(context.Context) error { return nil }
			r := MeasuredNetworkFaultRuntime{Network: NetworkFaultRuntime{
				Lifecycle:       FaultLifecycle{Preparation: FaultPreparation{Directory: p.OwnerDirectory, StatefulSetName: "brain", Network: p.Bindings.Network, Protocol: p.Bindings.Protocol, Client: client, Connection: original, Own: func(context.Context) error { return errors.New("preparation deliberately refused") }}, OutcomeAdmit: check, RecoveryConnection: healthy, RecoveryTimeout: time.Second},
				ScriptDirectory: "/bin", TargetsSHA256: strings.Repeat("a", 64), AdmitNetwork: check, SuccessorConnection: healthy, AdmitSuccessor: check, CaptureSeconds: 1,
			}, AdmitMetrics: check}
			h := ObservationHooks{AdmitOriginal: check, AdmitStack: func(context.Context, string) error { return nil }}
			predicate, err := filepath.Abs("../../same-pod-process.jq")
			require.NoError(t, err)
			join := filepath.Join(t.TempDir(), "join.sh")
			require.NoError(t, os.WriteFile(join, []byte("printf 'joined fixture\\n'\n"), 0600))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := p.RunVerified(ctx, r, h, VerifiedCommandInputs{Processes: CommandProcessInputs{JQ: "/usr/bin/jq", Predicate: predicate, Observer: observerRaw, Metrics: []json.RawMessage{p.Bindings.Network.PodBefore}}, MetricExecutable: "/bin/bash", JoinScript: join, Targets: metricTargets(t, p), OriginalConnection: original, AdmitTools: check})
			require.Error(t, err)
			joins, globErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "recovery-join.*.json"))
			require.NoError(t, globErr)
			if mode == "wrong-member" {
				require.Zero(t, creates)
				require.Nil(t, result.Owner)
				require.Empty(t, joins)
			} else {
				require.Equal(t, 1, creates)
				require.NotNil(t, result.Owner)
				require.True(t, result.Lifecycle.RecoveryAttempted)
				require.Len(t, joins, 1)
				require.Contains(t, err.Error(), "preparation deliberately refused")
			}
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb())
			}
		})
	}
}
