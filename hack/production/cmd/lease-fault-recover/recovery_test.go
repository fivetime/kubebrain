package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	serveretcd "github.com/kubewharf/kubebrain/pkg/server/etcd"
	"github.com/kubewharf/kubebrain/pkg/server/service"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

// Fixed single leader: real RPC implementation, not a term-change experiment.
type fixturePeer struct{ service.PeerService }

func (fixturePeer) IsLeader() bool                                 { return true }
func (fixturePeer) HasLeader() bool                                { return true }
func (fixturePeer) EpochAndLeadingFresh() (uint64, bool)           { return 0, true }
func (fixturePeer) LeadershipTerm(context.Context) (uint64, error) { return 1, nil }
func (fixturePeer) CurrentLeadershipTerm() uint64                  { return 1 }
func (fixturePeer) GetLeaderInfo() string                          { return "recovery-command-test" }
func (fixturePeer) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: "recovery-command-test", IsLeader: true}, nil
}
func (fixturePeer) EtcdProxyEnabled() bool                 { return false }
func (fixturePeer) SyncReadRevision(context.Context) error { return nil }
func (fixturePeer) Ready() error                           { return nil }

func recoveryService(t *testing.T) func() *grpc.ClientConn {
	t.Helper()
	metrics := mock.NewMinimalMetrics(gomock.NewController(t))
	b := backend.NewBackend(memkv.NewKvStorage(), backend.Config{Identity: "recovery-command-test", EnableEtcdCompatibility: true}, metrics)
	s := serveretcd.New(b, metrics, fixturePeer{})
	t.Cleanup(func() { require.NoError(t, s.Close()); require.NoError(t, b.(interface{ Close() error }).Close()) })
	server := grpc.NewServer(s.ClientServerOptions()...)
	pb.RegisterMaintenanceServer(server, s)
	pb.RegisterLeaseServer(server, s)
	pb.RegisterKVServer(server, s)
	listener := bufconn.Listen(1 << 20)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	return func() *grpc.ClientConn {
		conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		return conn
	}
}

func TestRecoveryCommandCompletesOrRetainsFailure(t *testing.T) {
	for _, mode := range []string{"retained", "identity-failed", "released", "release-proof-failed"} {
		t.Run(mode, func(t *testing.T) {
			identityFails, releaseFails := mode == "identity-failed", mode == "release-proof-failed"
			release := mode == "released" || releaseFails
			p := planFixture(t)
			p.TimeoutSeconds = 15
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			connectRPC := recoveryService(t)
			control := connectRPC()
			defer control.Close()
			status, err := pb.NewMaintenanceClient(control).Status(ctx, &pb.StatusRequest{})
			require.NoError(t, err)
			p.Protocol.ClusterID = status.Header.ClusterId
			p.Protocol.AlarmMemberID = status.Header.MemberId
			object := func(api, kind, ns, name, uid string) *unstructured.Unstructured {
				u := &unstructured.Unstructured{Object: map[string]interface{}{}}
				u.SetAPIVersion(api)
				u.SetKind(kind)
				u.SetNamespace(ns)
				u.SetName(name)
				u.SetUID(types.UID(uid))
				u.SetResourceVersion("1")
				return u
			}
			n := p.Network
			pod := object("v1", "Pod", n.Namespace, n.PodName, n.PodUID)
			pod.SetLabels(map[string]string{"app": "preserved"})
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "kubebrain-local", UID: types.UID(n.StatefulSetUID), Controller: &controller}})
			policies := schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}
			pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{policies: "CiliumNetworkPolicyList", pods: "PodList"}, pod,
				object("v1", "Namespace", "", n.Namespace, n.NamespaceUID), object("apps/v1", "StatefulSet", n.Namespace, "kubebrain-local", n.StatefulSetUID))
			client.PrependReactor("create", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
				u := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
				u.SetUID(types.UID("actual-" + a.GetResource().Resource))
				u.SetResourceVersion("1")
				return false, nil, nil
			})
			owner, err := leasefault.AcquireFaultOwner(ctx, client, p.Directory, leasefault.FaultOwnerBinding{Owner: n.Owner, Namespace: n.Namespace, NamespaceUID: n.NamespaceUID, StatefulSetName: "kubebrain-local", StatefulSetUID: n.StatefulSetUID})
			require.NoError(t, err)
			require.NoError(t, leasefault.ReserveNetwork(ctx, client, p.Directory, n, owner.Check))
			require.NoError(t, leasefault.PrepareNetworkLabel(ctx, client, p.Directory, n, "kubebrain-local", owner.Check))
			require.NoError(t, leasefault.PrepareProtocol(ctx, p.Directory, p.Protocol, control, owner.Check))
			require.Error(t, leasefault.VerifyProtocolRecovery(ctx, p.Protocol, control))
			// These scripts are boundary fixtures only: there are no real fault
			// workers or Cilium endpoints in this test. Never deploy them.
			require.NoError(t, os.WriteFile(p.JoinScript, []byte("set -eu\nprintf joined > \"$1/joined\"\n"), 0600))
			if releaseFails {
				require.NoError(t, os.WriteFile(p.JoinScript, []byte("set -eu\nif [[ -f $1/joined ]]; then touch \"$1/second-join\"; fi\nprintf joined > \"$1/joined\"\n"), 0600))
			}
			require.NoError(t, os.MkdirAll(p.ScriptDirectory, 0700))
			observer := filepath.Join(p.ScriptDirectory, "observe-local-network-restored.sh")
			script := "set -eu\n[[ -f $1/joined ]]\nprintf '%s\\n' \"$2\" >> \"$1/observations\"\n"
			if identityFails {
				script += "[[ $2 != absent-unlabelled ]] || exit 65\n"
			}
			if releaseFails {
				script += "[[ ! -f $1/second-join ]] || exit 65\n"
			}
			script += "printf 'fixture observation\\n'\n"
			require.NoError(t, os.WriteFile(observer, []byte(script), 0600))
			podFile, targetsFile := filepath.Join(p.Directory, "observer-pod.json"), filepath.Join(p.Directory, "observer-targets.json")
			require.NoError(t, os.WriteFile(podFile, n.PodBefore, 0600))
			require.NoError(t, os.WriteFile(targetsFile, []byte("[]"), 0600))
			p.TargetsSHA256 = digest([]byte("[]"))
			pins := map[string]string{}
			for _, path := range []string{p.JoinScript, observer, podFile, targetsFile} {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				p.Files[path] = digest(data)
				pins[path] = digest(data)
			}
			path, approved := savePlan(t, p)
			var out bytes.Buffer
			args := []string{"--plan", path, "--approve-sha256", approved, "--execute"}
			if release {
				args = append(args, "--release")
			}
			err = runWith(ctx, args, &out,
				func(plan) (dynamic.Interface, *grpc.ClientConn, error) { return client, connectRPC(), nil },
				func(ctx context.Context, p plan) error {
					// Inject the fake transport/admitted fixture bundle instead of
					// reading any real host credentials. Verify all executed fixture files.
					p.Files = pins
					return p.verifyFiles(ctx)
				})
			executionErr := err
			if identityFails || releaseFails {
				require.Error(t, err)
				require.Empty(t, out.String())
			} else {
				require.NoError(t, err)
				if release {
					require.Contains(t, out.String(), "RECOVERY_VERIFIED_OWNER_CLAIM_RELEASED_NOT_FAULT_ACCEPTANCE")
				} else {
					require.Contains(t, out.String(), "RECOVERY_VERIFIED_OWNER_CLAIM_RETAINED_NOT_FAULT_ACCEPTANCE")
				}
			}
			require.NoError(t, leasefault.VerifyProtocolRecovery(ctx, p.Protocol, control))
			if release && !releaseFails {
				require.Error(t, owner.Check(ctx))
			} else {
				require.NoError(t, owner.Check(ctx), "default command must retain ownership on success and failure")
			}
			_, getErr := client.Resource(policies).Namespace(n.Namespace).Get(ctx, n.PolicyName, metav1.GetOptions{})
			require.True(t, apierrors.IsNotFound(getErr))
			current, err := client.Resource(pods).Namespace(n.Namespace).Get(ctx, n.PodName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, map[string]string{"app": "preserved"}, current.GetLabels())
			observations, err := os.ReadFile(filepath.Join(p.Directory, "observations"))
			require.NoError(t, err)
			require.Contains(t, string(observations), "absent\n")
			require.Contains(t, string(observations), "absent-unlabelled\n")
			if release {
				logs, err := filepath.Glob(filepath.Join(p.Directory, "recovery-release-return.*.json"))
				require.NoError(t, err)
				require.Len(t, logs, 1)
				if releaseFails {
					require.FileExists(t, filepath.Join(p.Directory, "second-join"))
					require.ErrorContains(t, executionErr, "exit status 65")
				}
				return
			}
			logs, err := filepath.Glob(filepath.Join(p.Directory, "recovery-*.json"))
			require.NoError(t, err)
			require.Greater(t, len(logs), 1)
			stages := map[string]int{}
			failedObservations := 0
			for _, log := range logs {
				info, err := os.Lstat(log)
				require.NoError(t, err)
				require.True(t, info.Mode().IsRegular())
				require.Equal(t, os.FileMode(0600), info.Mode().Perm())
				data, err := os.ReadFile(log)
				require.NoError(t, err)
				var record struct {
					Output []byte    `json:"output"`
					Error  string    `json:"error"`
					At     time.Time `json:"at"`
				}
				require.NoError(t, json.Unmarshal(data, &record))
				require.False(t, record.At.IsZero())
				stage := strings.Split(strings.TrimPrefix(filepath.Base(log), "recovery-"), ".")[0]
				stages[stage]++
				if record.Error != "" {
					require.True(t, identityFails)
					// Label restoration rechecks network admission after its
					// patch, before the final IdentityRestored callback.
					require.Equal(t, "restored", stage)
					failedObservations++
					require.Contains(t, record.Error, "exit status 65")
					require.Empty(t, record.Output)
				} else {
					require.Empty(t, record.Error)
					if stage == "join" {
						require.Empty(t, record.Output)
					} else {
						require.Equal(t, "fixture observation\n", string(record.Output))
					}
				}
			}
			require.Equal(t, 1, stages["join"])
			require.Greater(t, stages["restored"], 1, "network admission is rechecked around protocol and label operations")
			if identityFails {
				require.Equal(t, 1, failedObservations)
				require.Equal(t, 0, stages["unlabelled"], "failed admission stops before the final identity callback")
				require.Len(t, stages, 2)
			} else {
				require.Zero(t, failedObservations)
				require.Equal(t, 1, stages["unlabelled"])
				require.Len(t, stages, 3)
			}
		})
	}
}
