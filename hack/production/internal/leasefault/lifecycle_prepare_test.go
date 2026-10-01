package leasefault

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

// Real KubeBrain RPCs, synthetic Kubernetes API and dataplane observations.
// This verifies composition, not election, Cilium convergence or fault timing.
func TestPrepareFaultAndRecover(t *testing.T) {
	for _, mode := range []string{
		"success", "owner-mismatch", "unsafe-nonce", "reservation-replaced", "dataplane-not-ready", "create-ambiguous", "after-grant-denied", "after-grant-policy-replaced", "after-grant-nonce-collision",
		"lifecycle-success", "lifecycle-child-fail", "lifecycle-baseline-fail", "lifecycle-deadline", "lifecycle-parent-cancel", "lifecycle-join-fail",
		"lifecycle-evidence-fail", "lifecycle-clock-changed", "lifecycle-outcome-mismatch", "lifecycle-outcome-timeout",
		"lifecycle-owner-lost",
		"lifecycle-activation-nonce-fail", "lifecycle-activation-conflict", "lifecycle-activation-lost-response",
		"lifecycle-activation-original-fail",
		"lifecycle-native-success", "lifecycle-native-gate-fail",
		"lifecycle-native-stale-successor", "lifecycle-native-identity-mismatch",
		"lifecycle-native-runtime-success", "lifecycle-native-runtime-drops-fail",
		"observer-matched", "observer-pending", "observer-input-before", "observer-input-during", "observer-retain-fail", "observer-owner-lost",
		"observer-active", "observer-active-pending", "observer-active-inactive", "observer-active-replaced",
		"observer-check-active", "observer-check-active-inactive", "observer-check-active-replaced",
		"observer-drops", "observer-drops-pending", "observer-drops-bad-output", "observer-drops-wrong-path",
		"observer-drops-input-during", "observer-drops-retain-fail", "observer-drops-inactive", "observer-drops-invalid-duration",
		"observer-drops-wrong-clock", "observer-drops-wrong-exit",
		"nonce-matched", "nonce-pending", "nonce-input-before", "nonce-input-during", "nonce-retain-fail", "nonce-owner-lost", "nonce-owner-after",
	} {
		t.Run(mode, func(t *testing.T) {
			// Bound fixture setup and preparation without treating their total
			// duration as fault availability. testFaultLifecycle separately
			// verifies the original origin+30s limit and expired-clock cases.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			// Fixed term 3 permits synthetic initial=2/successor=3 evidence in
			// lifecycle wiring tests; this service does not perform an election.
			conn := recoveryGRPCFixtureAtTerm(t, 3)
			status, err := pb.NewMaintenanceClient(conn).Status(ctx, &pb.StatusRequest{})
			require.NoError(t, err)
			n := networkPlan()
			p := ProtocolRecovery{Owner: n.Owner, NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, ClusterID: status.Header.ClusterId, AlarmMemberID: status.Header.MemberId, LeaseID: 5178, Key: "/acceptance/lifecycle"}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			obj := func(api, kind, ns, name, uid string) *unstructured.Unstructured {
				u := &unstructured.Unstructured{Object: map[string]interface{}{}}
				u.SetAPIVersion(api)
				u.SetKind(kind)
				u.SetNamespace(ns)
				u.SetName(name)
				u.SetUID(types.UID(uid))
				u.SetResourceVersion("1")
				return u
			}
			pod := obj("v1", "Pod", n.Namespace, n.PodName, n.PodUID)
			pod.SetLabels(map[string]string{"app": "brain"})
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID(n.StatefulSetUID), Controller: &controller}})
			policies := schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{policies: "CiliumNetworkPolicyList", {Version: "v1", Resource: "pods"}: "PodList"}, pod,
				obj("v1", "Namespace", "", n.Namespace, n.NamespaceUID), obj("apps/v1", "StatefulSet", n.Namespace, "brain", n.StatefulSetUID))
			creates := 0
			client.PrependReactor("create", "ciliumnetworkpolicies", func(a ktesting.Action) (bool, runtime.Object, error) {
				creates++
				_, err := LoadNetworkRecovery(dir, n)
				require.NoError(t, err, "intent must precede CREATE")
				u := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				u.SetUID("created-uid")
				u.SetResourceVersion("2")
				require.NoError(t, client.Tracker().Create(policies, u, n.Namespace))
				if mode == "create-ambiguous" {
					return true, nil, errors.New("response lost after CREATE")
				}
				return true, u, nil
			})
			client.PrependReactor("get", "ciliumnetworkpolicies", func(a ktesting.Action) (bool, runtime.Object, error) {
				if mode != "reservation-replaced" {
					return false, nil, nil
				}
				u, err := client.Tracker().Get(policies, n.Namespace, n.PolicyName)
				if err != nil {
					return true, nil, err
				}
				copy := u.(*unstructured.Unstructured).DeepCopy()
				copy.SetUID("replacement")
				return true, copy, nil
			})
			grantAdmissionDenied := false
			restoreDrift := func() {}
			own := func(ctx context.Context) error {
				if strings.HasPrefix(mode, "after-grant-") && !grantAdmissionDenied {
					ttl, err := pb.NewLeaseClient(conn).LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: p.LeaseID})
					if err != nil {
						return err
					}
					if ttl.TTL >= 0 {
						grantAdmissionDenied = true
						switch mode {
						case "after-grant-denied":
							return errors.New("admission lost after lease grant")
						case "after-grant-policy-replaced":
							original, err := client.Tracker().Get(policies, n.Namespace, n.PolicyName)
							require.NoError(t, err)
							changed := original.(*unstructured.Unstructured).DeepCopy()
							changed.SetUID("foreign-policy")
							require.NoError(t, client.Tracker().Update(policies, changed, n.Namespace))
							restoreDrift = func() { require.NoError(t, client.Tracker().Update(policies, original, n.Namespace)) }
						case "after-grant-nonce-collision":
							foreign := pod.DeepCopy()
							foreign.SetName("foreign-pod")
							foreign.SetUID("foreign-uid")
							foreign.SetLabels(map[string]string{"kubebrain.io/fault-owner": n.ReservedNonce})
							pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
							require.NoError(t, client.Tracker().Create(pods, foreign, n.Namespace))
							restoreDrift = func() { require.NoError(t, client.Tracker().Delete(pods, n.Namespace, foreign.GetName())) }
						}
					}
				}
				return nil
			}
			readyChecks := 0
			nonceChecks := 0
			prep := FaultPreparation{Directory: dir, StatefulSetName: "brain", Network: n, Protocol: p, Client: client, Connection: conn, Own: own,
				NoncesSafe: func(context.Context) error {
					nonceChecks++
					if mode == "unsafe-nonce" {
						return errors.New("foreign endpoint")
					}
					return nil
				},
				ReservedReady: func(context.Context) error {
					readyChecks++
					if mode == "dataplane-not-ready" {
						return errors.New("identity pending")
					}
					return nil
				},
			}
			if mode == "owner-mismatch" {
				prep.Protocol.Owner = "foreign"
			}
			if strings.HasPrefix(mode, "nonce-") {
				testNoncePreparation(t, ctx, prep, mode)
				return
			}
			if strings.HasPrefix(mode, "lifecycle-") {
				client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
					u := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
					u.SetUID("claim-uid")
					u.SetResourceVersion("1")
					return false, nil, nil
				})
				owner, err := AcquireFaultOwner(ctx, client, dir, FaultOwnerBinding{Owner: n.Owner, Namespace: n.Namespace, NamespaceUID: n.NamespaceUID, StatefulSetName: "brain", StatefulSetUID: n.StatefulSetUID})
				require.NoError(t, err)
				testFaultLifecycle(t, ctx, prep, owner, mode)
				return
			}
			err = PrepareFault(ctx, prep)
			if mode == "success" {
				t.Logf("complete preparation: nonce snapshots=%d, reserved-ready observations=%d", nonceChecks, readyChecks)
			}
			if strings.HasPrefix(mode, "observer-") {
				require.NoError(t, err)
				testNetworkObserver(t, ctx, prep, mode)
				return
			}
			if mode == "success" || strings.HasPrefix(mode, "after-grant-") {
				if mode == "success" {
					require.NoError(t, err)
				} else if mode == "after-grant-denied" {
					require.ErrorContains(t, err, "admission lost after lease grant")
				} else if mode == "after-grant-policy-replaced" {
					require.ErrorContains(t, err, "inactive reservation replaced")
				} else {
					require.ErrorContains(t, err, "fault selector matches multiple Pods")
				}
				ttl, err := pb.NewLeaseClient(conn).LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: p.LeaseID, Keys: true})
				require.NoError(t, err)
				require.Equal(t, int64(10), ttl.GrantedTTL)
				alarms, err := pb.NewMaintenanceClient(conn).Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET})
				require.NoError(t, err)
				if mode == "success" {
					require.Equal(t, [][]byte{[]byte(p.Key)}, ttl.Keys)
					require.Len(t, alarms.Alarms, 1)
					require.Equal(t, pb.AlarmType_CORRUPT, alarms.Alarms[0].Alarm)
				} else {
					require.Empty(t, ttl.Keys)
					require.Empty(t, alarms.Alarms)
					key, err := pb.NewKVClient(conn).Range(ctx, &pb.RangeRequest{Key: []byte(p.Key)})
					require.NoError(t, err)
					require.Zero(t, key.Count, "drift after Grant must prevent the fixture Txn")
					require.Empty(t, key.Kvs)
				}
				require.Error(t, VerifyProtocolRecovery(ctx, p, conn))
				restoreDrift()
				require.NoError(t, RecoverFault(ctx, FaultRecovery{Directory: dir, StatefulSetName: "brain", Network: n, Protocol: p, Client: client, Connection: conn, Own: own, Join: own, NetworkRestored: own, IdentityRestored: own}))
				require.NoError(t, VerifyProtocolRecovery(ctx, p, conn))
				current, err := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(n.Namespace).Get(ctx, n.PodName, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, map[string]string{"app": "brain"}, current.GetLabels())
			} else {
				require.Error(t, err)
				require.NoError(t, VerifyProtocolRecovery(ctx, p, conn), "failure before protocol setup must leave it untouched")
				_, err = LoadProtocolRecovery(dir, p)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			if mode == "owner-mismatch" || mode == "unsafe-nonce" {
				require.Zero(t, creates)
			} else {
				require.Equal(t, 1, creates)
			}
			if mode == "create-ambiguous" {
				_, err := LoadNetworkReservation(dir, n)
				require.ErrorIs(t, err, os.ErrNotExist)
				_, err = client.Tracker().Get(policies, n.Namespace, n.PolicyName)
				require.NoError(t, err)
				require.Error(t, PrepareFault(ctx, prep))
				require.Equal(t, 1, creates, "ambiguous CREATE must not be retried")
			}
		})
	}
}
