package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestReplicatedRestartPreservesState is opt-in because it sequentially
// replaces every KubeBrain, PD, and TiKV Pod in the external test cluster.
func TestReplicatedRestartPreservesState(t *testing.T) {
	kubeContext := os.Getenv("KUBEBRAIN_RESTART_CONTEXT")
	servingNamespace := os.Getenv("KUBEBRAIN_RESTART_NAMESPACE")
	servingPods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_RESTART_PODS"))
	backendNamespace := os.Getenv("KUBEBRAIN_RESTART_BACKEND_NAMESPACE")
	pdPods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_RESTART_PD_PODS"))
	tikvPods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_RESTART_TIKV_PODS"))
	if kubeContext == "" || servingNamespace == "" || backendNamespace == "" ||
		len(servingPods) == 0 || len(pdPods) == 0 || len(tikvPods) == 0 {
		t.Skip("set the explicit KUBEBRAIN_RESTART_* context, namespaces, and Pod lists")
	}
	require.Len(t, servingPods, 3)
	require.Len(t, pdPods, 3)
	require.Len(t, tikvPods, 3)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for replicated restart persistence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	durableKey := prefix + "durable"
	deletedKey := prefix + "deleted"
	leasedKey := prefix + "leased"
	historyKey := prefix + "history"
	binaryPrefix := append([]byte{0xfe}, []byte(prefix+"binary/")...)
	binaryKeys := [][]byte{
		append([]byte(nil), binaryPrefix...),
		append(append([]byte(nil), binaryPrefix...), 0),
		append(append([]byte(nil), binaryPrefix...), 1),
	}
	responseAdmission := &restartResponseAdmission{}

	durablePut, err := cli.Put(ctx, durableKey, "before-restart")
	require.NoError(t, err)
	_, err = responseAdmission.admitPut(durablePut)
	require.NoError(t, err)
	deletedPut, err := cli.Put(ctx, deletedKey, "must-not-return")
	require.NoError(t, err)
	_, err = responseAdmission.admitPut(deletedPut)
	require.NoError(t, err)
	deleted, err := cli.Delete(ctx, deletedKey)
	require.NoError(t, err)
	_, err = responseAdmission.admitDelete(deleted, 1)
	require.NoError(t, err)

	lease, err := cli.Grant(ctx, 900)
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitGrant(lease, 900))
	leasedPut, err := cli.Put(ctx, leasedKey, "leased-before-restart", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	_, err = responseAdmission.admitPut(leasedPut)
	require.NoError(t, err)
	historyPut, err := cli.Put(ctx, historyKey, "replay-after-restart")
	require.NoError(t, err)
	_, err = responseAdmission.admitPut(historyPut)
	require.NoError(t, err)
	beforeRevision := historyPut.Header.Revision
	var binaryStartRevision int64
	binaryPutRevisions := make([]int64, 0, len(binaryKeys))
	for i, key := range binaryKeys {
		put, putErr := cli.Put(ctx, string(key), fmt.Sprintf("binary-%d", i))
		require.NoError(t, putErr)
		_, putAdmissionErr := responseAdmission.admitPut(put)
		require.NoError(t, putAdmissionErr)
		if i == 0 {
			binaryStartRevision = put.Header.Revision
		}
		binaryPutRevisions = append(binaryPutRevisions, put.Header.Revision)
	}
	maintenance := etcdserverpb.NewMaintenanceClient(cli.ActiveConnection())
	statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitStatus(statusResponse))
	alarmMemberID := statusResponse.Header.MemberId
	require.NotZero(t, alarmMemberID)
	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: alarmMemberID,
	})
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitAlarm(activated))
	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanupCancel()
			_, cleanupErr := maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
				Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
				Alarm:    etcdserverpb.AlarmType_CORRUPT,
				MemberID: alarmMemberID,
			})
			return cleanupErr == nil
		}, 15*time.Second, 200*time.Millisecond, "CORRUPT cleanup did not complete")
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		for _, key := range binaryKeys {
			_, _ = cli.Delete(cleanupCtx, string(key))
		}
	})

	errCh := make(chan error, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var previousRevision = beforeRevision
		for {
			select {
			case <-stop:
				return
			default:
			}
			callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
			response, getErr := cli.Get(callCtx, durableKey)
			callCancel()
			if getErr != nil {
				if !isRestartTransient(getErr) {
					select {
					case errCh <- getErr:
					default:
					}
					return
				}
			} else {
				_, admissionErr := responseAdmission.admitRange(response, restartRangeExpectation{
					key:              []byte(durableKey),
					value:            []byte("before-restart"),
					present:          true,
					exactCreate:      durablePut.Header.Revision,
					exactModRevision: durablePut.Header.Revision,
				})
				if admissionErr != nil {
					select {
					case errCh <- admissionErr:
					default:
					}
					return
				}
				if response.Header.Revision < previousRevision {
					select {
					case errCh <- fmt.Errorf("revision regressed from %d to %d", previousRevision, response.Header.Revision):
					default:
					}
					return
				}
				previousRevision = response.Header.Revision
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	for _, pod := range servingPods {
		replaceCompatPod(t, ctx, kubeContext, servingNamespace, pod)
	}
	for _, pod := range pdPods {
		replaceCompatPod(t, ctx, kubeContext, backendNamespace, pod)
	}
	for _, pod := range tikvPods {
		replaceCompatPod(t, ctx, kubeContext, backendNamespace, pod)
	}
	close(stop)
	wg.Wait()
	select {
	case pollErr := <-errCh:
		require.NoError(t, pollErr)
	default:
	}
	requireEndpointReachable(t, grpcTarget(endpoint))

	alarms, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitAlarm(alarms))
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: alarmMemberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
	}}, alarms.Alarms)
	rawKV := etcdserverpb.NewKVClient(cli.ActiveConnection())
	_, err = rawKV.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(durableKey), Value: []byte("blocked-after-restart"),
	})
	require.Equal(t, codes.DataLoss, status.Code(err))
	if infoEndpoint := strings.TrimRight(os.Getenv("KUBEBRAIN_RESTART_INFO_ENDPOINT"), "/"); infoEndpoint != "" {
		assertCorruptAlarmHTTPState(t, ctx, infoEndpoint)
	}
	deactivated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: alarmMemberID,
	})
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitAlarm(deactivated))
	require.Equal(t, alarms.Alarms, deactivated.Alarms)

	durable, err := cli.Get(ctx, durableKey)
	require.NoError(t, err)
	_, err = responseAdmission.admitRange(durable, restartRangeExpectation{
		key: []byte(durableKey), value: []byte("before-restart"), present: true,
		exactCreate: durablePut.Header.Revision, exactModRevision: durablePut.Header.Revision,
	})
	require.NoError(t, err)

	tombstone, err := cli.Get(ctx, deletedKey)
	require.NoError(t, err)
	_, err = responseAdmission.admitRange(tombstone, restartRangeExpectation{key: []byte(deletedKey)})
	require.NoError(t, err)
	historicalDeleted, err := cli.Get(ctx, deletedKey, clientv3.WithRev(deletedPut.Header.Revision))
	require.NoError(t, err)
	_, err = responseAdmission.admitRange(historicalDeleted, restartRangeExpectation{
		key: []byte(deletedKey), value: []byte("must-not-return"), present: true,
		maxKVRevision: deletedPut.Header.Revision, exactCreate: deletedPut.Header.Revision, exactModRevision: deletedPut.Header.Revision,
	})
	require.NoError(t, err)

	leased, err := cli.Get(ctx, leasedKey)
	require.NoError(t, err)
	_, err = responseAdmission.admitRange(leased, restartRangeExpectation{
		key: []byte(leasedKey), value: []byte("leased-before-restart"), present: true, leaseID: lease.ID,
		exactCreate: leasedPut.Header.Revision, exactModRevision: leasedPut.Header.Revision,
	})
	require.NoError(t, err)
	ttl, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitTTL(ttl, lease.ID, lease.TTL, [][]byte{[]byte(leasedKey)}))

	watchCtx, watchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer watchCancel()
	watch := cli.Watch(watchCtx, historyKey, clientv3.WithRev(historyPut.Header.Revision))
	select {
	case response := <-watch:
		require.NoError(t, responseAdmission.admitWatch(response))
		require.Len(t, response.Events, 1)
		require.Equal(t, []byte(historyKey), response.Events[0].Kv.Key)
		require.Equal(t, "replay-after-restart", string(response.Events[0].Kv.Value))
		require.Equal(t, historyPut.Header.Revision, response.Events[0].Kv.CreateRevision)
		require.Equal(t, historyPut.Header.Revision, response.Events[0].Kv.ModRevision)
		require.Equal(t, int64(1), response.Events[0].Kv.Version)
		require.Zero(t, response.Events[0].Kv.Lease)
	case <-watchCtx.Done():
		t.Fatal("timed out replaying persisted watch history")
	}

	binaryWatchCtx, binaryWatchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer binaryWatchCancel()
	binaryWatch := cli.Watch(
		binaryWatchCtx, string(binaryPrefix),
		clientv3.WithPrefix(), clientv3.WithRev(binaryStartRevision),
	)
	var binaryEvents []*clientv3.Event
	for len(binaryEvents) < len(binaryKeys) {
		select {
		case response := <-binaryWatch:
			require.NoError(t, responseAdmission.admitWatch(response))
			binaryEvents = append(binaryEvents, response.Events...)
		case <-binaryWatchCtx.Done():
			t.Fatalf("timed out replaying binary prefix history; got %d events", len(binaryEvents))
		}
	}
	require.Len(t, binaryEvents, len(binaryKeys))
	for i := range binaryKeys {
		require.Equal(t, binaryKeys[i], binaryEvents[i].Kv.Key)
		require.Equal(t, fmt.Sprintf("binary-%d", i), string(binaryEvents[i].Kv.Value))
		require.Equal(t, binaryPutRevisions[i], binaryEvents[i].Kv.CreateRevision)
		require.Equal(t, binaryPutRevisions[i], binaryEvents[i].Kv.ModRevision)
		require.Equal(t, int64(1), binaryEvents[i].Kv.Version)
		require.Zero(t, binaryEvents[i].Kv.Lease)
	}

	after, err := cli.Put(ctx, durableKey, "after-restart")
	require.NoError(t, err)
	_, err = responseAdmission.admitPut(after)
	require.NoError(t, err)
	require.Greater(t, after.Header.Revision, beforeRevision)
	require.Greater(t, after.Header.Revision, durablePut.Header.Revision)
	require.Greater(t, after.Header.Revision, deleted.Header.Revision)

	revoked, err := cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	require.NoError(t, responseAdmission.admitRevoke(revoked))
}

func replaceCompatPod(t *testing.T, ctx context.Context, kubeContext, namespace, pod string) {
	t.Helper()
	oldUID := kubectlPodField(t, kubeContext, namespace, pod, "{.metadata.uid}")
	ownerKind := kubectlPodField(t, kubeContext, namespace, pod,
		"{.metadata.ownerReferences[?(@.controller==true)].kind}")
	require.Equal(t, "StatefulSet", ownerKind, "%s/%s must be controlled by a StatefulSet", namespace, pod)
	deleteArgs := kubectlContextArgs(kubeContext, "-n", namespace, "delete", "pod", pod,
		"--wait=true", "--timeout=240s")
	output, err := runCompatKubectlContext(t, ctx, deleteArgs...)
	require.NoErrorf(t, err, "replace %s/%s: %s", namespace, pod, strings.TrimSpace(string(output)))
	require.Eventually(t, func() bool {
		newUID := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.metadata.uid}")
		ready := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.status.containerStatuses[0].ready}")
		return newUID != "" && newUID != oldUID && ready == "true"
	}, 4*time.Minute, 500*time.Millisecond, "%s/%s replacement did not become Ready", namespace, pod)
}

func isRestartTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true
	default:
		return false
	}
}
