package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestAlarmMutationColdReplicaHeader makes Alarm ACTIVATE the first etcd RPC
// sent directly to a replacement serving process. It prevents a successful
// shared-TiKV mutation from carrying that process's empty revision cache in the
// response header.
func TestAlarmMutationColdReplicaHeader(t *testing.T) {
	directEndpoint := os.Getenv("KUBEBRAIN_COLD_ALARM_ENDPOINT")
	baselineEndpoint := os.Getenv("KUBEBRAIN_COLD_ALARM_BASELINE_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_COLD_ALARM_NAMESPACE")
	victimPod := os.Getenv("KUBEBRAIN_COLD_ALARM_VICTIM_POD")
	if directEndpoint == "" || baselineEndpoint == "" || namespace == "" || victimPod == "" {
		t.Skip("set KUBEBRAIN_COLD_ALARM_ENDPOINT, KUBEBRAIN_COLD_ALARM_BASELINE_ENDPOINT, KUBEBRAIN_COLD_ALARM_NAMESPACE, and KUBEBRAIN_COLD_ALARM_VICTIM_POD")
	}
	kubeContext := os.Getenv("KUBEBRAIN_COLD_ALARM_CONTEXT")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	baselineConn, err := grpc.NewClient(grpcTarget(baselineEndpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, baselineConn.Close()) })
	baselineKV := etcdserverpb.NewKVClient(baselineConn)
	baselineMaintenance := etcdserverpb.NewMaintenanceClient(baselineConn)

	seedKey := []byte(fmt.Sprintf("/a3466/alarm-cold-replica/%d", time.Now().UnixNano()))
	memberID := uint64(time.Now().UnixNano())
	const alarmType = etcdserverpb.AlarmType(125)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = baselineMaintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarmType,
		})
		_, _ = baselineKV.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
	})
	seed, err := baselineKV.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("durable")})
	require.NoError(t, err)

	oldUID := kubectlPodField(t, kubeContext, namespace, victimPod, "{.metadata.uid}")
	deleteArgs := kubectlContextArgs(kubeContext,
		"-n", namespace, "delete", "pod", victimPod, "--wait=true", "--timeout=60s")
	output, err := runCompatKubectlContext(t, ctx, deleteArgs...)
	require.NoErrorf(t, err, "replace cold alarm replica: %s", output)
	require.Eventually(t, func() bool {
		newUID := kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.metadata.uid}")
		ready := kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.status.containerStatuses[0].ready}")
		return newUID != "" && newUID != oldUID && ready == "true"
	}, 90*time.Second, 500*time.Millisecond)
	requireEndpointReachable(t, grpcTarget(directEndpoint))

	directConn, err := grpc.NewClient(grpcTarget(directEndpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directConn.Close()) })
	activated, err := etcdserverpb.NewMaintenanceClient(directConn).Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarmType,
	})
	require.NoError(t, err)
	require.NotNil(t, activated.Header)
	require.GreaterOrEqual(t, activated.Header.Revision, seed.Header.Revision)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarmType}}, activated.Alarms)

	shared, err := baselineMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: alarmType,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarmType}}, shared.Alarms)
}
