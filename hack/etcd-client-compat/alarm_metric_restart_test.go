package compat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestUnknownAlarmMetricRecoversAcrossAllReplicaReplacements proves that the
// gauge is reconstructed from TiKV rather than inherited from process memory.
// It is destructive and must target a disposable three-replica data plane.
func TestUnknownAlarmMetricRecoversAcrossAllReplicaReplacements(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_PODS"))
	if endpoint == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_ENDPOINT, KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_NAMESPACE, and KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_PODS")
	}
	require.Len(t, pods, 3)
	kubeContext := os.Getenv("KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_CONTEXT")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	const (
		memberID = uint64(0xa342801)
		alarm    = etcdserverpb.AlarmType(127)
	)
	disarm := func(callCtx context.Context) {
		_, _ = maintenance.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
		})
	}
	disarm(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		disarm(cleanupCtx)
	})
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	assertAlarmMetricOnPods(t, ctx, kubeContext, namespace, pods, memberID, alarm, 1)

	for _, pod := range pods {
		oldUID := kubectlPodField(t, kubeContext, namespace, pod, "{.metadata.uid}")
		output, deleteErr := runCompatKubectlContext(t, ctx, kubectlContextArgs(kubeContext,
			"-n", namespace, "delete", "pod", pod, "--wait=true", "--timeout=90s")...)
		require.NoErrorf(t, deleteErr, "replace %s: %s", pod, strings.TrimSpace(string(output)))
		require.Eventually(t, func() bool {
			newUID := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.metadata.uid}")
			ready := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.status.containerStatuses[0].ready}")
			return newUID != "" && newUID != oldUID && ready == "true"
		}, 90*time.Second, 500*time.Millisecond, "%s replacement did not become Ready", pod)
		assertAlarmMetricOnPods(t, ctx, kubeContext, namespace, pods, memberID, alarm, 1)
	}

	disarmed, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, disarmed.Alarms)
	assertAlarmMetricOnPods(t, ctx, kubeContext, namespace, pods, memberID, alarm, 0)
}

func assertAlarmMetricOnPods(
	t *testing.T,
	ctx context.Context,
	kubeContext, namespace string,
	pods []string,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
	want float64,
) {
	t.Helper()
	for _, pod := range pods {
		require.Eventually(t, func() bool {
			value, err := kubectlAlarmMetricValue(ctx, kubeContext, namespace, pod, memberID, alarm)
			return err == nil && value == want
		}, 15*time.Second, 200*time.Millisecond, "%s alarm metric did not converge to %v", pod, want)
	}
}

func kubectlAlarmMetricValue(
	ctx context.Context,
	kubeContext, namespace, pod string,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
) (float64, error) {
	args := kubectlContextArgs(kubeContext, "-n", namespace, "exec", pod, "--",
		"curl", "-fsS", "http://127.0.0.1:8080/metrics")
	output, err := runCompatCommand(ctx, "kubectl", args, nil)
	if err != nil {
		return 0, err
	}
	prefix := fmt.Sprintf(`etcd_debugging_server_alarms{alarm_type="%s",server_id="%x"} `,
		alarm.String(), memberID)
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		return strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
	}
	return 0, nil
}
