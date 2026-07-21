package compat

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestCorruptAlarmSurvivesAllReplicaReplacements is destructive and must use a
// disposable cluster. Replacing every serving process proves the alarm is read
// from shared durable storage rather than retained by an in-memory replica.
func TestCorruptAlarmSurvivesAllReplicaReplacements(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_CORRUPT_RESTART_ENDPOINT")
	infoEndpoint := strings.TrimRight(os.Getenv("KUBEBRAIN_CORRUPT_RESTART_INFO_ENDPOINT"), "/")
	namespace := os.Getenv("KUBEBRAIN_CORRUPT_RESTART_NAMESPACE")
	rawPods := os.Getenv("KUBEBRAIN_CORRUPT_RESTART_PODS")
	if endpoint == "" || infoEndpoint == "" || namespace == "" || rawPods == "" {
		t.Skip("set KUBEBRAIN_CORRUPT_RESTART_ENDPOINT, KUBEBRAIN_CORRUPT_RESTART_INFO_ENDPOINT, KUBEBRAIN_CORRUPT_RESTART_NAMESPACE, and KUBEBRAIN_CORRUPT_RESTART_PODS")
	}
	pods := strings.Split(rawPods, ",")
	require.Len(t, pods, 3)
	for index := range pods {
		pods[index] = strings.TrimSpace(pods[index])
		require.NotEmpty(t, pods[index])
	}
	kubeContext := os.Getenv("KUBEBRAIN_CORRUPT_RESTART_CONTEXT")
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResponse.Header.MemberId
	require.NotZero(t, memberID)
	key := []byte(testPrefix(t) + "/corrupt-alarm-restart")
	leasedKey := append(append([]byte(nil), key...), []byte("-leased")...)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: leasedKey, Value: []byte("leased"), Lease: grant.ID})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
		})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: leasedKey})
	})

	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	assertCorruptAlarmState(t, ctx, maintenance, kv, infoEndpoint, memberID, key, leasedKey)

	for _, pod := range pods {
		oldUID := kubectlPodField(t, kubeContext, namespace, pod, "{.metadata.uid}")
		deleteArgs := kubectlContextArgs(kubeContext,
			"-n", namespace, "delete", "pod", pod, "--wait=true", "--timeout=90s")
		output, deleteErr := exec.CommandContext(ctx, "kubectl", deleteArgs...).CombinedOutput()
		require.NoErrorf(t, deleteErr, "replace %s: %s", pod, strings.TrimSpace(string(output)))

		var newUID string
		require.Eventually(t, func() bool {
			newUID = kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.metadata.uid}")
			ready := kubectlPodFieldNoFail(
				kubeContext, namespace, pod, "{.status.containerStatuses[0].ready}",
			)
			return newUID != "" && newUID != oldUID && ready == "true"
		}, 90*time.Second, 500*time.Millisecond, "%s replacement did not become Ready", pod)

		assertCorruptAlarmState(t, ctx, maintenance, kv, infoEndpoint, memberID, key, leasedKey)
	}

	deactivated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
	}}, deactivated.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: leasedKey})
		return rangeErr == nil && len(response.Kvs) == 0
	}, 15*time.Second, 100*time.Millisecond)
}

func assertCorruptAlarmState(
	t *testing.T,
	ctx context.Context,
	maintenance etcdserverpb.MaintenanceClient,
	kv etcdserverpb.KVClient,
	infoEndpoint string,
	memberID uint64,
	key []byte,
	leasedKey []byte,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		alarms, err := maintenance.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_CORRUPT,
		})
		return err == nil && len(alarms.Alarms) == 1 &&
			alarms.Alarms[0].MemberID == memberID && alarms.Alarms[0].Alarm == etcdserverpb.AlarmType_CORRUPT
	}, 30*time.Second, 200*time.Millisecond)

	read, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, read.Kvs, 1)
	require.Equal(t, "before", string(read.Kvs[0].Value))
	leasedRead, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: leasedKey})
	require.NoError(t, err)
	require.Len(t, leasedRead.Kvs, 1)
	require.Equal(t, "leased", string(leasedRead.Kvs[0].Value))
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	require.Equal(t, codes.DataLoss, status.Code(err))
	assertCorruptAlarmHTTPState(t, ctx, infoEndpoint)
}

func assertCorruptAlarmHTTPState(t *testing.T, ctx context.Context, infoEndpoint string) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	for _, check := range []struct {
		path   string
		reason string
	}{
		{path: "/health", reason: "ALARM CORRUPT"},
		{path: "/readyz/data_corruption?verbose", reason: "alarm activated: CORRUPT"},
	} {
		require.Eventually(t, func() bool {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, infoEndpoint+check.path, nil)
			if err != nil {
				return false
			}
			response, err := client.Do(request)
			if err != nil {
				return false
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			return err == nil && response.StatusCode == http.StatusServiceUnavailable &&
				strings.Contains(string(body), check.reason)
		}, 30*time.Second, 200*time.Millisecond, "%s did not report CORRUPT", check.path)
	}
}
