package compat

import (
	"context"
	"encoding/json"
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

// TestHashKVCompactionRecoversAcrossAllReplicaReplacements is destructive and
// must target a disposable three-replica data plane. Replacing every process
// proves that the compact watermark and logical hash are recovered from TiKV.
func TestHashKVCompactionRecoversAcrossAllReplicaReplacements(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_HASHKV_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_HASHKV_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_HASHKV_RESTART_PODS"))
	if endpoint == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_HASHKV_RESTART_ENDPOINT, KUBEBRAIN_HASHKV_RESTART_NAMESPACE, and KUBEBRAIN_HASHKV_RESTART_PODS")
	}
	require.Len(t, pods, 3)
	kubeContext := os.Getenv("KUBEBRAIN_HASHKV_RESTART_CONTEXT")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	key := []byte(testPrefix(t) + "/hashkv-compaction-restart")
	put := func(value string) int64 {
		t.Helper()
		response, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte(value)})
		require.NoError(t, putErr)
		return response.GetHeader().GetRevision()
	}
	firstRevision := put("first")
	compactRevision := put("second")
	latestRevision := put("latest")
	require.Less(t, firstRevision, compactRevision)
	require.Less(t, compactRevision, latestRevision)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := grpc.NewClient(grpcTarget(endpoint),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if cleanupErr != nil {
			return
		}
		defer cleanupConn.Close()
		_, _ = etcdserverpb.NewKVClient(cleanupConn).DeleteRange(
			cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key},
		)
	})
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRevision})
	require.NoError(t, err)
	time.Sleep(1500 * time.Millisecond)
	baselineResponse, err := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	baseline := replicaHashKVSnapshot{
		Hash:            baselineResponse.Hash,
		HashRevision:    baselineResponse.HashRevision,
		CompactRevision: baselineResponse.CompactRevision,
	}
	require.GreaterOrEqual(t, baseline.HashRevision, latestRevision)
	require.Equal(t, compactRevision, baseline.CompactRevision)
	assertHashKVSnapshotOnPods(t, ctx, kubeContext, namespace, pods, baseline)

	oldUIDs := make(map[string]string, len(pods))
	for _, pod := range pods {
		oldUIDs[pod] = kubectlPodField(t, kubeContext, namespace, pod, "{.metadata.uid}")
	}
	deleteArgs := kubectlContextArgs(kubeContext, "-n", namespace, "delete", "pod")
	deleteArgs = append(deleteArgs, pods...)
	deleteArgs = append(deleteArgs, "--wait=true", "--timeout=90s")
	output, deleteErr := runCompatKubectlContext(t, ctx, deleteArgs...)
	require.NoErrorf(t, deleteErr, "replace all replicas: %s", strings.TrimSpace(string(output)))
	for _, pod := range pods {
		require.Eventually(t, func() bool {
			newUID := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.metadata.uid}")
			ready := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.status.containerStatuses[0].ready}")
			return newUID != "" && newUID != oldUIDs[pod] && ready == "true"
		}, 90*time.Second, 500*time.Millisecond, "%s replacement did not become Ready", pod)
	}
	assertHashKVSnapshotOnPods(t, ctx, kubeContext, namespace, pods, baseline)
}

func assertHashKVSnapshotOnPods(
	t *testing.T,
	ctx context.Context,
	kubeContext, namespace string,
	pods []string,
	want replicaHashKVSnapshot,
) {
	t.Helper()
	for _, pod := range pods {
		require.Eventually(t, func() bool {
			got, err := kubectlPodHashKVSnapshot(ctx, kubeContext, namespace, pod, want.HashRevision)
			return err == nil && got == want
		}, 30*time.Second, 250*time.Millisecond, "%s did not recover HashKV snapshot %+v", pod, want)
	}
}

func kubectlPodHashKVSnapshot(
	ctx context.Context,
	kubeContext, namespace, pod string,
	revision int64,
) (replicaHashKVSnapshot, error) {
	requestBody := fmt.Sprintf(`{"revision":"%d"}`, revision)
	args := kubectlContextArgs(kubeContext, "-n", namespace, "exec", pod, "--",
		"curl", "-fsS", "-X", "POST", "-H", "Content-Type: application/json", "-d", requestBody,
		"http://127.0.0.1:3379/v3/maintenance/hashkv")
	output, err := runCompatCommand(ctx, "kubectl", args, nil)
	if err != nil {
		return replicaHashKVSnapshot{}, fmt.Errorf("query %s HashKV: %w: %s", pod, err, strings.TrimSpace(string(output)))
	}
	var response struct {
		Hash            uint32 `json:"hash"`
		HashRevision    string `json:"hash_revision"`
		CompactRevision string `json:"compact_revision"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		return replicaHashKVSnapshot{}, fmt.Errorf("decode %s HashKV: %w", pod, err)
	}
	hashRevision, err := strconv.ParseInt(response.HashRevision, 10, 64)
	if err != nil {
		return replicaHashKVSnapshot{}, fmt.Errorf("decode %s hash revision: %w", pod, err)
	}
	compactRevision, err := strconv.ParseInt(response.CompactRevision, 10, 64)
	if err != nil {
		return replicaHashKVSnapshot{}, fmt.Errorf("decode %s compact revision: %w", pod, err)
	}
	return replicaHashKVSnapshot{
		Hash: response.Hash, HashRevision: hashRevision, CompactRevision: compactRevision,
	}, nil
}
