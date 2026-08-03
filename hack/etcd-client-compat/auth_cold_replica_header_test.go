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

// TestAuthStatusColdReplicaHeader makes AuthStatus the first etcd RPC sent
// directly to a replacement serving process. A leader-side read barrier is a
// no-op, so this specifically protects recovery of the shared durable user
// revision rather than relying on another RPC to warm the replica-local cache.
func TestAuthStatusColdReplicaHeader(t *testing.T) {
	directEndpoint := os.Getenv("KUBEBRAIN_COLD_AUTH_ENDPOINT")
	baselineEndpoint := os.Getenv("KUBEBRAIN_COLD_AUTH_BASELINE_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_COLD_AUTH_NAMESPACE")
	victimPod := os.Getenv("KUBEBRAIN_COLD_AUTH_VICTIM_POD")
	if directEndpoint == "" || baselineEndpoint == "" || namespace == "" || victimPod == "" {
		t.Skip("set KUBEBRAIN_COLD_AUTH_ENDPOINT, KUBEBRAIN_COLD_AUTH_BASELINE_ENDPOINT, KUBEBRAIN_COLD_AUTH_NAMESPACE, and KUBEBRAIN_COLD_AUTH_VICTIM_POD")
	}
	kubeContext := os.Getenv("KUBEBRAIN_COLD_AUTH_CONTEXT")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	baselineConn, err := grpc.NewClient(grpcTarget(baselineEndpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, baselineConn.Close()) })
	baselineKV := etcdserverpb.NewKVClient(baselineConn)

	seedKey := []byte(fmt.Sprintf("/a3467/auth-cold-replica/%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = baselineKV.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
	})
	seed, err := baselineKV.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("durable")})
	require.NoError(t, err)

	oldUID := kubectlPodField(t, kubeContext, namespace, victimPod, "{.metadata.uid}")
	deleteArgs := kubectlContextArgs(kubeContext,
		"-n", namespace, "delete", "pod", victimPod, "--wait=true", "--timeout=60s")
	output, err := runCompatKubectlContext(t, ctx, deleteArgs...)
	require.NoErrorf(t, err, "replace cold auth replica: %s", output)
	require.Eventually(t, func() bool {
		newUID := kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.metadata.uid}")
		ready := kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.status.containerStatuses[0].ready}")
		return newUID != "" && newUID != oldUID && ready == "true"
	}, 90*time.Second, 500*time.Millisecond)

	directConn, err := grpc.NewClient(grpcTarget(directEndpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directConn.Close()) })
	statusResponse, err := etcdserverpb.NewAuthClient(directConn).AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResponse.Header)
	require.GreaterOrEqual(t, statusResponse.Header.Revision, seed.Header.Revision)
	require.NotZero(t, statusResponse.Header.ClusterId)
	require.NotZero(t, statusResponse.Header.MemberId)
	require.Positive(t, statusResponse.Header.RaftTerm)
}
