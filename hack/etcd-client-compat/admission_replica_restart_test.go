package compat

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// TestAdmissionQuotaResetsAfterReplicaRestart requires two direct replica
// endpoints from a disposable three-replica KubeBrain deployment configured
// with --max-requests-inflight=1. A long-lived stream occupies the victim's
// sole slot while proving that admission accounting remains replica-local and
// that a replacement process starts with no leaked in-flight count.
func TestAdmissionQuotaResetsAfterReplicaRestart(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_ADMISSION_ENDPOINTS")
	namespace := os.Getenv("KUBEBRAIN_ADMISSION_NAMESPACE")
	victimPod := os.Getenv("KUBEBRAIN_ADMISSION_VICTIM_POD")
	if rawEndpoints == "" || namespace == "" || victimPod == "" {
		t.Skip("set KUBEBRAIN_ADMISSION_ENDPOINTS, KUBEBRAIN_ADMISSION_NAMESPACE, and KUBEBRAIN_ADMISSION_VICTIM_POD")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.Len(t, endpoints, 2, "provide victim and control replica endpoints")
	kubeContext := os.Getenv("KUBEBRAIN_ADMISSION_CONTEXT")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	_, victimHealth := admissionHealthClient(t, endpoints[0])
	_, controlHealth := admissionHealthClient(t, endpoints[1])

	stream, err := victimHealth.Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	initial, err := stream.Recv()
	require.NoError(t, err)
	require.NotEqual(t, healthpb.HealthCheckResponse_UNKNOWN, initial.Status)

	callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = victimHealth.Check(callCtx, &healthpb.HealthCheckRequest{})
	callCancel()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())

	callCtx, callCancel = context.WithTimeout(ctx, 3*time.Second)
	control, err := controlHealth.Check(callCtx, &healthpb.HealthCheckRequest{})
	callCancel()
	require.NoError(t, err)
	require.NotEqual(t, healthpb.HealthCheckResponse_UNKNOWN, control.Status)

	oldUID := kubectlPodField(t, kubeContext, namespace, victimPod, "{.metadata.uid}")
	deleteArgs := kubectlContextArgs(kubeContext,
		"-n", namespace, "delete", "pod", victimPod, "--wait=true", "--timeout=60s")
	output, err := exec.CommandContext(ctx, "kubectl", deleteArgs...).CombinedOutput()
	require.NoError(t, err, string(output))

	streamErr := make(chan error, 1)
	go func() {
		_, recvErr := stream.Recv()
		streamErr <- recvErr
	}()
	select {
	case err = <-streamErr:
		require.Error(t, err, "the old process stream must terminate")
	case <-time.After(30 * time.Second):
		t.Fatal("old admission stream remained open after its Pod was deleted")
	}

	var newUID string
	require.Eventually(t, func() bool {
		newUID = kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.metadata.uid}")
		ready := kubectlPodFieldNoFail(
			kubeContext, namespace, victimPod, "{.status.containerStatuses[0].ready}",
		)
		if newUID == "" || newUID == oldUID || ready != "true" {
			return false
		}
		probeCtx, probeCancel := context.WithTimeout(ctx, 2*time.Second)
		defer probeCancel()
		response, probeErr := victimHealth.Check(probeCtx, &healthpb.HealthCheckRequest{})
		return probeErr == nil && response.Status != healthpb.HealthCheckResponse_UNKNOWN
	}, 90*time.Second, 500*time.Millisecond)

	require.NotEqual(t, oldUID, newUID)
}

func admissionHealthClient(t *testing.T, endpoint string) (*grpc.ClientConn, healthpb.HealthClient) {
	t.Helper()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return conn, healthpb.NewHealthClient(conn)
}
