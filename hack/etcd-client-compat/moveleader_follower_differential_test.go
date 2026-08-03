package compat

import (
	"context"
	"os"
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

type moveLeaderFollowerOutcome struct {
	Code    string
	Message string
}

// TestMoveLeaderFollowerDifferentialAgainstReferenceEtcd fixes the serving
// member's leadership check ahead of target-state classification. Direct
// replica endpoints are required because a load balancer can hide which member
// handled this member-local maintenance RPC.
func TestMoveLeaderFollowerDifferentialAgainstReferenceEtcd(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")

	want := []moveLeaderFollowerOutcome{
		{Code: codes.FailedPrecondition.String(), Message: "etcdserver: not leader"},
		{Code: codes.FailedPrecondition.String(), Message: "etcdserver: not leader"},
	}
	reference := moveLeaderFollowerOutcomes(t, referenceEndpoints)
	require.Equal(t, want, reference)
	require.Equal(t, reference, moveLeaderFollowerOutcomes(t, kubeBrainEndpoints))
}

func splitRequiredDirectEndpoints(t *testing.T, envName string) []string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(envName))
	if raw == "" {
		t.Skipf("set %s to three comma-separated direct replica endpoints", envName)
	}
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		if endpoint := strings.TrimSpace(part); endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	require.Len(t, endpoints, 3, envName)
	return endpoints
}

func moveLeaderFollowerOutcomes(t *testing.T, endpoints []string) []moveLeaderFollowerOutcome {
	t.Helper()
	type replica struct {
		conn   *grpc.ClientConn
		status *etcdserverpb.StatusResponse
	}
	replicas := make([]replica, 0, len(endpoints))
	for _, endpoint := range endpoints {
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		statusResponse, statusErr := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
		cancel()
		require.NoError(t, statusErr, endpoint)
		require.NotNil(t, statusResponse.Header, endpoint)
		replicas = append(replicas, replica{conn: conn, status: statusResponse})
	}
	require.NoError(t, validateDirectReplicaTopology([]*etcdserverpb.StatusResponse{
		replicas[0].status, replicas[1].status, replicas[2].status,
	}))

	leaderID := replicas[0].status.Leader
	outcomes := make([]moveLeaderFollowerOutcome, 0, 2)
	for _, replica := range replicas {
		if replica.status.Header.MemberId == leaderID {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := etcdserverpb.NewMaintenanceClient(replica.conn).MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: leaderID})
		cancel()
		outcomes = append(outcomes, moveLeaderFollowerOutcome{
			Code: status.Code(err).String(), Message: status.Convert(err).Message(),
		})
	}
	require.Len(t, outcomes, 2)
	return outcomes
}
