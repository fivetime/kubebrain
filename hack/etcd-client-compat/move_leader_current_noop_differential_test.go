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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type moveLeaderCurrentOutcome struct {
	ServingIsLeader bool
	Code            string
	Message         string
	HeaderIsNil     bool
}

func TestMoveLeaderCurrentLeaderNoopDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := moveLeaderCurrentOutcomeForEndpoint(t, reference)
	require.True(t, referenceOutcome.ServingIsLeader)
	require.Equal(t, "OK", referenceOutcome.Code)
	require.Empty(t, referenceOutcome.Message)
	require.True(t, referenceOutcome.HeaderIsNil)
	kubeBrainOutcome := moveLeaderCurrentOutcomeForEndpoint(t, compatEndpoint(t))
	if kubeBrainOutcome.ServingIsLeader {
		require.Equal(t, referenceOutcome, kubeBrainOutcome)
	} else {
		require.Equal(t, moveLeaderCurrentOutcome{
			ServingIsLeader: false,
			Code:            "FailedPrecondition",
			Message:         "etcdserver: not leader",
			HeaderIsNil:     true,
		}, kubeBrainOutcome)
	}
}

func moveLeaderCurrentOutcomeForEndpoint(t *testing.T, endpoint string) moveLeaderCurrentOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	statusResponse, err := client.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.NotZero(t, statusResponse.GetLeader())

	response, callErr := client.MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: statusResponse.GetLeader()})
	header := response.GetHeader()
	return moveLeaderCurrentOutcome{
		ServingIsLeader: statusResponse.GetHeader().GetMemberId() == statusResponse.GetLeader(),
		Code:            status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		HeaderIsNil: header == nil,
	}
}
