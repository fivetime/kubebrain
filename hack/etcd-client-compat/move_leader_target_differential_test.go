package compat

import (
	"context"
	"math"
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

type moveLeaderTargetOutcome struct {
	Target  uint64
	Code    string
	Message string
}

func TestMoveLeaderMissingTargetDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := moveLeaderMissingTargetOutcomes(t, reference)
	require.Equal(t, []moveLeaderTargetOutcome{
		{Target: 0, Code: "FailedPrecondition", Message: "etcdserver: bad leader transferee"},
		{Target: math.MaxUint64, Code: "FailedPrecondition", Message: "etcdserver: bad leader transferee"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, moveLeaderMissingTargetOutcomes(t, compatEndpoint(t)))
}

func moveLeaderMissingTargetOutcomes(t *testing.T, endpoint string) []moveLeaderTargetOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	targets := []uint64{0, math.MaxUint64}
	outcomes := make([]moveLeaderTargetOutcome, 0, len(targets))
	for _, target := range targets {
		_, callErr := client.MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: target})
		outcomes = append(outcomes, moveLeaderTargetOutcome{
			Target: target, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
