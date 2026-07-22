package compat

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func requireDistinctDirectReplicaTopology(t *testing.T, endpoints []string) {
	t.Helper()
	statuses := make([]*etcdserverpb.StatusResponse, 0, len(endpoints))
	for _, rawEndpoint := range endpoints {
		endpoint := strings.TrimSpace(rawEndpoint)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		statusResponse, statusErr := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
		cancel()
		require.NoError(t, conn.Close())
		require.NoErrorf(t, statusErr, "status direct replica %s", endpoint)
		statuses = append(statuses, statusResponse)
	}
	require.NoError(t, validateDirectReplicaTopology(statuses))
}

func validateDirectReplicaTopology(statuses []*etcdserverpb.StatusResponse) error {
	if len(statuses) < 3 {
		return fmt.Errorf("direct replica topology requires at least three statuses, got %d", len(statuses))
	}
	members := make(map[uint64]struct{}, len(statuses))
	var clusterID, leaderID uint64
	for index, response := range statuses {
		if response == nil || response.Header == nil {
			return fmt.Errorf("direct replica status %d has no header", index)
		}
		if response.Header.ClusterId == 0 || response.Header.MemberId == 0 || response.Leader == 0 {
			return fmt.Errorf("direct replica status %d has zero cluster/member/leader identity", index)
		}
		if index == 0 {
			clusterID = response.Header.ClusterId
			leaderID = response.Leader
		} else if response.Header.ClusterId != clusterID || response.Leader != leaderID {
			return fmt.Errorf("direct replica status %d disagrees on cluster or leader identity", index)
		}
		if _, exists := members[response.Header.MemberId]; exists {
			return fmt.Errorf("direct replica status %d repeats member ID %d", index, response.Header.MemberId)
		}
		members[response.Header.MemberId] = struct{}{}
	}
	if _, exists := members[leaderID]; !exists {
		return fmt.Errorf("reported leader ID %d is not one of the direct replicas", leaderID)
	}
	return nil
}

func TestValidateDirectReplicaTopology(t *testing.T) {
	status := func(clusterID, memberID, leaderID uint64) *etcdserverpb.StatusResponse {
		return &etcdserverpb.StatusResponse{
			Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
			Leader: leaderID,
		}
	}
	valid := []*etcdserverpb.StatusResponse{status(1, 11, 12), status(1, 12, 12), status(1, 13, 12)}
	require.NoError(t, validateDirectReplicaTopology(valid))

	for name, statuses := range map[string][]*etcdserverpb.StatusResponse{
		"too few":          valid[:2],
		"nil response":     {valid[0], nil, valid[2]},
		"zero identity":    {valid[0], status(1, 0, 12), valid[2]},
		"cluster mismatch": {valid[0], status(2, 12, 12), valid[2]},
		"leader mismatch":  {valid[0], status(1, 12, 13), valid[2]},
		"duplicate member": {valid[0], status(1, 11, 12), valid[2]},
		"external leader":  {status(1, 11, 99), status(1, 12, 99), status(1, 13, 99)},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateDirectReplicaTopology(statuses))
		})
	}
}
