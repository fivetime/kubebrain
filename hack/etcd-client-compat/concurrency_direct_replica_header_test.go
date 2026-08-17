package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConcurrencyResponseHeadersAcrossDirectReplicas(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_DIRECT_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_DIRECT_ENDPOINTS to comma-separated direct replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.GreaterOrEqual(t, len(endpoints), 3)
	requireDistinctDirectReplicaTopology(t, endpoints)

	for index, rawEndpoint := range endpoints {
		endpoint := strings.TrimSpace(rawEndpoint)
		t.Run(endpoint, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			statusResponse, err := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
			require.NoError(t, err)
			require.NotNil(t, statusResponse.Header)
			assertLocal := func(name string, header *etcdserverpb.ResponseHeader) {
				t.Helper()
				require.NotNil(t, header, name)
				require.Equal(t, statusResponse.Header.ClusterId, header.ClusterId, name)
				require.Equal(t, statusResponse.Header.MemberId, header.MemberId, name)
				require.Positive(t, header.RaftTerm, name)
			}

			leaseClient := etcdserverpb.NewLeaseClient(conn)
			lockLease, err := leaseClient.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
			require.NoError(t, err)
			electionLease, err := leaseClient.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
			require.NoError(t, err)
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				_, _ = leaseClient.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: lockLease.ID})
				_, _ = leaseClient.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: electionLease.ID})
			}()
			root := fmt.Sprintf("/registry/etcd-client-compat/direct-concurrency-header/%d/%d", time.Now().UnixNano(), index)

			lockClient := v3lockpb.NewLockClient(conn)
			locked, err := lockClient.Lock(ctx, &v3lockpb.LockRequest{Name: []byte(root + "/lock"), Lease: lockLease.ID})
			require.NoError(t, err)
			assertLocal("lock", locked.Header)
			require.NotEmpty(t, locked.Key)
			unlocked, err := lockClient.Unlock(ctx, &v3lockpb.UnlockRequest{Key: locked.Key})
			require.NoError(t, err)
			assertLocal("unlock", unlocked.Header)

			electionClient := v3electionpb.NewElectionClient(conn)
			name := []byte(root + "/election")
			campaign, err := electionClient.Campaign(ctx, &v3electionpb.CampaignRequest{Name: name, Lease: electionLease.ID, Value: []byte("first")})
			require.NoError(t, err)
			assertLocal("campaign", campaign.Header)
			require.NotNil(t, campaign.Leader)
			require.Equal(t, electionLease.ID, campaign.Leader.Lease)
			leader, err := electionClient.Leader(ctx, &v3electionpb.LeaderRequest{Name: name})
			require.NoError(t, err)
			assertLocal("leader", leader.Header)
			require.NotNil(t, leader.Kv)
			require.Equal(t, []byte("first"), leader.Kv.Value)
			proclaimed, err := electionClient.Proclaim(ctx, &v3electionpb.ProclaimRequest{Leader: campaign.Leader, Value: []byte("second")})
			require.NoError(t, err)
			assertLocal("proclaim", proclaimed.Header)
			observe, err := electionClient.Observe(ctx, &v3electionpb.LeaderRequest{Name: name})
			require.NoError(t, err)
			observed, err := observe.Recv()
			require.NoError(t, err)
			assertLocal("observe", observed.Header)
			require.NotNil(t, observed.Kv)
			require.Equal(t, []byte("second"), observed.Kv.Value)
			resigned, err := electionClient.Resign(ctx, &v3electionpb.ResignRequest{Leader: campaign.Leader})
			require.NoError(t, err)
			assertLocal("resign", resigned.Header)
		})
	}
}
