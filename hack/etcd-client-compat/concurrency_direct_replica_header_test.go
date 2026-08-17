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

func TestContendedConcurrencyResponseHeadersAcrossDirectReplicas(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_DIRECT_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_DIRECT_ENDPOINTS to comma-separated direct replica endpoints")
	}
	raw := strings.Split(rawEndpoints, ",")
	require.GreaterOrEqual(t, len(raw), 3)
	endpoints := make([]string, 3)
	for index := range endpoints {
		endpoints[index] = strings.TrimSpace(raw[index])
	}
	requireDistinctDirectReplicaTopology(t, endpoints)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connections := make([]*grpc.ClientConn, len(endpoints))
	statuses := make([]*etcdserverpb.StatusResponse, len(endpoints))
	for index, endpoint := range endpoints {
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		connections[index] = conn
		defer conn.Close()
		statuses[index], err = etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
		require.NoError(t, err)
		require.NotNil(t, statuses[index].Header)
	}
	assertLocal := func(endpointIndex int, name string, header *etcdserverpb.ResponseHeader) {
		t.Helper()
		require.NotNil(t, header, name)
		require.Equal(t, statuses[endpointIndex].Header.ClusterId, header.ClusterId, name)
		require.Equal(t, statuses[endpointIndex].Header.MemberId, header.MemberId, name)
		require.Positive(t, header.RaftTerm, name)
	}

	ownerLease, err := etcdserverpb.NewLeaseClient(connections[0]).LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	waiterLease, err := etcdserverpb.NewLeaseClient(connections[1]).LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		leaseClient := etcdserverpb.NewLeaseClient(connections[0])
		_, _ = leaseClient.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: ownerLease.ID})
		_, _ = leaseClient.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: waiterLease.ID})
	}()
	root := fmt.Sprintf("/registry/etcd-client-compat/direct-concurrency-contended/%d", time.Now().UnixNano())
	kvClient := etcdserverpb.NewKVClient(connections[2])
	waitForQueuedPair := func(name []byte) {
		t.Helper()
		require.Eventually(t, func() bool {
			response, rangeErr := kvClient.Range(ctx, &etcdserverpb.RangeRequest{
				Key: name, RangeEnd: []byte(clientPrefixRangeEnd(string(name))), CountOnly: true,
			})
			return rangeErr == nil && response.Count == 2
		}, 5*time.Second, 10*time.Millisecond, "owner and waiter were not both persisted under %q", name)
	}

	lockClients := []v3lockpb.LockClient{
		v3lockpb.NewLockClient(connections[0]),
		v3lockpb.NewLockClient(connections[1]),
		v3lockpb.NewLockClient(connections[2]),
	}
	ownerLock, err := lockClients[0].Lock(ctx, &v3lockpb.LockRequest{Name: []byte(root + "/lock"), Lease: ownerLease.ID})
	require.NoError(t, err)
	assertLocal(0, "owner lock", ownerLock.Header)
	type lockResult struct {
		response *v3lockpb.LockResponse
		err      error
	}
	waiterLockStarted := make(chan struct{})
	waiterLockDone := make(chan lockResult, 1)
	go func() {
		close(waiterLockStarted)
		response, lockErr := lockClients[1].Lock(ctx, &v3lockpb.LockRequest{Name: []byte(root + "/lock"), Lease: waiterLease.ID})
		waiterLockDone <- lockResult{response: response, err: lockErr}
	}()
	<-waiterLockStarted
	waitForQueuedPair([]byte(root + "/lock"))
	select {
	case result := <-waiterLockDone:
		require.Failf(t, "contended lock returned before release", "response=%v error=%v", result.response, result.err)
	case <-time.After(300 * time.Millisecond):
	}
	releasedOwnerLock, err := lockClients[2].Unlock(ctx, &v3lockpb.UnlockRequest{Key: ownerLock.Key})
	require.NoError(t, err)
	assertLocal(2, "cross-replica owner unlock", releasedOwnerLock.Header)
	waiterLock := <-waiterLockDone
	require.NoError(t, waiterLock.err)
	require.NotNil(t, waiterLock.response)
	assertLocal(1, "waiter lock", waiterLock.response.Header)
	releasedWaiterLock, err := lockClients[0].Unlock(ctx, &v3lockpb.UnlockRequest{Key: waiterLock.response.Key})
	require.NoError(t, err)
	assertLocal(0, "cross-replica waiter unlock", releasedWaiterLock.Header)

	electionClients := []v3electionpb.ElectionClient{
		v3electionpb.NewElectionClient(connections[0]),
		v3electionpb.NewElectionClient(connections[1]),
		v3electionpb.NewElectionClient(connections[2]),
	}
	electionName := []byte(root + "/election")
	ownerCampaign, err := electionClients[0].Campaign(ctx, &v3electionpb.CampaignRequest{Name: electionName, Lease: ownerLease.ID, Value: []byte("owner")})
	require.NoError(t, err)
	assertLocal(0, "owner campaign", ownerCampaign.Header)
	type campaignResult struct {
		response *v3electionpb.CampaignResponse
		err      error
	}
	waiterCampaignStarted := make(chan struct{})
	waiterCampaignDone := make(chan campaignResult, 1)
	go func() {
		close(waiterCampaignStarted)
		response, campaignErr := electionClients[1].Campaign(ctx, &v3electionpb.CampaignRequest{Name: electionName, Lease: waiterLease.ID, Value: []byte("waiter")})
		waiterCampaignDone <- campaignResult{response: response, err: campaignErr}
	}()
	<-waiterCampaignStarted
	waitForQueuedPair(electionName)
	select {
	case result := <-waiterCampaignDone:
		require.Failf(t, "contended campaign returned before resign", "response=%v error=%v", result.response, result.err)
	case <-time.After(300 * time.Millisecond):
	}
	ownerResign, err := electionClients[2].Resign(ctx, &v3electionpb.ResignRequest{Leader: ownerCampaign.Leader})
	require.NoError(t, err)
	assertLocal(2, "cross-replica owner resign", ownerResign.Header)
	waiterCampaign := <-waiterCampaignDone
	require.NoError(t, waiterCampaign.err)
	require.NotNil(t, waiterCampaign.response)
	assertLocal(1, "waiter campaign", waiterCampaign.response.Header)
	require.NotNil(t, waiterCampaign.response.Leader)
	require.Equal(t, waiterLease.ID, waiterCampaign.response.Leader.Lease)
	waiterLeader, err := electionClients[0].Leader(ctx, &v3electionpb.LeaderRequest{Name: electionName})
	require.NoError(t, err)
	assertLocal(0, "cross-replica waiter leader", waiterLeader.Header)
	require.NotNil(t, waiterLeader.Kv)
	require.Equal(t, []byte("waiter"), waiterLeader.Kv.Value)
	waiterResign, err := electionClients[0].Resign(ctx, &v3electionpb.ResignRequest{Leader: waiterCampaign.response.Leader})
	require.NoError(t, err)
	assertLocal(0, "cross-replica waiter resign", waiterResign.Header)
}
