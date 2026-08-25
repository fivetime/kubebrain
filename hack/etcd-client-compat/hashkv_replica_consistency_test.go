package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type replicaHashKVSnapshot struct {
	Hash            uint32
	HashRevision    int64
	CompactRevision int64
}

func TestHashKVSnapshotIsConsistentAcrossKubeBrainReplicas(t *testing.T) {
	endpoints := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_MULTI_ENDPOINTS"))
	if len(endpoints) == 0 {
		t.Skip("set KUBEBRAIN_MULTI_ENDPOINTS to direct KubeBrain replica endpoints")
	}
	require.Len(t, endpoints, 3)

	connections := make([]*grpc.ClientConn, 0, len(endpoints))
	kvClients := make([]etcdserverpb.KVClient, 0, len(endpoints))
	maintenanceClients := make([]etcdserverpb.MaintenanceClient, 0, len(endpoints))
	for _, endpoint := range endpoints {
		connection, err := grpc.NewClient(grpcTarget(endpoint),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		connections = append(connections, connection)
		kvClients = append(kvClients, etcdserverpb.NewKVClient(connection))
		maintenanceClients = append(maintenanceClients, etcdserverpb.NewMaintenanceClient(connection))
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			require.NoError(t, connection.Close())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	identity := &liveResponseIdentityAdmission{}
	key := []byte(testPrefix(t) + "/replica-hashkv")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		deleted, deleteErr := kvClients[0].DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
		require.NoError(t, deleteErr)
		require.NoError(t, identity.admitHeader(0, deleted.GetHeader(), 1))
		remaining, getErr := kvClients[0].Range(cleanupCtx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, getErr)
		require.NoError(t, identity.admitHeader(0, remaining.GetHeader(), deleted.GetHeader().GetRevision()))
		require.Empty(t, remaining.Kvs)
	})
	firstPut, err := kvClients[0].Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("first")})
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, firstPut.GetHeader(), 1))
	firstRevision := firstPut.GetHeader().GetRevision()
	firstSnapshots := readReplicaHashKVSnapshots(t, ctx, maintenanceClients, identity, firstRevision)
	requireAllReplicaSnapshotsEqual(t, firstSnapshots)

	secondPut, err := kvClients[len(kvClients)-1].Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("second"),
	})
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(len(kvClients)-1, secondPut.GetHeader(), firstRevision+1))
	secondRevision := secondPut.GetHeader().GetRevision()
	require.Greater(t, secondRevision, firstRevision)
	secondSnapshots := readReplicaHashKVSnapshots(t, ctx, maintenanceClients, identity, secondRevision)
	requireAllReplicaSnapshotsEqual(t, secondSnapshots)
	require.NotEqual(t, firstSnapshots[0].Hash, secondSnapshots[0].Hash)

	firstSnapshotsAfterUpdate := readReplicaHashKVSnapshots(t, ctx, maintenanceClients, identity, firstRevision)
	require.Equal(t, firstSnapshots, firstSnapshotsAfterUpdate,
		"a historical HashKV snapshot must remain immutable after a later write")
}

func readReplicaHashKVSnapshots(
	t *testing.T,
	ctx context.Context,
	clients []etcdserverpb.MaintenanceClient,
	identity *liveResponseIdentityAdmission,
	revision int64,
) []replicaHashKVSnapshot {
	t.Helper()
	snapshots := make([]replicaHashKVSnapshot, 0, len(clients))
	for i, client := range clients {
		response, err := client.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: revision})
		require.NoError(t, err, "replica %d", i)
		require.NotNil(t, response.Header, "replica %d", i)
		require.NoError(t, identity.admitHeader(i, response.Header, revision), "replica %d", i)
		require.GreaterOrEqual(t, response.Header.Revision, revision, "replica %d", i)
		if revision == 0 {
			require.Positive(t, response.HashRevision, "replica %d", i)
		} else {
			require.Equal(t, revision, response.HashRevision, "replica %d", i)
		}
		require.LessOrEqual(t, response.CompactRevision, response.HashRevision, "replica %d", i)
		snapshots = append(snapshots, replicaHashKVSnapshot{
			Hash:            response.Hash,
			HashRevision:    response.HashRevision,
			CompactRevision: response.CompactRevision,
		})
	}
	return snapshots
}

func requireAllReplicaSnapshotsEqual(t *testing.T, snapshots []replicaHashKVSnapshot) {
	t.Helper()
	require.NotEmpty(t, snapshots)
	for i := 1; i < len(snapshots); i++ {
		require.Equal(t, snapshots[0], snapshots[i], "replica %d", i)
	}
}
