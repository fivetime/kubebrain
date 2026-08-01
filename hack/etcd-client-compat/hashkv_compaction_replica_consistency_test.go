package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestHashKVCompactionConvergesAcrossKubeBrainReplicas(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := []byte(testPrefix(t) + "/replica-hashkv-compaction")
	put := func(client etcdserverpb.KVClient, value string) int64 {
		t.Helper()
		response, err := client.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte(value)})
		require.NoError(t, err)
		return response.GetHeader().GetRevision()
	}
	firstRevision := put(kvClients[0], "first")
	compactRevision := put(kvClients[1], "second")
	latestRevision := put(kvClients[2], "latest")
	require.Less(t, firstRevision, compactRevision)
	require.Less(t, compactRevision, latestRevision)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kvClients[0].DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	compactResponse, err := kvClients[1].Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRevision})
	require.NoError(t, err)
	require.NotNil(t, compactResponse.Header)
	require.GreaterOrEqual(t, compactResponse.Header.Revision, latestRevision)

	// The hash cached at the compact boundary can have been computed just before
	// or just after physical compaction. etcd only guarantees that R remains
	// readable, not that its hash is invariant while compaction is progressing.
	for i, client := range maintenanceClients {
		boundary, boundaryErr := client.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: compactRevision})
		require.NoError(t, boundaryErr, "replica %d", i)
		require.Equal(t, compactRevision, boundary.HashRevision, "replica %d", i)
		require.LessOrEqual(t, boundary.CompactRevision, compactRevision, "replica %d", i)
	}

	for i, client := range maintenanceClients {
		_, compactedErr := client.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: firstRevision})
		require.Equal(t, codes.OutOfRange, status.Code(compactedErr), "replica %d", i)
		require.Equal(t, rpctypes.ErrCompacted.Error(), status.Convert(compactedErr).Message(), "replica %d", i)
	}

	// Physical compaction progresses asynchronously. Once it settles, every
	// replica must expose the same latest logical hash and completed watermark.
	time.Sleep(1500 * time.Millisecond)
	latestSnapshots := readReplicaHashKVSnapshots(t, ctx, maintenanceClients, 0)
	requireAllReplicaSnapshotsEqual(t, latestSnapshots)
	require.Equal(t, compactRevision, latestSnapshots[0].CompactRevision)
	require.GreaterOrEqual(t, latestSnapshots[0].HashRevision, latestRevision)
	time.Sleep(250 * time.Millisecond)
	afterPhysicalProgress := readReplicaHashKVSnapshots(t, ctx, maintenanceClients, 0)
	require.Equal(t, latestSnapshots, afterPhysicalProgress)
}
