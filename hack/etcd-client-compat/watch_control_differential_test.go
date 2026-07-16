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

type watchControlOutcome struct {
	WatchID      int64
	Created      bool
	Canceled     bool
	CancelReason string
}

func TestWatchControlDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	require.Equal(t,
		runWatchControlScenario(t, reference),
		runWatchControlScenario(t, compatEndpoint()),
	)
}

func runWatchControlScenario(t *testing.T, endpoint string) []watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)

	create := func(key string, id, revision int64) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte(key), WatchId: id, StartRevision: revision,
			}},
		}))
	}
	recv := func() watchControlOutcome {
		resp, err := stream.Recv()
		require.NoError(t, err)
		return watchControlOutcome{
			WatchID: resp.WatchId, Created: resp.Created,
			Canceled: resp.Canceled, CancelReason: resp.CancelReason,
		}
	}

	create("/dbaas-watch-control/a", 42, 0)
	outcomes := []watchControlOutcome{recv()}
	create("/dbaas-watch-control/duplicate", 42, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999}},
	}))
	create("/dbaas-watch-control/b", 43, 0)
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/negative", 44, -1)
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/c", 45, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 42}},
	}))
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.CloseSend())
	return outcomes
}
