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
)

type watchIDRangeBoundaryOutcome struct {
	Name           string
	WatchID        int64
	Created        bool
	Canceled       bool
	CancelReason   string
	HeaderPositive bool
}

func TestWatchIDRangeBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []watchIDRangeBoundaryOutcome{
		watchCreatedBoundaryOutcome("create-negative-one", -1),
		watchCreatedBoundaryOutcome("create-minimum", math.MinInt64),
		watchCreatedBoundaryOutcome("create-maximum", math.MaxInt64),
		watchCanceledCreateBoundaryOutcome("negative-duplicate-and-equal-range", "etcdserver: mvcc: required revision has been compacted"),
		watchCanceledCreateBoundaryOutcome("duplicate-and-equal-range", "mvcc: watcher range is empty"),
		watchCanceledCreateBoundaryOutcome("duplicate-maximum", "mvcc: duplicate watch ID provided on the WatchStream"),
		watchCanceledCreateBoundaryOutcome("equal-range", "mvcc: watcher range is empty"),
		watchCanceledCreateBoundaryOutcome("descending-range", "mvcc: watcher range is empty"),
		watchCreatedBoundaryOutcome("create-after-errors", 102),
		watchCreatedBoundaryOutcome("automatic-id", 0),
		watchCanceledBoundaryOutcome("cancel-negative-one", -1),
		watchCanceledBoundaryOutcome("cancel-minimum", math.MinInt64),
		watchCanceledBoundaryOutcome("cancel-maximum", math.MaxInt64),
		watchCanceledBoundaryOutcome("cancel-after-errors", 102),
		watchCanceledBoundaryOutcome("cancel-automatic", 0),
		watchCreatedBoundaryOutcome("create-after-unknown-cancel", 103),
		watchCanceledBoundaryOutcome("cancel-final", 103),
	}
	referenceOutcomes := runWatchIDRangeBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runWatchIDRangeBoundaryScenario(t, compatEndpoint()))
}

func watchCreatedBoundaryOutcome(name string, id int64) watchIDRangeBoundaryOutcome {
	return watchIDRangeBoundaryOutcome{Name: name, WatchID: id, Created: true, HeaderPositive: true}
}

func watchCanceledCreateBoundaryOutcome(name, reason string) watchIDRangeBoundaryOutcome {
	return watchIDRangeBoundaryOutcome{
		Name: name, WatchID: -1, Created: true, Canceled: true,
		CancelReason: reason, HeaderPositive: true,
	}
}

func watchCanceledBoundaryOutcome(name string, id int64) watchIDRangeBoundaryOutcome {
	return watchIDRangeBoundaryOutcome{Name: name, WatchID: id, Canceled: true, HeaderPositive: true}
}

func runWatchIDRangeBoundaryScenario(t *testing.T, endpoint string) []watchIDRangeBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })

	outcomes := make([]watchIDRangeBoundaryOutcome, 0, 17)
	recv := func(name string) {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr, name)
		outcomes = append(outcomes, watchIDRangeBoundaryOutcome{
			Name: name, WatchID: resp.WatchId, Created: resp.Created,
			Canceled: resp.Canceled, CancelReason: resp.CancelReason,
			HeaderPositive: resp.Header != nil && resp.Header.Revision > 0,
		})
	}
	createAtRevision := func(name string, id int64, key, end []byte, revision int64) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: key, RangeEnd: end, WatchId: id, StartRevision: revision,
				},
			},
		}))
		recv(name)
	}
	create := func(name string, id int64, key, end []byte) {
		createAtRevision(name, id, key, end, 0)
	}
	cancelWatch := func(name string, id int64) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: id},
			},
		}))
		recv(name)
	}

	create("create-negative-one", -1, []byte("/dbaas-watch-id/negative-one"), nil)
	create("create-minimum", math.MinInt64, []byte("/dbaas-watch-id/minimum"), nil)
	create("create-maximum", math.MaxInt64, []byte("/dbaas-watch-id/maximum"), nil)
	createAtRevision("negative-duplicate-and-equal-range", math.MaxInt64,
		[]byte("/dbaas-watch-id/invalid-negative"), []byte("/dbaas-watch-id/invalid-negative"), -1)
	create("duplicate-and-equal-range", math.MaxInt64, []byte("/dbaas-watch-id/invalid"), []byte("/dbaas-watch-id/invalid"))
	create("duplicate-maximum", math.MaxInt64, []byte("/dbaas-watch-id/duplicate"), nil)
	create("equal-range", 100, []byte("/dbaas-watch-id/equal"), []byte("/dbaas-watch-id/equal"))
	create("descending-range", 101, []byte("/dbaas-watch-id/z"), []byte("/dbaas-watch-id/a"))
	create("create-after-errors", 102, []byte("/dbaas-watch-id/after-errors"), nil)
	create("automatic-id", 0, []byte("/dbaas-watch-id/automatic"), nil)

	cancelWatch("cancel-negative-one", -1)
	cancelWatch("cancel-minimum", math.MinInt64)
	cancelWatch("cancel-maximum", math.MaxInt64)
	cancelWatch("cancel-after-errors", 102)
	cancelWatch("cancel-automatic", 0)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999},
		},
	}))
	create("create-after-unknown-cancel", 103, []byte("/dbaas-watch-id/after-unknown"), nil)
	cancelWatch("cancel-final", 103)
	return outcomes
}
