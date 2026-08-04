package compat

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type signedRevisionValidationOutcome struct {
	LogicalCompactCode     string
	LogicalCompactMessage  string
	PhysicalCompactCode    string
	PhysicalCompactMessage string
	WatchCreated           bool
	WatchCanceled          bool
	WatchID                int64
	WatchReason            string
	WatchHeaderPresent     bool
	NextWatchCreated       bool
	NextWatchID            int64
	NextWatchCanceled      bool
}

func TestSignedRevisionValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run signed revision validation differential tests")
	}
	want := signedRevisionValidationOutcome{
		LogicalCompactCode:     "OutOfRange",
		LogicalCompactMessage:  "etcdserver: mvcc: required revision has been compacted",
		PhysicalCompactCode:    "OutOfRange",
		PhysicalCompactMessage: "etcdserver: mvcc: required revision has been compacted",
		WatchCreated:           true,
		WatchCanceled:          true,
		WatchID:                -1,
		WatchReason:            "etcdserver: mvcc: required revision has been compacted",
		WatchHeaderPresent:     true,
		NextWatchCreated:       true,
		NextWatchID:            77,
		NextWatchCanceled:      true,
	}
	referenceOutcome := runSignedRevisionValidation(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runSignedRevisionValidation(t, compatEndpoint(t)))
}

func runSignedRevisionValidation(t *testing.T, endpoint string) signedRevisionValidationOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	kv := etcdserverpb.NewKVClient(conn)
	_, logicalErr := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: math.MinInt64})
	_, physicalErr := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: math.MinInt64, Physical: true})
	logicalStatus := status.Convert(logicalErr)
	physicalStatus := status.Convert(physicalErr)

	watch, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte("/dbaas-signed-revision/z"), RangeEnd: []byte("/dbaas-signed-revision/a"),
			StartRevision: math.MinInt64, WatchId: math.MaxInt64,
		},
	}}))
	negative, err := watch.Recv()
	require.NoError(t, err)
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/dbaas-signed-revision/next"), WatchId: 77},
	}}))
	next, err := watch.Recv()
	require.NoError(t, err)
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 77},
	}}))
	canceled, err := watch.Recv()
	require.NoError(t, err)

	return signedRevisionValidationOutcome{
		LogicalCompactCode:     logicalStatus.Code().String(),
		LogicalCompactMessage:  logicalStatus.Message(),
		PhysicalCompactCode:    physicalStatus.Code().String(),
		PhysicalCompactMessage: physicalStatus.Message(),
		WatchCreated:           negative.Created,
		WatchCanceled:          negative.Canceled,
		WatchID:                negative.WatchId,
		WatchReason:            negative.CancelReason,
		WatchHeaderPresent:     negative.Header != nil,
		NextWatchCreated:       next.Created,
		NextWatchID:            next.WatchId,
		NextWatchCanceled:      canceled.Canceled,
	}
}
