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

type rangeRevisionBoundaryOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestRangeRevisionBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []rangeRevisionBoundaryOutcome{
		{Name: "negative-point", Code: "OK"},
		{Name: "below-negative-one", Code: "OK"},
		{Name: "negative-range", Code: "OK"},
		{Name: "negative-count", Code: "OK"},
		{Name: "negative-keys", Code: "OK"},
		futureRangeBoundaryOutcome("maximum-point"),
		compactedRangeBoundaryOutcome("txn-selected-negative"),
		{Name: "txn-unselected-negative", Code: "OK"},
		futureRangeBoundaryOutcome("txn-selected-maximum"),
	}
	referenceOutcomes := runRangeRevisionBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runRangeRevisionBoundaryScenario(t, compatEndpoint(t)))
}

func compactedRangeBoundaryOutcome(name string) rangeRevisionBoundaryOutcome {
	return rangeRevisionBoundaryOutcome{
		Name: name, Code: "OutOfRange",
		Message: "etcdserver: mvcc: required revision has been compacted",
	}
}

func futureRangeBoundaryOutcome(name string) rangeRevisionBoundaryOutcome {
	return rangeRevisionBoundaryOutcome{
		Name: name, Code: "OutOfRange",
		Message: "etcdserver: mvcc: required revision is a future revision",
	}
}

func runRangeRevisionBoundaryScenario(t *testing.T, endpoint string) []rangeRevisionBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := []byte("/dbaas-range-revision-boundary/")
	end := []byte("/dbaas-range-revision-boundary0")
	key := append(append([]byte{}, prefix...), 'a')
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	current, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: current.Header.Revision})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: prefix, RangeEnd: end})
	})

	rangeRequests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "negative-point", req: &etcdserverpb.RangeRequest{Key: key, Revision: -1}},
		{name: "below-negative-one", req: &etcdserverpb.RangeRequest{Key: key, Revision: -2}},
		{name: "negative-range", req: &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: end, Revision: -1}},
		{name: "negative-count", req: &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: end, Revision: -1, CountOnly: true}},
		{name: "negative-keys", req: &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: end, Revision: -1, KeysOnly: true}},
		{name: "maximum-point", req: &etcdserverpb.RangeRequest{Key: key, Revision: math.MaxInt64}},
	}
	outcomes := make([]rangeRevisionBoundaryOutcome, 0, 9)
	for _, test := range rangeRequests {
		_, callErr := kv.Range(ctx, test.req)
		outcomes = append(outcomes, normalizeRangeRevisionBoundaryError(test.name, callErr))
	}

	selectedNegative := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rangeRequestOp(&etcdserverpb.RangeRequest{Key: key, Revision: -1})},
	}
	_, callErr := kv.Txn(ctx, selectedNegative)
	outcomes = append(outcomes, normalizeRangeRevisionBoundaryError("txn-selected-negative", callErr))

	unselectedNegative := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{rangeRequestOp(&etcdserverpb.RangeRequest{Key: key})},
		Failure: []*etcdserverpb.RequestOp{rangeRequestOp(&etcdserverpb.RangeRequest{Key: key, Revision: -1})},
	}
	_, callErr = kv.Txn(ctx, unselectedNegative)
	outcomes = append(outcomes, normalizeRangeRevisionBoundaryError("txn-unselected-negative", callErr))

	selectedMaximum := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rangeRequestOp(&etcdserverpb.RangeRequest{
			Key: key, Revision: math.MaxInt64,
		})},
	}
	_, callErr = kv.Txn(ctx, selectedMaximum)
	outcomes = append(outcomes, normalizeRangeRevisionBoundaryError("txn-selected-maximum", callErr))
	return outcomes
}

func normalizeRangeRevisionBoundaryError(name string, err error) rangeRevisionBoundaryOutcome {
	return rangeRevisionBoundaryOutcome{
		Name: name, Code: status.Code(err).String(), Message: status.Convert(err).Message(),
	}
}
