package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type generatedStagedTxnRangeOutcome struct {
	Nested                bool
	Code, Message         string
	HeaderGap             int64
	MarkerPresent         bool
	RangeHeaderMatchesTxn bool
	InnerHeaderKind       int
	Range                 generatedRangeOutcome
}

func TestGeneratedStagedTxnRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	specs := generatedRangeSpecs()
	referenceOutcomes := runGeneratedStagedTxnRangeScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcomes,
		runGeneratedStagedTxnRangeScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedStagedTxnRangeMatrixCoversEverySpecAtTopAndNestedLevels(t *testing.T) {
	specs := generatedRangeSpecs()
	require.Len(t, specs, 92)
	seen := make(map[string]bool, len(specs)*2)
	for index := range specs {
		for _, nested := range []bool{false, true} {
			key := fmt.Sprintf("%d/%t", index, nested)
			require.False(t, seen[key])
			seen[key] = true
		}
	}
	require.Len(t, seen, 184)
}

func runGeneratedStagedTxnRangeScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedRangeSpec,
) []generatedStagedTxnRangeOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root := fmt.Sprintf("/a3724/generated-staged-txn-range/%s/%d/", instance, time.Now().UnixNano())
	dataPrefix := root + "data/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, root, clientv3.WithPrefix())
	})

	revisions := make([]int64, 10)
	revisions[1] = generatedRangePut(t, ctx, cli, dataPrefix+"a", "va1")
	revisions[2] = generatedRangePut(t, ctx, cli, dataPrefix+"b", "vb1")
	revisions[3] = generatedRangePut(t, ctx, cli, dataPrefix+"c", "vc1")
	revisions[4] = generatedRangePut(t, ctx, cli, dataPrefix+"d", "vd1")
	revisions[5] = generatedRangePut(t, ctx, cli, dataPrefix+"b", "vb2")
	deleted, err := cli.Delete(ctx, dataPrefix+"c")
	require.NoError(t, err)
	revisions[6] = deleted.Header.Revision
	revisions[7] = generatedRangePut(t, ctx, cli, dataPrefix+"c", "vc2")
	revisions[8] = generatedRangePut(t, ctx, cli, dataPrefix+"e", "ve1")
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
	})
	leased, err := cli.Put(ctx, dataPrefix+"f", "vf1", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	revisions[9] = leased.Header.Revision
	revisionOrdinal := make(map[int64]int, len(revisions))
	for ordinal := 1; ordinal < len(revisions); ordinal++ {
		revisionOrdinal[revisions[ordinal]] = ordinal
	}

	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	outcomes := make([]generatedStagedTxnRangeOutcome, 0, len(specs)*2)
	for index, spec := range specs {
		for _, nested := range []bool{false, true} {
			marker := []byte(fmt.Sprintf("%scontrol/%03d/%t", root, index, nested))
			before, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: marker})
			require.NoError(t, rangeErr)
			require.NotNil(t, before.Header)

			rangeOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: generatedRangeRequest(dataPrefix, spec, revisions),
			}}
			putOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: marker, Value: []byte("committed")},
			}}
			request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{putOp, rangeOp}}
			if nested {
				request.Success[1] = &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
					RequestTxn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp}},
				}}
			}

			response, callErr := kv.Txn(ctx, request)
			outcome := generatedStagedTxnRangeOutcome{
				Nested: nested, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			}
			after, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: marker})
			require.NoError(t, rangeErr)
			require.NotNil(t, after.Header)
			outcome.HeaderGap = after.Header.Revision - before.Header.Revision
			outcome.MarkerPresent = len(after.Kvs) == 1
			if callErr == nil {
				require.NotNil(t, response.Header, "case %d nested=%t", index, nested)
				responses := response.Responses
				require.Len(t, responses, 2)
				responses = responses[1:]
				if nested {
					inner := responses[0].GetResponseTxn()
					require.NotNil(t, inner)
					outcome.InnerHeaderKind = generatedTxnInnerHeaderKind(inner.Header, response.Header)
					responses = inner.Responses
				}
				require.Len(t, responses, 1)
				rangeResponse := responses[0].GetResponseRange()
				require.NotNil(t, rangeResponse)
				outcome.RangeHeaderMatchesTxn = rangeResponse.Header != nil &&
					rangeResponse.Header.Revision == response.Header.Revision
				outcome.Range = observeGeneratedTxnRange(rangeResponse, dataPrefix, revisionOrdinal)
				// The response header is the newly committed revision, outside the
				// fixed fixture ordinal map; normalize it to the single expected gap.
				outcome.Range.HeaderRevision = int(response.Header.Revision - before.Header.Revision)
			}
			outcomes = append(outcomes, outcome)
		}
	}
	return outcomes
}
