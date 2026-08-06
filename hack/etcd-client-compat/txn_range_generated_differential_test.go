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

type generatedTxnRangeOutcome struct {
	Nested              bool
	Code, Message       string
	OuterHeaderRevision int
	InnerHeaderKind     int
	Range               generatedRangeOutcome
}

func TestGeneratedTxnRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	specs := generatedRangeSpecs()
	referenceOutcomes := runGeneratedTxnRangeScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcomes,
		runGeneratedTxnRangeScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedTxnRangeMatrixCoversUnarySpecsAtTopAndNestedLevels(t *testing.T) {
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

func runGeneratedTxnRangeScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedRangeSpec,
) []generatedTxnRangeOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a3723/generated-txn-range/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	revisions := make([]int64, 10)
	revisions[1] = generatedRangePut(t, ctx, cli, prefix+"a", "va1")
	revisions[2] = generatedRangePut(t, ctx, cli, prefix+"b", "vb1")
	revisions[3] = generatedRangePut(t, ctx, cli, prefix+"c", "vc1")
	revisions[4] = generatedRangePut(t, ctx, cli, prefix+"d", "vd1")
	revisions[5] = generatedRangePut(t, ctx, cli, prefix+"b", "vb2")
	deleted, err := cli.Delete(ctx, prefix+"c")
	require.NoError(t, err)
	revisions[6] = deleted.Header.Revision
	revisions[7] = generatedRangePut(t, ctx, cli, prefix+"c", "vc2")
	revisions[8] = generatedRangePut(t, ctx, cli, prefix+"e", "ve1")
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
	})
	leased, err := cli.Put(ctx, prefix+"f", "vf1", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	revisions[9] = leased.Header.Revision
	revisionOrdinal := make(map[int64]int, len(revisions))
	for ordinal := 1; ordinal < len(revisions); ordinal++ {
		revisionOrdinal[revisions[ordinal]] = ordinal
	}

	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	outcomes := make([]generatedTxnRangeOutcome, 0, len(specs)*2)
	for index, spec := range specs {
		for _, nested := range []bool{false, true} {
			rangeOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: generatedRangeRequest(prefix, spec, revisions),
			}}
			request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp}}
			if nested {
				request.Success[0] = &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
					RequestTxn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp}},
				}}
			}
			response, callErr := kv.Txn(ctx, request)
			outcome := generatedTxnRangeOutcome{
				Nested: nested, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			}
			if callErr == nil {
				require.NotNil(t, response.Header, "case %d nested=%t", index, nested)
				outcome.OuterHeaderRevision = revisionOrdinal[response.Header.Revision]
				responses := response.Responses
				if nested {
					require.Len(t, responses, 1)
					inner := responses[0].GetResponseTxn()
					require.NotNil(t, inner)
					outcome.InnerHeaderKind = generatedTxnInnerHeaderKind(inner.Header, response.Header)
					responses = inner.Responses
				}
				require.Len(t, responses, 1)
				rangeResponse := responses[0].GetResponseRange()
				require.NotNil(t, rangeResponse)
				outcome.Range = observeGeneratedTxnRange(rangeResponse, prefix, revisionOrdinal)
			}
			outcomes = append(outcomes, outcome)
		}
	}
	return outcomes
}

func generatedTxnInnerHeaderKind(inner, outer *etcdserverpb.ResponseHeader) int {
	if inner == nil || inner.Revision == 0 {
		return 0
	}
	if outer != nil && inner.Revision == outer.Revision {
		return 1
	}
	return -1
}

func observeGeneratedTxnRange(
	response *etcdserverpb.RangeResponse,
	prefix string,
	revisionOrdinal map[int64]int,
) generatedRangeOutcome {
	outcome := generatedRangeOutcome{
		Code: "OK", HeaderRevision: revisionOrdinal[response.Header.Revision],
		Count: response.Count, More: response.More,
	}
	for _, item := range response.Kvs {
		outcome.KVs = append(outcome.KVs, generatedRangeKV{
			Key: string(item.Key[len(prefix):]), Value: string(item.Value),
			CreateRevision: revisionOrdinal[item.CreateRevision], ModRevision: revisionOrdinal[item.ModRevision],
			Version: item.Version, Leased: item.Lease != 0,
		})
	}
	return outcome
}
