package compat

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type generatedTxnResponse struct {
	Kind      string
	Revision  int64
	Succeeded bool
	Deleted   int64
	KVs       []normalizedKV
	PrevKVs   []normalizedKV
	Children  []generatedTxnResponse
}

type generatedTxnCase struct {
	Succeeded bool
	Revision  int64
	Responses []generatedTxnResponse
	Final     []normalizedKV
}

func TestGeneratedNestedTxnDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceCases := runGeneratedNestedTxnCases(t, reference, "reference")
	kubeBrainCases := runGeneratedNestedTxnCases(t, compatEndpoint(), "kubebrain")
	require.Equal(t, referenceCases, kubeBrainCases)
}

func runGeneratedNestedTxnCases(t *testing.T, endpoint, instance string) []generatedTxnCase {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a369/generated-nested/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	rng := rand.New(rand.NewSource(369))
	outcomes := make([]generatedTxnCase, 0, 32)
	for caseIndex := 0; caseIndex < 32; caseIndex++ {
		casePrefix := fmt.Sprintf("%s%02d/", prefix, caseIndex)
		ctrl, a, b, c := casePrefix+"ctrl", casePrefix+"a", casePrefix+"b", casePrefix+"c"
		outerTrue, middleTrue, innerTrue := rng.Intn(2) == 0, rng.Intn(2) == 0, rng.Intn(2) == 0
		seed, seedErr := cli.Txn(ctx).Then(
			clientv3.OpPut(ctrl, map[bool]string{true: "yes", false: "no"}[outerTrue]),
			clientv3.OpPut(a, map[bool]string{true: "middle", false: "other"}[middleTrue]),
			clientv3.OpPut(b, "seed-b"),
			clientv3.OpPut(c, map[bool]string{true: "inner", false: "other"}[innerTrue]),
		).Commit()
		require.NoError(t, seedErr)
		baseRev := seed.Header.Revision

		inner := clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Value(c), "=", "inner")},
			[]clientv3.Op{
				clientv3.OpPut(casePrefix+"inner-put", "inner-value", clientv3.WithPrevKV()),
				clientv3.OpGet(casePrefix, clientv3.WithPrefix()),
			},
			[]clientv3.Op{clientv3.OpDelete(c, clientv3.WithPrevKV())},
		)
		middle := clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Value(a), "=", "middle")},
			[]clientv3.Op{
				inner,
				clientv3.OpPut(casePrefix+"middle-put", "middle-value", clientv3.WithPrevKV()),
			},
			[]clientv3.Op{
				clientv3.OpDelete(b, clientv3.WithPrevKV()),
				clientv3.OpGet(casePrefix, clientv3.WithPrefix()),
			},
		)
		failure := clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(casePrefix+"missing"), "=", 0)},
			[]clientv3.Op{
				clientv3.OpPut(casePrefix+"failure-put", "failure-value", clientv3.WithPrevKV()),
				clientv3.OpGet(b),
			},
			[]clientv3.Op{clientv3.OpDelete(a, clientv3.WithPrevKV())},
		)
		response, txnErr := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(ctrl), "=", "yes")).
			Then(middle, clientv3.OpGet(casePrefix, clientv3.WithPrefix())).
			Else(failure, clientv3.OpGet(casePrefix, clientv3.WithPrefix())).
			Commit()
		require.NoError(t, txnErr, "case %d", caseIndex)

		final, getErr := cli.Get(ctx, casePrefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		require.NoError(t, getErr)
		outcomes = append(outcomes, generatedTxnCase{
			Succeeded: response.Succeeded,
			Revision:  1,
			Responses: normalizeGeneratedTxnResponses(response.Responses, casePrefix, baseRev, response.Header.Revision),
			Final:     normalizeGeneratedKVs(final.Kvs, casePrefix, baseRev, response.Header.Revision),
		})
	}
	return outcomes
}

func normalizeGeneratedTxnResponses(responses []*etcdserverpb.ResponseOp, prefix string, baseRev, txnRev int64) []generatedTxnResponse {
	result := make([]generatedTxnResponse, 0, len(responses))
	for _, response := range responses {
		switch {
		case response.GetResponseRange() != nil:
			rangeResponse := response.GetResponseRange()
			result = append(result, generatedTxnResponse{
				Kind: "range", Revision: normalizeGeneratedRevision(rangeResponse.Header.Revision, baseRev, txnRev),
				KVs: normalizeGeneratedKVs(rangeResponse.Kvs, prefix, baseRev, txnRev),
			})
		case response.GetResponsePut() != nil:
			putResponse := response.GetResponsePut()
			previous := []*mvccpb.KeyValue{}
			if putResponse.PrevKv != nil {
				previous = append(previous, putResponse.PrevKv)
			}
			result = append(result, generatedTxnResponse{
				Kind: "put", Revision: normalizeGeneratedRevision(putResponse.Header.Revision, baseRev, txnRev),
				PrevKVs: normalizeGeneratedKVs(previous, prefix, baseRev, txnRev),
			})
		case response.GetResponseDeleteRange() != nil:
			deleteResponse := response.GetResponseDeleteRange()
			result = append(result, generatedTxnResponse{
				Kind: "delete", Revision: normalizeGeneratedRevision(deleteResponse.Header.Revision, baseRev, txnRev),
				Deleted: deleteResponse.Deleted, PrevKVs: normalizeGeneratedKVs(deleteResponse.PrevKvs, prefix, baseRev, txnRev),
			})
		case response.GetResponseTxn() != nil:
			txnResponse := response.GetResponseTxn()
			result = append(result, generatedTxnResponse{
				Kind: "txn", Revision: normalizeGeneratedRevision(txnResponse.Header.Revision, baseRev, txnRev), Succeeded: txnResponse.Succeeded,
				Children: normalizeGeneratedTxnResponses(txnResponse.Responses, prefix, baseRev, txnRev),
			})
		}
	}
	// Range responses use key order from the request; keep normalization stable
	// even if a backend legally emits an equivalent unsorted point collection.
	for i := range result {
		sort.Slice(result[i].KVs, func(a, b int) bool { return result[i].KVs[a].Key < result[i].KVs[b].Key })
	}
	return result
}

func normalizeGeneratedRevision(revision, baseRev, txnRev int64) int64 {
	if revision == 0 {
		return 0
	}
	if revision == txnRev {
		return 1
	}
	return revision - baseRev
}

func normalizeGeneratedKVs(kvs []*mvccpb.KeyValue, prefix string, baseRev, txnRev int64) []normalizedKV {
	result := normalizeKVs(kvs, prefix, baseRev)
	for i := range result {
		result[i].CreateRev = normalizeGeneratedRevision(result[i].CreateRev+baseRev, baseRev, txnRev)
		result[i].ModRev = normalizeGeneratedRevision(result[i].ModRev+baseRev, baseRev, txnRev)
	}
	return result
}
