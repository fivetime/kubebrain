package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rangeOptionOutcome struct {
	Name          string
	Count         int64
	More          bool
	Keys          []string
	Values        []string
	Versions      []int64
	AtTxnRevision []bool
}

func TestRangeOptionInteractionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcome := runRangeOptionInteractionScenario(t, reference, "etcd")
	out := func(name string, count int64, more bool, keys, values []string, versions []int64, atTxn []bool) rangeOptionOutcome {
		return rangeOptionOutcome{Name: name, Count: count, More: more, Keys: keys, Values: values, Versions: versions, AtTxnRevision: atTxn}
	}
	want := []rangeOptionOutcome{
		out("filter-before-limit", 4, false, []string{"b"}, []string{"y"}, []int64{2}, []bool{false}),
		out("filtered-count-only-ignores-limit", 4, false, nil, nil, nil, nil),
		out("contradictory-filters", 4, false, nil, nil, nil, nil),
		out("value-default-ascending", 4, true, []string{"c", "b"}, []string{"a", "y"}, []int64{1, 2}, []bool{false, false}),
		out("create-none-limit-lookahead", 4, true, []string{"c", "b"}, []string{"a", "y"}, []int64{1, 2}, []bool{false, false}),
		out("mod-none-limit-lookahead", 4, true, []string{"c", "a"}, []string{"a", "z"}, []int64{1, 1}, []bool{false, false}),
		out("keys-only-sorts-by-original-value", 4, true, []string{"a", "b"}, []string{"", ""}, []int64{1, 2}, []bool{false, false}),
		out("version-descending", 4, false, []string{"b", "a", "c", "d"}, []string{"y", "z", "a", "n"}, []int64{2, 1, 1, 1}, []bool{false, false, false, false}),
		out("key-max-int-limit", 4, false, []string{"a", "b", "c", "d"}, []string{"z", "y", "a", "n"}, []int64{1, 2, 1, 1}, []bool{false, false, false, false}),
		out("value-none-max-int-limit", 4, false, []string{"c", "d", "b", "a"}, []string{"a", "n", "y", "z"}, []int64{1, 1, 2, 1}, []bool{false, false, false, false}),
		out("txn-staged-put-filter-before-limit", 5, false, []string{"e"}, []string{"0"}, []int64{1}, []bool{true}),
		out("txn-staged-update-count-only", 5, false, nil, nil, nil, nil),
		out("txn-staged-delete-sort-limit", 4, true, []string{"b", "d"}, []string{"updated", "n"}, []int64{3, 1}, []bool{false, false}),
		out("txn-value-none-limit-lookahead", 4, true, []string{"c", "d"}, []string{"a", "n"}, []int64{1, 1}, []bool{false, false}),
		out("txn-value-none-max-int-limit", 4, false, []string{"e", "c", "d", "b"}, []string{"0", "a", "n", "updated"}, []int64{1, 1, 1, 3}, []bool{false, false, false, false}),
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runRangeOptionInteractionScenario(t, compatEndpoint(), "kubebrain"))
}

func runRangeOptionInteractionScenario(t *testing.T, endpoint, instance string) []rangeOptionOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-range-options/%s/%d/", instance, time.Now().UnixNano())
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
	})

	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "d", value: "n"},
		{key: "c", value: "a"},
		{key: "b", value: "m"},
		{key: "a", value: "z"},
	} {
		_, err = client.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + seed.key), Value: []byte(seed.value),
		})
		require.NoError(t, err)
	}
	updateB, err := client.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "b"), Value: []byte("y"),
	})
	require.NoError(t, err)

	requests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{
			name: "filter-before-limit",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision,
				Limit: 1, SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
			},
		},
		{
			name: "filtered-count-only-ignores-limit",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision,
				Limit: 1, CountOnly: true,
			},
		},
		{
			name: "contradictory-filters",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd,
				MinModRevision: updateB.Header.Revision, MaxModRevision: updateB.Header.Revision - 1, Limit: 1,
			},
		},
		{
			name: "value-default-ascending",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, Limit: 2,
				SortTarget: etcdserverpb.RangeRequest_VALUE,
			},
		},
		{
			name: "create-none-limit-lookahead",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, Limit: 2,
				SortTarget: etcdserverpb.RangeRequest_CREATE,
			},
		},
		{
			name: "mod-none-limit-lookahead",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, Limit: 2,
				SortTarget: etcdserverpb.RangeRequest_MOD,
			},
		},
		{
			name: "keys-only-sorts-by-original-value",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, Limit: 2, KeysOnly: true,
				SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
			},
		},
		{
			name: "version-descending",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd,
				SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VERSION,
			},
		},
		{
			name: "key-max-int-limit",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, Limit: math.MaxInt64,
			},
		},
		{
			name: "value-none-max-int-limit",
			req: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: rangeEnd, Limit: math.MaxInt64,
				SortTarget: etcdserverpb.RangeRequest_VALUE,
			},
		},
	}

	outcomes := make([]rangeOptionOutcome, 0, len(requests)+3)
	for _, test := range requests {
		resp, rangeErr := client.Range(ctx, test.req)
		require.NoError(t, rangeErr, test.name)
		outcomes = append(outcomes, rangeOptionResult(test.name, resp, prefix, 0))
	}

	txnCases := []struct {
		name string
		ops  []*etcdserverpb.RequestOp
	}{
		{
			name: "txn-staged-put-filter-before-limit",
			ops: []*etcdserverpb.RequestOp{
				putRequestOp([]byte(prefix+"e"), "0"),
				rangeRequestOp(&etcdserverpb.RangeRequest{
					Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision + 1,
					Limit: 1, SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
				}),
			},
		},
		{
			name: "txn-staged-update-count-only",
			ops: []*etcdserverpb.RequestOp{
				putRequestOp([]byte(prefix+"b"), "updated"),
				rangeRequestOp(&etcdserverpb.RangeRequest{
					Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision + 1,
					Limit: 1, CountOnly: true,
				}),
			},
		},
		{
			name: "txn-staged-delete-sort-limit",
			ops: []*etcdserverpb.RequestOp{
				deleteRequestOp([]byte(prefix+"a"), nil),
				rangeRequestOp(&etcdserverpb.RangeRequest{
					Key: []byte(prefix), RangeEnd: rangeEnd, Limit: 2,
					SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
				}),
			},
		},
		{
			name: "txn-value-none-limit-lookahead",
			ops: []*etcdserverpb.RequestOp{
				rangeRequestOp(&etcdserverpb.RangeRequest{
					Key: []byte(prefix), RangeEnd: rangeEnd, Limit: 2,
					SortTarget: etcdserverpb.RangeRequest_VALUE,
				}),
			},
		},
		{
			name: "txn-value-none-max-int-limit",
			ops: []*etcdserverpb.RequestOp{
				rangeRequestOp(&etcdserverpb.RangeRequest{
					Key: []byte(prefix), RangeEnd: rangeEnd, Limit: math.MaxInt64,
					SortTarget: etcdserverpb.RangeRequest_VALUE,
				}),
			},
		},
	}
	for _, test := range txnCases {
		txn, txnErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.ops})
		require.NoError(t, txnErr, test.name)
		require.Len(t, txn.Responses, len(test.ops))
		resp := txn.Responses[len(txn.Responses)-1].GetResponseRange()
		require.NotNil(t, resp)
		outcomes = append(outcomes, rangeOptionResult(test.name, resp, prefix, txn.Header.Revision))
	}
	return outcomes
}

func rangeRequestOp(req *etcdserverpb.RangeRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: req}}
}

func rangeOptionResult(name string, resp *etcdserverpb.RangeResponse, prefix string, txnRevision int64) rangeOptionOutcome {
	out := rangeOptionOutcome{Name: name, Count: resp.Count, More: resp.More}
	for _, kv := range resp.Kvs {
		out.Keys = append(out.Keys, strings.TrimPrefix(string(kv.Key), prefix))
		out.Values = append(out.Values, string(kv.Value))
		out.Versions = append(out.Versions, kv.Version)
		out.AtTxnRevision = append(out.AtTxnRevision, txnRevision > 0 && kv.ModRevision == txnRevision)
	}
	return out
}
