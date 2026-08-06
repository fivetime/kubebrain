package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type txnFutureRangeOrderOutcome struct {
	Name        string
	Code        string
	Message     string
	WrittenKeys int64
}

func TestTxnFutureRangeOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []txnFutureRangeOrderOutcome{
		{Name: "range-before-put", Code: "Unknown", Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "put-before-range", Code: "Unknown", Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "nested-put-before-range", Code: "Unknown", Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "unselected-future-range", Code: "OK", WrittenKeys: 1},
		{Name: "nested-unselected-future-range", Code: "OK", WrittenKeys: 3},
	}
	referenceOutcome := runTxnFutureRangeOrderScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnFutureRangeOrderScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnFutureRangeOrderScenario(t *testing.T, endpoint, instance string) []txnFutureRangeOrderOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a3712/txn-future-order/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	seed, err := cli.Put(ctx, prefix+"source", "current")
	require.NoError(t, err)
	futureGet := clientv3.OpGet(prefix+"source", clientv3.WithRev(seed.Header.Revision+(1<<40)))
	cases := []struct {
		name string
		ops  []clientv3.Op
	}{
		{
			name: "range-before-put",
			ops:  []clientv3.Op{futureGet, clientv3.OpPut(prefix+"range-before-put", "must-not-commit")},
		},
		{
			name: "put-before-range",
			ops:  []clientv3.Op{clientv3.OpPut(prefix+"put-before-range", "must-not-commit"), futureGet},
		},
		{
			name: "nested-put-before-range",
			ops: []clientv3.Op{
				clientv3.OpPut(prefix+"outer-before", "must-not-commit"),
				clientv3.OpTxn(nil, []clientv3.Op{
					clientv3.OpPut(prefix+"inner-before", "must-not-commit"), futureGet,
				}, nil),
				clientv3.OpPut(prefix+"outer-after", "must-not-commit"),
			},
		},
	}

	outcomes := make([]txnFutureRangeOrderOutcome, 0, 5)
	for _, tc := range cases {
		_, txnErr := cli.Txn(ctx).Then(tc.ops...).Commit()
		require.Error(t, txnErr, tc.name)
		written, getErr := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		require.NoError(t, getErr)
		outcomes = append(outcomes, txnFutureRangeOrderOutcome{
			Name: tc.name, Code: status.Code(txnErr).String(), Message: status.Convert(txnErr).Message(),
			WrittenKeys: written.Count - 1,
		})
	}

	selectedKey := prefix + "selected-put"
	forbiddenKey := prefix + "unselected-put"
	response, txnErr := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"missing"), "=", 0)).
		Then(clientv3.OpPut(selectedKey, "committed")).
		Else(futureGet, clientv3.OpPut(forbiddenKey, "must-not-commit")).
		Commit()
	require.NoError(t, txnErr)
	require.True(t, response.Succeeded)
	requireKeyCounts(t, ctx, cli, map[string]int64{selectedKey: 1, forbiddenKey: 0})
	written, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	outcomes = append(outcomes, txnFutureRangeOrderOutcome{
		Name: "unselected-future-range", Code: status.Code(txnErr).String(),
		Message: status.Convert(txnErr).Message(), WrittenKeys: written.Count - 1,
	})

	nestedSelectedKey := prefix + "nested-selected-put"
	outerAfterNestedKey := prefix + "outer-after-nested"
	nestedForbiddenKey := prefix + "nested-unselected-put"
	response, txnErr = cli.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"nested-missing"), "=", 0)},
			[]clientv3.Op{clientv3.OpPut(nestedSelectedKey, "committed")},
			[]clientv3.Op{futureGet, clientv3.OpPut(nestedForbiddenKey, "must-not-commit")},
		),
		clientv3.OpPut(outerAfterNestedKey, "committed"),
	).Commit()
	require.NoError(t, txnErr)
	require.True(t, response.Succeeded)
	requireKeyCounts(t, ctx, cli, map[string]int64{
		nestedSelectedKey: 1, outerAfterNestedKey: 1, nestedForbiddenKey: 0,
	})
	written, err = cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	outcomes = append(outcomes, txnFutureRangeOrderOutcome{
		Name: "nested-unselected-future-range", Code: status.Code(txnErr).String(),
		Message: status.Convert(txnErr).Message(), WrittenKeys: written.Count - 1,
	})
	return outcomes
}

func requireKeyCounts(t *testing.T, ctx context.Context, cli *clientv3.Client, counts map[string]int64) {
	t.Helper()
	for key, want := range counts {
		response, err := cli.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, want, response.Count, key)
	}
}
