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

type txnCompactedRangeOrderOutcome struct {
	Name        string
	Code        string
	Message     string
	WrittenKeys int64
}

func TestTxnCompactedRangeOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []txnCompactedRangeOrderOutcome{
		{Name: "range-before-put", Code: "Unknown", Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "put-before-range", Code: "Unknown", Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "nested-put-before-range", Code: "Unknown", Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "unselected-compacted-range", Code: "OK", WrittenKeys: 1},
		{Name: "nested-unselected-compacted-range", Code: "OK", WrittenKeys: 3},
	}
	referenceOutcome := runTxnCompactedRangeOrderScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnCompactedRangeOrderScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnCompactedRangeOrderScenario(t *testing.T, endpoint, instance string) []txnCompactedRangeOrderOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a3709/txn-compacted-order/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	first, err := cli.Put(ctx, prefix+"source", "v1")
	require.NoError(t, err)
	second, err := cli.Put(ctx, prefix+"source", "v2")
	require.NoError(t, err)
	_, err = cli.Compact(ctx, second.Header.Revision)
	require.NoError(t, err)
	compactedGet := clientv3.OpGet(prefix+"source", clientv3.WithRev(first.Header.Revision))

	cases := []struct {
		name string
		ops  []clientv3.Op
	}{
		{
			name: "range-before-put",
			ops: []clientv3.Op{
				compactedGet,
				clientv3.OpPut(prefix+"range-before-put", "must-not-commit"),
			},
		},
		{
			name: "put-before-range",
			ops: []clientv3.Op{
				clientv3.OpPut(prefix+"put-before-range", "must-not-commit"),
				compactedGet,
			},
		},
		{
			name: "nested-put-before-range",
			ops: []clientv3.Op{
				clientv3.OpPut(prefix+"outer-before", "must-not-commit"),
				clientv3.OpTxn(nil, []clientv3.Op{
					clientv3.OpPut(prefix+"inner-before", "must-not-commit"),
					compactedGet,
				}, nil),
				clientv3.OpPut(prefix+"outer-after", "must-not-commit"),
			},
		},
	}

	outcomes := make([]txnCompactedRangeOrderOutcome, 0, len(cases))
	for _, tc := range cases {
		_, txnErr := cli.Txn(ctx).Then(tc.ops...).Commit()
		require.Error(t, txnErr, tc.name)
		written, getErr := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		require.NoError(t, getErr)
		// Only the source key may exist; every case-specific key must remain absent.
		outcomes = append(outcomes, txnCompactedRangeOrderOutcome{
			Name: tc.name, Code: status.Code(txnErr).String(), Message: status.Convert(txnErr).Message(),
			WrittenKeys: written.Count - 1,
		})
	}

	selectedKey := prefix + "selected-put"
	forbiddenKey := prefix + "unselected-put"
	response, txnErr := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"missing"), "=", 0)).
		Then(clientv3.OpPut(selectedKey, "committed")).
		Else(compactedGet, clientv3.OpPut(forbiddenKey, "must-not-commit")).
		Commit()
	require.NoError(t, txnErr)
	require.True(t, response.Succeeded)
	selected, getErr := cli.Get(ctx, selectedKey)
	require.NoError(t, getErr)
	require.Equal(t, int64(1), selected.Count)
	forbidden, getErr := cli.Get(ctx, forbiddenKey)
	require.NoError(t, getErr)
	require.Zero(t, forbidden.Count)
	written, getErr := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, getErr)
	outcomes = append(outcomes, txnCompactedRangeOrderOutcome{
		Name: "unselected-compacted-range", Code: status.Code(txnErr).String(),
		Message: status.Convert(txnErr).Message(), WrittenKeys: written.Count - 1,
	})

	nestedSelectedKey := prefix + "nested-selected-put"
	outerAfterNestedKey := prefix + "outer-after-nested"
	nestedForbiddenKey := prefix + "nested-unselected-put"
	response, txnErr = cli.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"nested-missing"), "=", 0)},
			[]clientv3.Op{clientv3.OpPut(nestedSelectedKey, "committed")},
			[]clientv3.Op{compactedGet, clientv3.OpPut(nestedForbiddenKey, "must-not-commit")},
		),
		clientv3.OpPut(outerAfterNestedKey, "committed"),
	).Commit()
	require.NoError(t, txnErr)
	require.True(t, response.Succeeded)
	for _, key := range []string{nestedSelectedKey, outerAfterNestedKey} {
		selected, nestedGetErr := cli.Get(ctx, key)
		require.NoError(t, nestedGetErr)
		require.Equal(t, int64(1), selected.Count)
	}
	forbidden, getErr = cli.Get(ctx, nestedForbiddenKey)
	require.NoError(t, getErr)
	require.Zero(t, forbidden.Count)
	written, getErr = cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, getErr)
	outcomes = append(outcomes, txnCompactedRangeOrderOutcome{
		Name: "nested-unselected-compacted-range", Code: status.Code(txnErr).String(),
		Message: status.Convert(txnErr).Message(), WrittenKeys: written.Count - 1,
	})
	return outcomes
}
