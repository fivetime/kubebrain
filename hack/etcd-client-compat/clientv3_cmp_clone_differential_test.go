package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type cmpCloneOutcome struct {
	Name      string
	Succeeded bool
	Value     string
}

// TestClientV3CmpCloneBoundaries pins upstream etcd 160684ae0. clientv3 Cmp
// values retained by Txn.If and OpTxn must be deep-cloned, so caller-side
// mutations after request construction cannot redirect the compare.
func TestClientV3CmpCloneBoundaries(t *testing.T) {
	base := clientv3.ModRevision("foo")
	cmp := clientv3.Compare(base, "=", 7)
	base.WithKeyBytes([]byte("bar"))
	require.Equal(t, "foo", string(cmp.KeyBytes()))
	require.Equal(t, int64(7), cmp.GetCompare().GetModRevision())

	ranged := base.WithRange("zoo")
	require.Empty(t, string(base.GetCompare().GetRangeEnd()))
	require.Equal(t, "zoo", string(ranged.GetCompare().GetRangeEnd()))
}

func TestClientV3CmpCloneDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcome := runClientV3CmpCloneScenario(t, reference, "etcd")
	require.Equal(t, []cmpCloneOutcome{
		{Name: "txn-if", Succeeded: true, Value: "txn-if-then"},
		{Name: "op-txn", Succeeded: true, Value: "op-txn-then"},
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, runClientV3CmpCloneScenario(t, compatEndpoint(t), "kubebrain"))
}

func runClientV3CmpCloneScenario(t *testing.T, endpoint, instance string) []cmpCloneOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/a3753-clientv3-cmp-clone/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.Delete(ctx, prefix, clientv3.WithPrefix())
	})

	originalKey := prefix + "original"
	mutatedKey := prefix + "mutated!"
	resultKey := prefix + "result"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.Put(ctx, originalKey, "expected")
	require.NoError(t, err)
	_, err = client.Put(ctx, mutatedKey, "unexpected")
	require.NoError(t, err)

	outcomes := make([]cmpCloneOutcome, 0, 2)

	cmp := clientv3.Compare(clientv3.Value("placeholder"), "=", "expected")
	cmp.WithKeyBytes([]byte(originalKey))
	txn := client.Txn(ctx).If(cmp).Then(clientv3.OpPut(resultKey, "txn-if-then")).Else(clientv3.OpPut(resultKey, "txn-if-else"))
	mutateCmpKeyInPlace(t, &cmp, mutatedKey)
	txnResp, err := txn.Commit()
	require.NoError(t, err)
	value := mustGetSingleValue(t, client, resultKey)
	outcomes = append(outcomes, cmpCloneOutcome{Name: "txn-if", Succeeded: txnResp.Succeeded, Value: value})
	require.True(t, txnResp.Succeeded)
	require.Equal(t, "txn-if-then", value)

	_, err = client.Delete(ctx, resultKey)
	require.NoError(t, err)
	cmp = clientv3.Compare(clientv3.Value("placeholder"), "=", "expected")
	cmp.WithKeyBytes([]byte(originalKey))
	cmps := []clientv3.Cmp{cmp}
	op := clientv3.OpTxn(
		cmps,
		[]clientv3.Op{clientv3.OpPut(resultKey, "op-txn-then")},
		[]clientv3.Op{clientv3.OpPut(resultKey, "op-txn-else")},
	)
	mutateCmpKeyInPlace(t, &cmps[0], mutatedKey)
	doResp, err := client.Do(ctx, op)
	require.NoError(t, err)
	require.NotNil(t, doResp.Txn())
	value = mustGetSingleValue(t, client, resultKey)
	outcomes = append(outcomes, cmpCloneOutcome{Name: "op-txn", Succeeded: doResp.Txn().Succeeded, Value: value})
	require.True(t, doResp.Txn().Succeeded)
	require.Equal(t, "op-txn-then", value)

	return outcomes
}

func mutateCmpKeyInPlace(t *testing.T, cmp *clientv3.Cmp, mutatedKey string) {
	t.Helper()
	keyBytes := cmp.KeyBytes()
	require.Len(t, keyBytes, len(mutatedKey))
	copy(keyBytes, mutatedKey)
}

func mustGetSingleValue(t *testing.T, client *clientv3.Client, key string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	return string(resp.Kvs[0].Value)
}
