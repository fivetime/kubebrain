package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingRangeOutcome struct {
	RangeCompareSucceeded bool
	NestedValues          []string
	NestedSingleRevision  bool
	DeleteCount           int64
	DeleteEvents          int
	DeleteSingleRevision  bool
	RangeEmpty            bool
	OutsideValue          string
}

func TestLeasingRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing range differential tests")
	}

	require.Equal(t,
		runLeasingRangeScenario(t, reference, "etcd"),
		runLeasingRangeScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingRangeScenario(t *testing.T, endpoint, instance string) leasingRangeOutcome {
	t.Helper()
	first, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-range/%s/%d/", instance, time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	outsideKey := prefix + "outside"
	firstKV, closeFirst, err := leasing.NewKV(first, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeSecond)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = first.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	const initialKeys = 4
	for index := 0; index < initialKeys; index++ {
		key := fmt.Sprintf("%s%d", dataPrefix, index)
		_, err = first.Put(ctx, key, fmt.Sprintf("initial-%d", index))
		require.NoError(t, err)
	}
	_, err = first.Put(ctx, dataPrefix+"1", "version-two")
	require.NoError(t, err)
	_, err = first.Put(ctx, outsideKey, "outside")
	require.NoError(t, err)
	cached, err := firstKV.Get(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, cached.Kvs, initialKeys)

	rangeCompare, err := firstKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(dataPrefix).WithPrefix(), "=", 1)).
		Commit()
	require.NoError(t, err)

	nested, err := secondKV.Txn(ctx).Then(clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpPut(dataPrefix+"0", "nested-zero"),
			clientv3.OpPut(dataPrefix+"4", "nested-four"),
		},
		nil,
	)).Commit()
	require.NoError(t, err)
	require.Len(t, nested.Responses, 1)
	nestedResponse := nested.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedResponse)
	require.Len(t, nestedResponse.Responses, 2)
	afterNested, err := firstKV.Get(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, afterNested.Kvs, initialKeys+1)
	nestedValues := make([]string, 0, len(afterNested.Kvs))
	for _, kv := range afterNested.Kvs {
		nestedValues = append(nestedValues, string(kv.Value))
	}
	nestedSingleRevision := true
	for _, response := range nestedResponse.Responses {
		if response.GetResponsePut().Header.Revision != nested.Header.Revision {
			nestedSingleRevision = false
		}
	}

	deleted, err := secondKV.Delete(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	watchCtx, watchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer watchCancel()
	watch := second.Watch(
		watchCtx, dataPrefix, clientv3.WithPrefix(), clientv3.WithRev(deleted.Header.Revision),
	)
	deleteEvents := 0
	deleteSingleRevision := true
	for deleteEvents < initialKeys+1 {
		select {
		case response, ok := <-watch:
			require.True(t, ok)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				deleteEvents++
				if event.Kv.ModRevision != deleted.Header.Revision {
					deleteSingleRevision = false
				}
			}
		case <-watchCtx.Done():
			require.NoError(t, watchCtx.Err())
		}
	}
	afterDelete, err := firstKV.Get(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	outside, err := first.Get(ctx, outsideKey)
	require.NoError(t, err)
	require.Len(t, outside.Kvs, 1)

	return leasingRangeOutcome{
		RangeCompareSucceeded: rangeCompare.Succeeded,
		NestedValues:          nestedValues,
		NestedSingleRevision:  nestedSingleRevision,
		DeleteCount:           deleted.Deleted,
		DeleteEvents:          deleteEvents,
		DeleteSingleRevision:  deleteSingleRevision,
		RangeEmpty:            len(afterDelete.Kvs) == 0,
		OutsideValue:          string(outside.Kvs[0].Value),
	}
}
