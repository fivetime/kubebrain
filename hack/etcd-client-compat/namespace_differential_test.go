package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

type namespaceOutcome struct {
	InitialKeys          []string
	TxnSucceeded         bool
	NestedPrevKey        string
	DeletePrevKey        string
	TxnEvents            []string
	TxnEventsOneRevision bool
	PostTxnKeys          []string
	Deleted              int64
	DeletePrevKeys       []string
	NamespaceEmpty       bool
	OutsideValue         string
}

func TestNamespaceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run namespace differential tests")
	}

	want := namespaceOutcome{
		InitialKeys:          []string{"a", "b", "c"},
		TxnSucceeded:         true,
		NestedPrevKey:        "a",
		DeletePrevKey:        "b",
		TxnEvents:            []string{"PUT:a:updated-a", "PUT:d:created-d", "DELETE:b:"},
		TxnEventsOneRevision: true,
		PostTxnKeys:          []string{"a", "c", "d"},
		Deleted:              3,
		DeletePrevKeys:       []string{"a", "c", "d"},
		NamespaceEmpty:       true,
		OutsideValue:         "outside",
	}
	referenceOutcome := runNamespaceScenario(t, reference, "etcd")
	kubebrainOutcome := runNamespaceScenario(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, kubebrainOutcome)
}

func runNamespaceScenario(t *testing.T, endpoint, instance string) namespaceOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	base := fmt.Sprintf("/dbaas-namespace/%s/%d/", instance, time.Now().UnixNano())
	tenantPrefix := base + "tenant/"
	outsideKey := base + "tenant0/outside"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, base, clientv3.WithPrefix())
	})

	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	namespacedWatcher := namespace.NewWatcher(client.Watcher, tenantPrefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = namespacedKV.Put(ctx, key, "seed-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, outsideKey, "outside")
	require.NoError(t, err)

	initial, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, namespaceKeys(initial.Kvs))

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := namespacedWatcher.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(initial.Header.Revision+1))
	txn, err := namespacedKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("a"), "=", "seed-a")).
		Then(
			clientv3.OpTxn(nil, []clientv3.Op{
				clientv3.OpPut("a", "updated-a", clientv3.WithPrevKV()),
				clientv3.OpPut("d", "created-d"),
			}, nil),
			clientv3.OpDelete("b", clientv3.WithPrevKV()),
		).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)
	nested := txn.Responses[0].GetResponseTxn()
	deleted := txn.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, nested)
	require.Len(t, nested.Responses, 2)
	require.NotNil(t, deleted)
	require.Len(t, deleted.PrevKvs, 1)
	nestedPut := nested.Responses[0].GetResponsePut()
	require.NotNil(t, nestedPut)
	require.NotNil(t, nestedPut.PrevKv)

	events := make([]string, 0, 3)
	eventsOneRevision := true
	for len(events) < 3 {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "namespace watch closed after %d/3 events", len(events))
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				events = append(events, fmt.Sprintf("%s:%s:%s",
					event.Type.String(), event.Kv.Key, event.Kv.Value))
				eventsOneRevision = eventsOneRevision && event.Kv.ModRevision == txn.Header.Revision
				if event.PrevKv != nil {
					require.False(t, bytes.HasPrefix(event.PrevKv.Key, []byte(tenantPrefix)))
				}
			}
		case <-watchCtx.Done():
			t.Fatalf("timed out after %d/3 namespace events", len(events))
		}
	}
	postTxn, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "c", "d"}, namespaceKeys(postTxn.Kvs))
	deleteAll, err := namespacedKV.Delete(ctx, "", clientv3.WithFromKey(), clientv3.WithPrevKV())
	require.NoError(t, err)
	deletePrevKeys := namespaceKeys(deleteAll.PrevKvs)

	empty, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	outside, err := client.Get(ctx, outsideKey)
	require.NoError(t, err)
	require.Len(t, outside.Kvs, 1)

	return namespaceOutcome{
		InitialKeys:          namespaceKeys(initial.Kvs),
		TxnSucceeded:         txn.Succeeded,
		NestedPrevKey:        string(nestedPut.PrevKv.Key),
		DeletePrevKey:        string(deleted.PrevKvs[0].Key),
		TxnEvents:            events,
		TxnEventsOneRevision: eventsOneRevision,
		PostTxnKeys:          namespaceKeys(postTxn.Kvs),
		Deleted:              deleteAll.Deleted,
		DeletePrevKeys:       deletePrevKeys,
		NamespaceEmpty:       len(empty.Kvs) == 0,
		OutsideValue:         string(outside.Kvs[0].Value),
	}
}

func namespaceKeys(kvs []*mvccpb.KeyValue) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys
}
