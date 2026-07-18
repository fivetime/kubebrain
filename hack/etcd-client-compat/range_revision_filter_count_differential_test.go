package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type revisionFilterCountOutcome struct {
	RangeCount     int64
	RangeKeys      []string
	CountOnlyCount int64
	CountOnlyKVs   int
	TxnCount       int64
	TxnKeys        []string
	TxnMore        bool
}

func TestRangeRevisionFilterCountDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runRevisionFilterCountScenario(t, reference, "etcd"),
		runRevisionFilterCountScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runRevisionFilterCountScenario(t *testing.T, endpoint, instance string) revisionFilterCountOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-range-revision-filter/%s/%d/", instance, time.Now().UnixNano())
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
	})

	_, err = client.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("old-a")})
	require.NoError(t, err)
	_, err = client.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("old-b")})
	require.NoError(t, err)
	updateB, err := client.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("new-b")})
	require.NoError(t, err)

	filtered, err := client.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision,
	})
	require.NoError(t, err)
	counted, err := client.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision, CountOnly: true,
	})
	require.NoError(t, err)

	txn, err := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		putRequestOp([]byte(prefix+"c"), "new-c"),
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd, MinModRevision: updateB.Header.Revision + 1,
		}}},
	}})
	require.NoError(t, err)
	require.Len(t, txn.Responses, 2)
	txnRange := txn.Responses[1].GetResponseRange()
	require.NotNil(t, txnRange)

	return revisionFilterCountOutcome{
		RangeCount:     filtered.Count,
		RangeKeys:      relativeKeys(filtered.Kvs, prefix),
		CountOnlyCount: counted.Count,
		CountOnlyKVs:   len(counted.Kvs),
		TxnCount:       txnRange.Count,
		TxnKeys:        relativeKeys(txnRange.Kvs, prefix),
		TxnMore:        txnRange.More,
	}
}

func relativeKeys(kvs []*mvccpb.KeyValue, prefix string) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, strings.TrimPrefix(string(kv.Key), prefix))
	}
	return keys
}
