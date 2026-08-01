package compat

import (
	"bytes"
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

type txnFromKeyExecutionOutcome struct {
	Name                string
	SeedRevisionGaps    []int64
	TxnRevisionGap      int64
	Deleted             int64
	DeleteAtTxnRevision bool
	DeletePrev          []txnFromKeyKV
	RangeAtTxnRevision  bool
	TxnRange            []txnFromKeyKV
	FinalHeaderGap      int64
	Final               []txnFromKeyKV
}

type txnFromKeyKV struct {
	Key           string
	Value         string
	Version       int64
	CreatedInTxn  bool
	ModifiedInTxn bool
}

func TestTxnFromKeyExecutionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []txnFromKeyExecutionOutcome{
		{
			Name:                "put-then-delete",
			SeedRevisionGaps:    []int64{1, 1},
			TxnRevisionGap:      1,
			Deleted:             3,
			DeleteAtTxnRevision: true,
			DeletePrev: []txnFromKeyKV{
				{Key: "b", Value: "seed-b", Version: 1},
				{Key: "c", Value: "seed-c", Version: 1},
				{Key: "d", Value: "txn-d", Version: 1, CreatedInTxn: true, ModifiedInTxn: true},
			},
			RangeAtTxnRevision: true,
			TxnRange:           []txnFromKeyKV{{Key: "a", Value: "seed-a", Version: 1}},
			Final:              []txnFromKeyKV{{Key: "a", Value: "seed-a", Version: 1}},
		},
		{
			Name:                "delete-then-put",
			SeedRevisionGaps:    []int64{1, 1},
			TxnRevisionGap:      1,
			Deleted:             2,
			DeleteAtTxnRevision: true,
			DeletePrev: []txnFromKeyKV{
				{Key: "b", Value: "seed-b", Version: 1},
				{Key: "c", Value: "seed-c", Version: 1},
			},
			RangeAtTxnRevision: true,
			TxnRange: []txnFromKeyKV{
				{Key: "a", Value: "seed-a", Version: 1},
				{Key: "d", Value: "txn-d", Version: 1, CreatedInTxn: true, ModifiedInTxn: true},
			},
			Final: []txnFromKeyKV{
				{Key: "a", Value: "seed-a", Version: 1},
				{Key: "d", Value: "txn-d", Version: 1, CreatedInTxn: true, ModifiedInTxn: true},
			},
		},
	}
	referenceOutcome := runTxnFromKeyExecutionScenario(t, reference, "etcd")
	kubebrainOutcome := runTxnFromKeyExecutionScenario(t, compatEndpoint(), "kubebrain")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, kubebrainOutcome)
}

func runTxnFromKeyExecutionScenario(t *testing.T, endpoint, instance string) []txnFromKeyExecutionOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	tests := []struct {
		name     string
		putFirst bool
	}{
		{name: "put-then-delete", putFirst: true},
		{name: "delete-then-put"},
	}
	outcomes := make([]txnFromKeyExecutionOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		prefix := string(bytes.Repeat([]byte{0xff}, 64)) +
			fmt.Sprintf("/dbaas-txn-from-key/%s/%d/%s/", instance, time.Now().UnixNano(), test.name)
		rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
		var seedRevisions []int64
		for _, suffix := range []string{"a", "b", "c"} {
			seed, putErr := client.Put(ctx, &etcdserverpb.PutRequest{
				Key: []byte(prefix + suffix), Value: []byte("seed-" + suffix),
			})
			require.NoError(t, putErr)
			seedRevisions = append(seedRevisions, seed.Header.Revision)
		}

		put := putRequestOp([]byte(prefix+"d"), "txn-d")
		del := deleteRequestOp([]byte(prefix+"b"), []byte{0})
		del.GetRequestDeleteRange().PrevKv = true
		ops := []*etcdserverpb.RequestOp{del, put}
		deleteIndex := 0
		if test.putFirst {
			ops = []*etcdserverpb.RequestOp{put, del}
			deleteIndex = 1
		}
		ops = append(ops, &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: rangeEnd},
		}})
		txn, txnErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: ops})
		require.NoError(t, txnErr)
		require.True(t, txn.Succeeded)
		require.Len(t, txn.Responses, 3)
		deleted := txn.Responses[deleteIndex].GetResponseDeleteRange()
		txnRange := txn.Responses[2].GetResponseRange()
		require.NotNil(t, deleted)
		require.NotNil(t, txnRange)
		final, finalErr := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: rangeEnd})
		require.NoError(t, finalErr)

		outcomes = append(outcomes, txnFromKeyExecutionOutcome{
			Name:                test.name,
			SeedRevisionGaps:    []int64{seedRevisions[1] - seedRevisions[0], seedRevisions[2] - seedRevisions[1]},
			TxnRevisionGap:      txn.Header.Revision - seedRevisions[2],
			Deleted:             deleted.Deleted,
			DeleteAtTxnRevision: deleted.Header.Revision == txn.Header.Revision,
			DeletePrev:          normalizeTxnFromKeyKVs(deleted.PrevKvs, prefix, txn.Header.Revision),
			RangeAtTxnRevision:  txnRange.Header.Revision == txn.Header.Revision,
			TxnRange:            normalizeTxnFromKeyKVs(txnRange.Kvs, prefix, txn.Header.Revision),
			FinalHeaderGap:      final.Header.Revision - txn.Header.Revision,
			Final:               normalizeTxnFromKeyKVs(final.Kvs, prefix, txn.Header.Revision),
		})
		_, cleanupErr := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
		cancel()
		require.NoError(t, cleanupErr)
	}
	return outcomes
}

func normalizeTxnFromKeyKVs(kvs []*mvccpb.KeyValue, prefix string, txnRevision int64) []txnFromKeyKV {
	out := make([]txnFromKeyKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, txnFromKeyKV{
			Key:           strings.TrimPrefix(string(kv.Key), prefix),
			Value:         string(kv.Value),
			Version:       kv.Version,
			CreatedInTxn:  kv.CreateRevision == txnRevision,
			ModifiedInTxn: kv.ModRevision == txnRevision,
		})
	}
	return out
}
