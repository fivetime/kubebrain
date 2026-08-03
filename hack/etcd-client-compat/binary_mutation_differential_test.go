package compat

import (
	"context"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type binaryMutationOutcome struct {
	Name    string
	Deleted int64
	Keys    []string
	Values  []string
}

func TestBinaryMutationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run binary mutation differential tests")
	}

	require.Equal(t,
		runBinaryMutationScenario(t, reference),
		runBinaryMutationScenario(t, compatEndpoint(t)),
	)
}

func runBinaryMutationScenario(t *testing.T, endpoint string) []binaryMutationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	keys := [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01},
		{0xfe},
		{0xfe, 0x00},
		{0xfe, 0x01},
		{0xff},
	}
	requireBinaryRangesEmpty(t, ctx, client,
		&etcdserverpb.RangeRequest{Key: []byte{0x00}, RangeEnd: []byte{0x01}},
		&etcdserverpb.RangeRequest{Key: []byte{0x01}},
		&etcdserverpb.RangeRequest{Key: []byte{0xfe}, RangeEnd: []byte{0xff}},
		&etcdserverpb.RangeRequest{Key: []byte{0xff}},
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		deleteExactKeys(cleanupCtx, client, keys)
	})
	for i, key := range keys {
		_, err = client.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte{byte('a' + i)},
		})
		require.NoError(t, err)
	}

	txnRange, err := client.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{
					Key: []byte{0x00}, RangeEnd: []byte{0x01},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, txnRange.Responses, 1)
	outcomes := []binaryMutationOutcome{
		binaryMutationRangeOutcome("txn-range", txnRange.Responses[0].GetResponseRange()),
	}

	txnDelete, err := client.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte{0x00}, RangeEnd: []byte{0x01}, PrevKv: true,
				},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, txnDelete.Responses, 1)
	deleted := txnDelete.Responses[0].GetResponseDeleteRange()
	outcomes = append(outcomes, binaryMutationDeleteOutcome("txn-delete", deleted))

	afterTxn, err := client.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0x00}, RangeEnd: []byte{0x01},
	})
	require.NoError(t, err)
	outcomes = append(outcomes, binaryMutationRangeOutcome("after-txn-delete", afterTxn))

	standalone, err := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff}, PrevKv: true,
	})
	require.NoError(t, err)
	outcomes = append(outcomes, binaryMutationDeleteOutcome("standalone-delete", standalone))

	afterStandalone, err := client.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff},
	})
	require.NoError(t, err)
	outcomes = append(outcomes, binaryMutationRangeOutcome("after-standalone-delete", afterStandalone))
	return outcomes
}

func binaryMutationRangeOutcome(name string, response *etcdserverpb.RangeResponse) binaryMutationOutcome {
	out := binaryMutationOutcome{Name: name}
	for _, kv := range response.Kvs {
		out.Keys = append(out.Keys, hex.EncodeToString(kv.Key))
		out.Values = append(out.Values, string(kv.Value))
	}
	return out
}

func binaryMutationDeleteOutcome(name string, response *etcdserverpb.DeleteRangeResponse) binaryMutationOutcome {
	out := binaryMutationOutcome{Name: name, Deleted: response.Deleted}
	for _, kv := range response.PrevKvs {
		out.Keys = append(out.Keys, hex.EncodeToString(kv.Key))
		out.Values = append(out.Values, string(kv.Value))
	}
	return out
}
