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

type txnRangeCompareDeleteOutcome struct {
	Succeeded bool
	Deleted   int64
	PrevKeys  []string
	Remaining []string
}

func TestTxnRangeCompareDeleteDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcome := runTxnRangeCompareDeleteScenario(t, reference, "etcd")
	kubebrainOutcome := runTxnRangeCompareDeleteScenario(t, compatEndpoint(), "kubebrain")
	require.Equal(t, referenceOutcome, kubebrainOutcome)
}

func runTxnRangeCompareDeleteScenario(t *testing.T, endpoint, instance string) txnRangeCompareDeleteOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-txn-range-compare-delete/%s/%d/", instance, time.Now().UnixNano())
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
	})

	for _, suffix := range []string{"a", "b", "c"} {
		_, err := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("v-" + suffix),
		})
		require.NoError(t, err)
	}
	resp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: []byte(prefix), RangeEnd: rangeEnd,
			Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte(prefix + "b"), RangeEnd: rangeEnd, PrevKv: true,
				},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "failure"), Value: []byte("unexpected")},
			},
		}},
	})
	require.NoError(t, err)
	deleteResp := resp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)

	remaining, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: rangeEnd})
	require.NoError(t, err)
	return txnRangeCompareDeleteOutcome{
		Succeeded: resp.Succeeded,
		Deleted:   deleteResp.Deleted,
		PrevKeys:  trimTxnRangeCompareDeleteKeys(deleteResp.PrevKvs, prefix),
		Remaining: trimTxnRangeCompareDeleteKeys(remaining.Kvs, prefix),
	}
}

func trimTxnRangeCompareDeleteKeys(kvs []*mvccpb.KeyValue, prefix string) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, strings.TrimPrefix(string(kv.Key), prefix))
	}
	return keys
}
