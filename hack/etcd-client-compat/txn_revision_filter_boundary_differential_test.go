package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type txnFilterBoundaryOutcome struct {
	Filtered       rangeFilterBoundaryOutcome
	CountOnly      rangeFilterBoundaryOutcome
	KeysLimited    rangeFilterBoundaryOutcome
	InvertedBounds rangeFilterBoundaryOutcome
}

func TestTxnRevisionFilterBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	want := txnFilterBoundaryOutcome{
		Filtered:       rangeFilterBoundaryOutcome{Count: 4},
		CountOnly:      rangeFilterBoundaryOutcome{Count: 4},
		KeysLimited:    rangeFilterBoundaryOutcome{Keys: []string{"a", "b"}, Count: 4, More: true},
		InvertedBounds: rangeFilterBoundaryOutcome{Count: 4},
	}
	referenceOutcome := runTxnRevisionFilterBoundaryScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnRevisionFilterBoundaryScenario(t, kubebrain, "kubebrain"))
}

func runTxnRevisionFilterBoundaryScenario(t *testing.T, endpoint, instance string) txnFilterBoundaryOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/compat/txn-filter-boundary/%s/%d/", instance, time.Now().UnixNano())
	end := clientv3.GetPrefixRangeEnd(prefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = client.Put(ctx, prefix+key, key)
		require.NoError(t, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	}()

	response, err := etcdserverpb.NewKVClient(client.ActiveConnection()).Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			putRequestOp([]byte(prefix+"d"), "d"),
			rangeRequestOp(&etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(end), MaxModRevision: -1,
			}),
			rangeRequestOp(&etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(end), MaxModRevision: -1, CountOnly: true, Limit: 1,
			}),
			rangeRequestOp(&etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(end), MinCreateRevision: -1, KeysOnly: true, Limit: 2,
			}),
			rangeRequestOp(&etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(end),
				MinModRevision: responseRevisionPlaceholder, MaxModRevision: 1,
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, response.Responses, 5)
	return txnFilterBoundaryOutcome{
		Filtered:       normalizeTxnFilterBoundaryResponse(t, response.Responses[1].GetResponseRange(), prefix),
		CountOnly:      normalizeTxnFilterBoundaryResponse(t, response.Responses[2].GetResponseRange(), prefix),
		KeysLimited:    normalizeTxnFilterBoundaryResponse(t, response.Responses[3].GetResponseRange(), prefix),
		InvertedBounds: normalizeTxnFilterBoundaryResponse(t, response.Responses[4].GetResponseRange(), prefix),
	}
}

const responseRevisionPlaceholder int64 = 1<<62 - 1

func normalizeTxnFilterBoundaryResponse(t *testing.T, response *etcdserverpb.RangeResponse, prefix string) rangeFilterBoundaryOutcome {
	t.Helper()
	require.NotNil(t, response)
	outcome := rangeFilterBoundaryOutcome{Count: response.Count, More: response.More}
	for _, kv := range response.Kvs {
		outcome.Keys = append(outcome.Keys, string(kv.Key[len(prefix):]))
	}
	return outcome
}
