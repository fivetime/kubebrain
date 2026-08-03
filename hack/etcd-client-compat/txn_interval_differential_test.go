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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type txnIntervalOutcome struct {
	Name        string
	Code        string
	Message     string
	HasResponse bool
	Succeeded   bool
	RevisionGap int64
	FinalKVs    []string
}

func TestTxnIntervalDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcome := runTxnIntervalScenario(t, reference, "etcd")
	want := []txnIntervalOutcome{
		{Name: "from-key-delete-before-put-in-range", Code: "OK", HasResponse: true, Succeeded: true, RevisionGap: 1, FinalKVs: []string{"z=value"}},
		{Name: "put-before-from-key-delete-in-range", Code: "OK", HasResponse: true, Succeeded: true, RevisionGap: 1},
		{Name: "from-key-delete-with-put-before-range", Code: "OK", HasResponse: true, Succeeded: true, RevisionGap: 1, FinalKVs: []string{"a=value"}},
		{Name: "empty-range-with-put-at-start", Code: "OK", HasResponse: true, Succeeded: true, RevisionGap: 1, FinalKVs: []string{"m=value", "seed=seed"}},
		{Name: "reversed-range-with-put-at-start", Code: "OK", HasResponse: true, Succeeded: true, RevisionGap: 1, FinalKVs: []string{"seed=seed", "z=value"}},
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnIntervalScenario(t, compatEndpoint(), "kubebrain"))
}

func runTxnIntervalScenario(t *testing.T, endpoint, instance string) []txnIntervalOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := etcdserverpb.NewKVClient(conn)
	tests := []struct {
		name    string
		wantGap int64
		wantKVs []string
		build   func(string) []*etcdserverpb.RequestOp
	}{
		{
			name: "from-key-delete-before-put-in-range", wantGap: 1, wantKVs: []string{"z=value"},
			build: func(prefix string) []*etcdserverpb.RequestOp {
				return []*etcdserverpb.RequestOp{
					deleteRequestOp([]byte(prefix+"m"), []byte{0}),
					putRequestOp([]byte(prefix+"z"), "value"),
				}
			},
		},
		{
			name: "put-before-from-key-delete-in-range", wantGap: 1,
			build: func(prefix string) []*etcdserverpb.RequestOp {
				return []*etcdserverpb.RequestOp{
					putRequestOp([]byte(prefix+"z"), "value"),
					deleteRequestOp([]byte(prefix+"m"), []byte{0}),
				}
			},
		},
		{
			name: "from-key-delete-with-put-before-range", wantGap: 1, wantKVs: []string{"a=value"},
			build: func(prefix string) []*etcdserverpb.RequestOp {
				return []*etcdserverpb.RequestOp{
					deleteRequestOp([]byte(prefix+"m"), []byte{0}),
					putRequestOp([]byte(prefix+"a"), "value"),
				}
			},
		},
		{
			name: "empty-range-with-put-at-start", wantGap: 1, wantKVs: []string{"m=value", "seed=seed"},
			build: func(prefix string) []*etcdserverpb.RequestOp {
				return []*etcdserverpb.RequestOp{
					deleteRequestOp([]byte(prefix+"m"), []byte(prefix+"m")),
					putRequestOp([]byte(prefix+"m"), "value"),
				}
			},
		},
		{
			name: "reversed-range-with-put-at-start", wantGap: 1, wantKVs: []string{"seed=seed", "z=value"},
			build: func(prefix string) []*etcdserverpb.RequestOp {
				return []*etcdserverpb.RequestOp{
					deleteRequestOp([]byte(prefix+"z"), []byte(prefix+"m")),
					putRequestOp([]byte(prefix+"z"), "value"),
				}
			},
		},
	}

	outcomes := make([]txnIntervalOutcome, 0, len(tests))
	for i, test := range tests {
		prefix := string(bytes.Repeat([]byte{0xff}, 64)) +
			fmt.Sprintf("/dbaas-txn-interval/%s/%d/%02d/", instance, time.Now().UnixNano(), i)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			})
		})
		seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
		seed, seedErr := client.Put(seedCtx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "seed"), Value: []byte("seed"),
		})
		seedCancel()
		require.NoError(t, seedErr, test.name)
		require.NotNil(t, seed.Header, test.name)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.build(prefix)})
		cancel()
		outcome := txnIntervalOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		require.NoError(t, callErr, test.name)
		require.NotNil(t, resp, test.name)
		require.True(t, resp.Succeeded, test.name)
		outcome.HasResponse = true
		outcome.Succeeded = resp.Succeeded
		rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		final, rangeErr := client.Range(rangeCtx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
		rangeCancel()
		require.NoError(t, rangeErr, test.name)
		require.NotNil(t, final.Header, test.name)
		outcome.RevisionGap = final.Header.Revision - seed.Header.Revision
		for _, kv := range final.Kvs {
			outcome.FinalKVs = append(outcome.FinalKVs,
				fmt.Sprintf("%s=%s", strings.TrimPrefix(string(kv.Key), prefix), kv.Value))
		}
		require.Equal(t, test.wantGap, outcome.RevisionGap, test.name)
		require.Equal(t, test.wantKVs, outcome.FinalKVs, test.name)
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func deleteRequestOp(key, rangeEnd []byte) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
	}}
}
