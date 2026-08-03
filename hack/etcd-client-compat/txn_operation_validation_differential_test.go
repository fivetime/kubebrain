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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type txnOperationValidationOutcome struct {
	Name        string
	Code        string
	Message     string
	HasResponse bool
	Succeeded   bool
	RevisionGap int64
	FinalKVs    []string
}

func TestTxnOperationValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	prefix := fmt.Sprintf("/dbaas-txn-operation-validation/%d/", time.Now().UnixNano())
	require.Equal(t,
		runTxnOperationValidationScenario(t, reference, prefix),
		runTxnOperationValidationScenario(t, compatEndpoint(t), prefix),
	)
	require.Equal(t,
		runTxnOperationBudgetScenario(t, reference, prefix+"budget/"),
		runTxnOperationBudgetScenario(t, compatEndpoint(t), prefix+"budget/"),
	)
}

func runTxnOperationValidationScenario(t *testing.T, endpoint, prefix string) []txnOperationValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	key := []byte(prefix + "compare")
	mutationKey := []byte(prefix + "mutation")
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	seed, err := client.Put(seedCtx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "seed"), Value: []byte("seed"),
	})
	seedCancel()
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	tests := []struct {
		name string
		op   *etcdserverpb.RequestOp
		txn  *etcdserverpb.TxnRequest
	}{
		{
			name: "put-empty-key-precedes-ignore-value",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("value"), IgnoreValue: true},
			}},
		},
		{
			name: "put-ignore-value-precedes-ignore-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{
					Key: key, Value: []byte("value"), Lease: 1, IgnoreValue: true, IgnoreLease: true,
				},
			}},
		},
		{
			name: "put-ignore-lease-with-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 1, IgnoreLease: true},
			}},
		},
		{
			name: "put-missing-lease-precedes-missing-ignore-value-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 987654321, IgnoreValue: true},
			}},
		},
		{
			name: "range-empty-key-precedes-invalid-sort",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{SortOrder: 99, SortTarget: 99},
			}},
		},
		{
			name: "range-invalid-order-precedes-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortOrder: 99, SortTarget: 99},
			}},
		},
		{
			name: "range-invalid-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortTarget: 99},
			}},
		},
		{
			name: "delete-empty-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}},
			}},
		},
		{
			name: "empty-operation",
			op:   &etcdserverpb.RequestOp{},
		},
		{
			name: "nested-compare-empty-key-precedes-success-empty-operation",
			txn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Result:      etcdserverpb.Compare_EQUAL,
						Target:      etcdserverpb.Compare_VERSION,
						TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
					}},
					Success: []*etcdserverpb.RequestOp{{}},
				}},
			}}},
		},
		{
			name: "nested-success-empty-operation-precedes-failure-empty-delete",
			txn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{}},
					Failure: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestDeleteRange{
							RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}, PrevKv: true},
						},
					}},
				}},
			}}},
		},
		{
			name: "nested-unselected-failure-empty-operation-is-statically-validated",
			txn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         key,
						Result:      etcdserverpb.Compare_EQUAL,
						Target:      etcdserverpb.Compare_VERSION,
						TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
					}},
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: mutationKey, Value: []byte("unexpected-success")},
						},
					}},
					Failure: []*etcdserverpb.RequestOp{{}},
				}},
			}}},
		},
		{
			name: "nested-unselected-success-empty-delete-is-statically-validated",
			txn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         key,
						Result:      etcdserverpb.Compare_EQUAL,
						Target:      etcdserverpb.Compare_VERSION,
						TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
					}},
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestDeleteRange{
							RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}, PrevKv: true},
						},
					}},
					Failure: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: mutationKey, Value: []byte("unexpected-failure")},
						},
					}},
				}},
			}}},
		},
	}

	outcomes := make([]txnOperationValidationOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		request := test.txn
		if request == nil {
			request = &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{test.op}}
		}
		_, callErr := client.Txn(ctx, request)
		cancel()
		require.Error(t, callErr, test.name)
		rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		final, rangeErr := client.Range(rangeCtx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
		rangeCancel()
		require.NoError(t, rangeErr, test.name)
		require.NotNil(t, final.Header, test.name)
		finalKVs := make([]string, 0, len(final.Kvs))
		for _, kv := range final.Kvs {
			finalKVs = append(finalKVs, fmt.Sprintf("%s=%s", strings.TrimPrefix(string(kv.Key), prefix), kv.Value))
		}
		require.Zero(t, final.Header.Revision-seed.Header.Revision, test.name)
		require.Equal(t, []string{"seed=seed"}, finalKVs, test.name)
		outcomes = append(outcomes, txnOperationValidationOutcome{
			Name:        test.name,
			Code:        status.Code(callErr).String(),
			Message:     status.Convert(callErr).Message(),
			RevisionGap: final.Header.Revision - seed.Header.Revision,
			FinalKVs:    finalKVs,
		})
	}
	return outcomes
}

func runTxnOperationBudgetScenario(t *testing.T, endpoint, prefix string) []txnOperationValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	seed, err := client.Put(seedCtx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "seed"), Value: []byte("seed"),
	})
	seedCancel()
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	mutationKey := []byte(prefix + "mutation")
	putOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: mutationKey, Value: []byte("unexpected")},
	}}
	rangeOp := func(suffix int) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte(fmt.Sprintf("%srange/%03d", prefix, suffix)),
			},
		}}
	}
	rangeOps := func(n int) []*etcdserverpb.RequestOp {
		ops := make([]*etcdserverpb.RequestOp, n)
		for i := range ops {
			ops[i] = rangeOp(i)
		}
		return ops
	}
	nested := func(ops []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
			RequestTxn: &etcdserverpb.TxnRequest{Success: ops},
		}}
	}
	compares := make([]*etcdserverpb.Compare, 128)
	for i := range compares {
		compares[i] = &etcdserverpb.Compare{
			Key:         []byte(fmt.Sprintf("%scompare/%03d", prefix, i)),
			Result:      etcdserverpb.Compare_EQUAL,
			Target:      etcdserverpb.Compare_VERSION,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}
	}

	tests := []struct {
		name string
		txn  *etcdserverpb.TxnRequest
	}{
		{name: "top-level-at-limit", txn: &etcdserverpb.TxnRequest{Success: rangeOps(128)}},
		{name: "top-level-over-limit", txn: &etcdserverpb.TxnRequest{
			Success: append([]*etcdserverpb.RequestOp{putOp}, rangeOps(128)...),
		}},
		{name: "nested-exact-remaining-budget", txn: &etcdserverpb.TxnRequest{
			Success: append(rangeOps(126), nested(rangeOps(1))),
		}},
		{name: "nested-over-remaining-budget", txn: &etcdserverpb.TxnRequest{
			Success: append([]*etcdserverpb.RequestOp{putOp}, append(rangeOps(126), nested(rangeOps(1)))...),
		}},
		{name: "compare-max-does-not-charge-range-child", txn: &etcdserverpb.TxnRequest{
			Compare: compares,
			Success: []*etcdserverpb.RequestOp{rangeOp(0)},
		}},
		{name: "unselected-failure-nested-over-budget", txn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{putOp},
			Failure: append(rangeOps(127), nested(rangeOps(1))),
		}},
	}

	outcomes := make([]txnOperationValidationOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, callErr := client.Txn(ctx, test.txn)
		cancel()
		outcome := txnOperationValidationOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		if resp != nil {
			outcome.HasResponse = true
			outcome.Succeeded = resp.Succeeded
		}
		rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		final, rangeErr := client.Range(rangeCtx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
		rangeCancel()
		require.NoError(t, rangeErr, test.name)
		require.NotNil(t, final.Header, test.name)
		for _, kv := range final.Kvs {
			outcome.FinalKVs = append(outcome.FinalKVs,
				fmt.Sprintf("%s=%s", strings.TrimPrefix(string(kv.Key), prefix), kv.Value))
		}
		outcome.RevisionGap = final.Header.Revision - seed.Header.Revision
		require.Zero(t, outcome.RevisionGap, test.name)
		require.Equal(t, []string{"seed=seed"}, outcome.FinalKVs, test.name)
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}
