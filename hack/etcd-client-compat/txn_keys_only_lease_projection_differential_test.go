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
	"google.golang.org/grpc/status"
)

type txnKeysOnlyLeaseOutcome struct {
	Name          string
	Code, Message string
	Succeeded     bool
	Count         int64
	More          bool
	Keys, Values  []string
	Leased        []bool
}

func TestTxnKeysOnlyLeaseProjectionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run Txn KeysOnly lease projection differential tests")
	}

	want := []txnKeysOnlyLeaseOutcome{
		{Name: "top-key", Code: "OK", Succeeded: true, Count: 2, Keys: []string{"a", "b"}, Values: []string{"", ""}, Leased: []bool{false, false}},
		{Name: "top-value", Code: "OK", Succeeded: true, Count: 2, Keys: []string{"b", "a"}, Values: []string{"", ""}, Leased: []bool{true, true}},
		{Name: "nested-key", Code: "OK", Succeeded: true, Count: 2, Keys: []string{"a", "b"}, Values: []string{"", ""}, Leased: []bool{false, false}},
		{Name: "nested-value", Code: "OK", Succeeded: true, Count: 2, Keys: []string{"b", "a"}, Values: []string{"", ""}, Leased: []bool{true, true}},
	}
	referenceOutcome := runTxnKeysOnlyLeaseProjection(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnKeysOnlyLeaseProjection(t, compatEndpoint(t), "kubebrain"))
}

func runTxnKeysOnlyLeaseProjection(t *testing.T, endpoint, instance string) []txnKeysOnlyLeaseOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a3718/txn-keys-only-lease/%s/%d/", instance, time.Now().UnixNano())
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Revoke(cleanupCtx, lease.ID)
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	cases := []struct {
		name   string
		target etcdserverpb.RangeRequest_SortTarget
		nested bool
	}{
		{name: "top-key", target: etcdserverpb.RangeRequest_KEY},
		{name: "top-value", target: etcdserverpb.RangeRequest_VALUE},
		{name: "nested-key", target: etcdserverpb.RangeRequest_KEY, nested: true},
		{name: "nested-value", target: etcdserverpb.RangeRequest_VALUE, nested: true},
	}
	outcomes := make([]txnKeysOnlyLeaseOutcome, 0, len(cases))
	for _, test := range cases {
		casePrefix := prefix + test.name + "/"
		ops := []*etcdserverpb.RequestOp{
			txnKeysOnlyLeasePutOp(casePrefix+"a", "z", int64(lease.ID)),
			txnKeysOnlyLeasePutOp(casePrefix+"b", "a", int64(lease.ID)),
			txnKeysOnlyLeaseRangeOp(casePrefix, test.target),
		}
		request := &etcdserverpb.TxnRequest{Success: ops}
		if test.nested {
			request = &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: request},
			}}}
		}
		response, callErr := kv.Txn(ctx, request)
		outcome := txnKeysOnlyLeaseOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		}
		if callErr == nil {
			outcome.Succeeded = response.Succeeded
			responses := response.Responses
			if test.nested {
				require.Len(t, responses, 1)
				nested := responses[0].GetResponseTxn()
				require.NotNil(t, nested)
				outcome.Succeeded = outcome.Succeeded && nested.Succeeded
				responses = nested.Responses
			}
			require.Len(t, responses, 3)
			rangeResponse := responses[2].GetResponseRange()
			require.NotNil(t, rangeResponse)
			outcome.Count, outcome.More = rangeResponse.Count, rangeResponse.More
			for _, item := range rangeResponse.Kvs {
				outcome.Keys = append(outcome.Keys, string(item.Key[len(casePrefix):]))
				outcome.Values = append(outcome.Values, string(item.Value))
				outcome.Leased = append(outcome.Leased, item.Lease != 0)
			}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func txnKeysOnlyLeasePutOp(key, value string, lease int64) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
		Key: []byte(key), Value: []byte(value), Lease: lease,
	}}}
}

func txnKeysOnlyLeaseRangeOp(prefix string, target etcdserverpb.RangeRequest_SortTarget) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), KeysOnly: true,
		SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: target,
	}}}
}
