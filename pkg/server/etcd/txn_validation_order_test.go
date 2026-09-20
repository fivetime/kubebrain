package etcd

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

func TestSerializableTxnValidationBeforeBackend(t *testing.T) {
	rangeOp := func(key string, order etcdserverpb.RangeRequest_SortOrder) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key), Serializable: true, SortOrder: order},
		}}
	}
	for _, tc := range []struct {
		name string
		req  *etcdserverpb.TxnRequest
		want error
	}{
		{"empty-compare", &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{}}}, rpctypes.ErrGRPCEmptyKey},
		{"success-empty-key", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp("", 0)}}, rpctypes.ErrGRPCEmptyKey},
		{"failure-empty-key", &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{rangeOp("", 0)}}, rpctypes.ErrGRPCEmptyKey},
		{"success-sort", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp("k", 99)}}, rpctypes.ErrGRPCInvalidSortOption},
		{"failure-sort", &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{rangeOp("k", 99)}}, rpctypes.ErrGRPCInvalidSortOption},
		{"compare-before-sort", &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{}}, Success: []*etcdserverpb.RequestOp{rangeOp("k", 99)}}, rpctypes.ErrGRPCEmptyKey},
		{"limit-before-key", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp("", 0), rangeOp("k", 0), rangeOp("k", 0)}}, rpctypes.ErrGRPCTooManyOps},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Missing peers/backend make any consultation before rejection fail.
			s := &RPCServer{metricCli: mock.NewMinimalMetrics(gomock.NewController(t)), maxTxnOps: 2}
			for _, call := range []func(context.Context, *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error){
				s.Txn, s.txnOnce, s.proxyLatestSerializableTxn,
			} {
				var resp *etcdserverpb.TxnResponse
				var err error
				require.NotPanics(t, func() { resp, err = call(context.Background(), tc.req) })
				require.Nil(t, resp)
				require.Equal(t, status.Code(tc.want), status.Code(err))
				require.Equal(t, status.Convert(tc.want).Message(), status.Convert(err).Message())
			}
		})
	}
}

func TestTxnValidatesWholeTreeBeforeNestedDuplicateKeys(t *testing.T) {
	put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: []byte("duplicate")},
	}}
	nested := func(ops ...*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
			RequestTxn: &etcdserverpb.TxnRequest{Success: ops},
		}}
	}
	duplicate := nested(put, put)
	for _, tc := range []struct {
		name string
		op   *etcdserverpb.RequestOp
		want error
	}{
		{"empty-key", &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{}}}, rpctypes.ErrGRPCEmptyKey},
		{"sort", &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte("k"), SortOrder: 99}}}, rpctypes.ErrGRPCInvalidSortOption},
		{"empty-op", &etcdserverpb.RequestOp{}, rpctypes.ErrGRPCKeyNotFound},
		{"nested-budget", nested(make([]*etcdserverpb.RequestOp, defaultMaxTxnOps)...), rpctypes.ErrGRPCTooManyOps},
		{"valid-request-still-rejects-duplicate", put, rpctypes.ErrGRPCDuplicateKey},
	} {
		for _, shape := range []string{"parent-failure", "later-sibling", "deep-parent-failure"} {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				req := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{duplicate}, Failure: []*etcdserverpb.RequestOp{tc.op}}
				if shape == "later-sibling" {
					req.Success = append(req.Success, tc.op)
					req.Failure = nil
				} else if shape == "deep-parent-failure" {
					req.Success = []*etcdserverpb.RequestOp{nested(duplicate)}
				}
				check := func(err error) {
					t.Helper()
					require.Equal(t, status.Code(tc.want), status.Code(err))
					require.Equal(t, status.Convert(tc.want).Message(), status.Convert(err).Message())
				}
				check(validateTxnRequest(req))
				s, closeFn := newTestRPCServer(t)
				defer closeFn()
				resp, err := s.Txn(context.Background(), req)
				check(err)
				require.Nil(t, resp)
			})
		}
	}
}
