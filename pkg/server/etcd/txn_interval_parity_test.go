package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/pkg/v3/adt"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// etcd/server/etcdserver/api/v3rpc/key.go checkIntervals uses the upstream
// affine interval tree, not the MVCC interpretation of a from-key sentinel.
// Compare binary boundaries against that implementation rather than assuming
// that ordinary half-open range membership is equivalent for malformed ranges.
func TestTxnDeleteIntervalParityWithEtcd(t *testing.T) {
	// Earlier RPC tests may have initialized protobuf's private message cache.
	// Exercise that order here rather than relying on an isolated cold process.
	_, err := proto.Marshal(status.Convert(rpctypes.ErrGRPCDuplicateKey).Proto())
	require.NoError(t, err)
	keys := []string{"\x00", "\x00\x00", "a", "a\x00", "ab", "b", "\xff"}
	for _, start := range keys {
		for _, end := range append([]string{""}, keys...) {
			tree := adt.NewIntervalTree()
			interval := adt.NewStringAffinePoint(start)
			if end != "" {
				interval = adt.NewStringAffineInterval(start, end)
			}
			tree.Insert(interval, struct{}{})
			local := newTxnDeleteInterval(&etcdserverpb.DeleteRangeRequest{Key: []byte(start), RangeEnd: []byte(end)})
			for _, key := range keys {
				want := tree.Intersects(adt.NewStringAffinePoint(key))
				require.Equal(t, want, local.contains([]byte(key)), "delete [%x,%x), put %x", start, end, key)
				put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(key)}}}
				del := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(start), RangeEnd: []byte(end)}}}
				for _, ops := range [][]*etcdserverpb.RequestOp{{put, del}, {del, put}} {
					for _, txn := range []*etcdserverpb.TxnRequest{{Success: ops}, {Failure: ops}} {
						err := validateTxnRequest(txn)
						if want {
							wantStatus, gotStatus := status.Convert(rpctypes.ErrGRPCDuplicateKey), status.Convert(err)
							require.Equal(t, wantStatus.Code(), gotStatus.Code())
							require.Equal(t, wantStatus.Message(), gotStatus.Message())
							require.Empty(t, gotStatus.Details())
						} else {
							require.NoError(t, err, "delete [%x,%x), put %x", start, end, key)
						}
					}
				}
			}
		}
	}
}
