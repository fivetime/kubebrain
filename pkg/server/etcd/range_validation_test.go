package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

// A server with no peers or backend proves rejection precedes consultation of
// leadership and the serializable checkpoint, including during an outage.
func TestRangeValidationBeforeBackend(t *testing.T) {
	for _, serializable := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			req        *etcdserverpb.RangeRequest
			code       codes.Code
			message    string
			streamOnly bool
		}{
			{"nil", nil, codes.InvalidArgument, "etcdserver: key is not provided", false},
			{"empty", &etcdserverpb.RangeRequest{}, codes.InvalidArgument, "etcdserver: key is not provided", false},
			{"sort-order", &etcdserverpb.RangeRequest{Key: []byte("k"), SortOrder: 99}, codes.InvalidArgument, "etcdserver: invalid sort option", false},
			{"sort-target", &etcdserverpb.RangeRequest{Key: []byte("k"), SortTarget: 99}, codes.InvalidArgument, "etcdserver: invalid sort option", false},
			{"empty-before-sort", &etcdserverpb.RangeRequest{SortOrder: 99}, codes.InvalidArgument, "etcdserver: key is not provided", false},
			{"stream-sort", &etcdserverpb.RangeRequest{Key: []byte("k"), SortOrder: etcdserverpb.RangeRequest_DESCEND}, codes.Unimplemented, "RangeStream does not support custom sort orders", true},
			{"stream-filter", &etcdserverpb.RangeRequest{Key: []byte("k"), MinModRevision: 1}, codes.Unimplemented, "RangeStream does not support revision filters", true},
			{"sort-before-filter", &etcdserverpb.RangeRequest{Key: []byte("k"), SortOrder: etcdserverpb.RangeRequest_DESCEND, MinModRevision: 1}, codes.Unimplemented, "RangeStream does not support custom sort orders", true},
		} {
			t.Run(tc.name+map[bool]string{false: "/linearizable", true: "/serializable"}[serializable], func(t *testing.T) {
				if tc.req != nil {
					tc.req.Serializable = serializable
				}
				s := &RPCServer{metricCli: mock.NewMinimalMetrics(gomock.NewController(t))}
				check := func(err error) {
					t.Helper()
					require.Equal(t, tc.code, status.Code(err))
					require.Equal(t, tc.message, status.Convert(err).Message())
				}
				if !tc.streamOnly {
					resp, err := s.Range(context.Background(), tc.req)
					check(err)
					require.Nil(t, resp)
					resp, err = s.rangeWithAfterReadOnce(context.Background(), tc.req, nil)
					check(err)
					require.Nil(t, resp)
				}
				stream := &fakeRangeStreamServer{ctx: context.Background()}
				check(s.RangeStream(tc.req, stream))
				check(s.rangeStreamOnce(tc.req, stream, time.Now()))
				require.Empty(t, stream.sent)
			})
		}
	}
}
