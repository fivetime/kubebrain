package tikv

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestProtocolLatencyReadReplies(t *testing.T) {
	locked := &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{Key: []byte("test")}}
	for _, tc := range []struct {
		name     string
		response interface{}
		want     protocolLatencyReadStats
	}{
		{"get-lock", &kvrpcpb.GetResponse{Error: locked}, protocolLatencyReadStats{GetLocked: 1}},
		{"batch-response-lock", &kvrpcpb.BatchGetResponse{Error: locked}, protocolLatencyReadStats{BatchGetLocked: 1}},
		{"many-locks-one-retry", &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: locked}, {Error: locked}}}, protocolLatencyReadStats{BatchGetLocked: 1}},
		{"successful-batch", &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Value: []byte("v")}}}, protocolLatencyReadStats{}},
		{"non-lock-error", &kvrpcpb.BatchGetResponse{Error: &kvrpcpb.KeyError{Abort: "test"}}, protocolLatencyReadStats{Errors: 1}},
		{"mixed-error", &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: locked}, {Error: &kvrpcpb.KeyError{Abort: "test"}}}}, protocolLatencyReadStats{Errors: 1}},
		{"region-error", &kvrpcpb.BatchGetResponse{RegionError: &errorpb.Error{}, Error: locked}, protocolLatencyReadStats{Errors: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &protocolResponseStub{response: &tikvrpc.Response{Resp: tc.response}}
			client := &protocolLatencyClient{Client: stub}
			req := tikvrpc.NewRequest(tikvrpc.CmdBatchGet, &kvrpcpb.BatchGetRequest{})
			if _, ok := tc.response.(*kvrpcpb.GetResponse); ok {
				req = tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
			}
			_, err := client.SendRequest(context.Background(), "unused", req, time.Second)
			require.NoError(t, err)
			require.Equal(t, protocolLatencyReadStats{}, client.readSnapshot(), "unmarked work must not count")
			ctx := context.WithValue(context.Background(), protocolLatencyMarker{}, true)
			_, err = client.SendRequest(ctx, "unused", req, time.Second)
			require.NoError(t, err)
			require.Equal(t, tc.want, client.readSnapshot())
		})
	}
}
