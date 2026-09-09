package tikv

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc"
)

type safeTSCodecTestServer struct {
	tikvpb.TikvServer
	start, end []byte
}

func (s *safeTSCodecTestServer) GetStoreSafeTS(_ context.Context, request *kvrpcpb.StoreSafeTSRequest) (*kvrpcpb.StoreSafeTSResponse, error) {
	r := request.GetKeyRange()
	// The target Region has safe-ts 50; its neighbors have 900. Match TiKV's
	// half-open Region metadata overlap check, not raw transaction key order.
	ts := uint64(900)
	if bytes.Compare(r.GetStartKey(), s.end) < 0 &&
		(len(r.GetEndKey()) == 0 || bytes.Compare(r.GetEndKey(), s.start) > 0) {
		ts = 50
	}
	return &kvrpcpb.StoreSafeTSResponse{SafeTs: ts}, nil
}

func TestTiKVClientSafeTSUsesEncodedRegionRanges(t *testing.T) {
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) { cfg.TiKVClient.MaxBatchSize = 0 }))
	for _, apiV2 := range []bool{false, true} {
		codec := clienttikv.NewCodecV1(clienttikv.ModeTxn)
		if apiV2 {
			var err error
			codec, err = clienttikv.NewCodecV2(clienttikv.ModeTxn, 123)
			require.NoError(t, err)
		}
		t.Run(codec.GetAPIVersion().String(), func(t *testing.T) {
			start, end := []byte("abcdefghA"), []byte("abcdefghZ")
			regionStart, regionEnd := codec.EncodeRegionRange(start, end)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			grpcServer := grpc.NewServer()
			tikvpb.RegisterTikvServer(grpcServer, &safeTSCodecTestServer{start: regionStart, end: regionEnd})
			done := make(chan error, 1)
			go func() { done <- grpcServer.Serve(listener) }()
			t.Cleanup(func() {
				grpcServer.Stop()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Error("safe-ts test server did not stop")
				}
			})
			client := clienttikv.NewRPCClient(clienttikv.WithCodec(codec),
				clienttikv.WithGRPCDialOptions(grpc.WithDefaultCallOptions(grpc.ForceCodecV2(newTiKVProtoCodec()))))
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			request := tikvrpc.NewRequest(tikvrpc.CmdStoreSafeTS, &kvrpcpb.StoreSafeTSRequest{
				KeyRange: &kvrpcpb.KeyRange{StartKey: start, EndKey: end},
			})
			for attempt := 0; attempt < 2; attempt++ {
				response, err := client.SendRequest(context.Background(), listener.Addr().String(), request, 5*time.Second)
				require.NoError(t, err)
				require.Equal(t, uint64(50), response.Resp.(*kvrpcpb.StoreSafeTSResponse).GetSafeTs(),
					"the pinned dependency and application gRPC codec must select the target Region, not its faster neighbor")
				require.Equal(t, start, request.StoreSafeTS().KeyRange.StartKey)
				require.Equal(t, end, request.StoreSafeTS().KeyRange.EndKey)
			}
		})
	}
}
