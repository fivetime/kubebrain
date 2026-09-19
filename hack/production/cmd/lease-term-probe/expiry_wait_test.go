package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestProbeWaitsForExpiryBeforeSingleRenewal(t *testing.T) {
	for _, mode := range []string{"expires", "deadline", "changed-term", "changed-grant", "extra-key", "removed", "read-fails", "no-deadline"} {
		t.Run(mode, func(t *testing.T) {
			header := &pb.ResponseHeader{ClusterId: 11, MemberId: 22, RaftTerm: 4}
			first := &pb.LeaseTimeToLiveResponse{Header: header, ID: 33, TTL: 1, GrantedTTL: 10, Keys: [][]byte{[]byte("owned")}}
			last := &pb.LeaseTimeToLiveResponse{Header: header, ID: 33, TTL: -1, GrantedTTL: 10, Keys: [][]byte{[]byte("owned")}}
			switch mode {
			case "changed-term":
				last.Header = &pb.ResponseHeader{ClusterId: 11, MemberId: 22, RaftTerm: 5}
			case "changed-grant":
				last.GrantedTTL = 20
			case "extra-key":
				last.Keys = append(last.Keys, []byte("foreign"))
			case "removed":
				last.GrantedTTL, last.Keys = 0, nil
			case "deadline":
				last.TTL = 0
			}
			s := &fixture{header: header, ttlSequence: []*pb.LeaseTimeToLiveResponse{first, last}, response: &pb.LeaseKeepAliveResponse{Header: header, ID: 33, TTL: 10}}
			if mode == "read-fails" {
				s.secondTTLFailure = status.Error(codes.Unavailable, "read failed")
			}
			server := grpc.NewServer()
			pb.RegisterMaintenanceServer(server, s)
			pb.RegisterLeaseServer(server, s)
			listener := bufconn.Listen(1 << 20)
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(listener) }()
			defer func() { server.Stop(); _ = listener.Close(); <-done }()
			var dials atomic.Int32
			conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithDisableRetry(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				dials.Add(1)
				return listener.DialContext(ctx)
			}))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if mode == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 300*time.Millisecond)
				defer stop()
			}
			if mode == "no-deadline" {
				ctx = context.Background()
			}
			var output bytes.Buffer
			err = probeWithExpiryWait(ctx, conn, 33, 11, 22, "owned", &output, true)
			if mode == "expires" {
				require.NoError(t, err)
				require.Equal(t, int32(1), s.streams.Load())
				d := json.NewDecoder(&output)
				for _, phase := range []string{"expired_preflight", "request_sent", "response"} {
					var event map[string]any
					require.NoError(t, d.Decode(&event))
					require.Equal(t, phase, event["phase"])
				}
				var extra any
				require.ErrorIs(t, d.Decode(&extra), io.EOF)
			} else {
				require.Error(t, err)
				require.Zero(t, s.streams.Load())
				require.Empty(t, output.String())
				if mode == "deadline" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			}
			if mode == "no-deadline" {
				require.Zero(t, s.ttlReads.Load())
				require.Zero(t, dials.Load())
			} else {
				require.Equal(t, int32(2), s.ttlReads.Load())
				require.Equal(t, int32(1), dials.Load())
			}
		})
	}
}
