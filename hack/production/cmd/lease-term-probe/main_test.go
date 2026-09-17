package main

import (
	"bytes"
	"context"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fixture struct {
	pb.UnimplementedLeaseServer
	pb.UnimplementedMaintenanceServer
	header    *pb.ResponseHeader
	ttl       *pb.LeaseTimeToLiveResponse
	response  *pb.LeaseKeepAliveResponse
	streamErr error
	block     bool
	streams   atomic.Int32
}

func (s *fixture) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{Header: s.header, Leader: 22}, nil
}
func (s *fixture) LeaseTimeToLive(context.Context, *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	return s.ttl, nil
}
func (s *fixture) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	s.streams.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	if s.block {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if s.streamErr != nil {
		return s.streamErr
	}
	return stream.Send(s.response)
}

func TestProbeOriginalStream(t *testing.T) {
	for _, name := range []string{"success", "unavailable", "deadline", "wrong-cluster", "live-lease", "missing-lease", "wrong-key", "wrong-response", "output-failure"} {
		t.Run(name, func(t *testing.T) {
			header := &pb.ResponseHeader{ClusterId: 11, MemberId: 22, Revision: 3, RaftTerm: 4}
			s := &fixture{header: header,
				ttl:      &pb.LeaseTimeToLiveResponse{Header: header, ID: 33, TTL: -2, GrantedTTL: 3, Keys: [][]byte{[]byte("owned")}},
				response: &pb.LeaseKeepAliveResponse{Header: header, ID: 33, TTL: 3}}
			wantStreams := int32(1)
			switch name {
			case "unavailable":
				s.streamErr = status.Error(codes.Unavailable, "injected stream termination")
			case "deadline":
				s.block = true
			case "wrong-cluster":
				s.header.ClusterId = 99
				wantStreams = 0
			case "live-lease":
				s.ttl.TTL = 1
				wantStreams = 0
			case "missing-lease":
				s.ttl.GrantedTTL = 0
				wantStreams = 0
			case "wrong-key":
				s.ttl.Keys = nil
				wantStreams = 0
			case "wrong-response":
				s.response.ID = 44
			case "output-failure":
				wantStreams = 0
			}
			gs := grpc.NewServer()
			pb.RegisterLeaseServer(gs, s)
			pb.RegisterMaintenanceServer(gs, s)
			listener := bufconn.Listen(1 << 20)
			go func() { _ = gs.Serve(listener) }()
			defer listener.Close()
			defer gs.Stop()
			conn, err := grpc.NewClient("passthrough:///probe", grpc.WithDisableRetry(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var output bytes.Buffer
			var writer io.Writer = &output
			if name == "output-failure" {
				writer = failingWriter{}
			}
			err = probe(ctx, conn, 33, 11, 22, "owned", writer)
			if name == "success" {
				require.NoError(t, err)
				require.Contains(t, output.String(), `"phase":"response"`)
			} else {
				require.Error(t, err)
				require.NotContains(t, output.String(), `"phase":"response"`)
			}
			require.Equal(t, wantStreams, s.streams.Load())
			if name == "unavailable" {
				require.Equal(t, codes.Unavailable, status.Code(err))
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestRunRejectsMissingIdentityBeforeDial(t *testing.T) {
	require.Error(t, run(context.Background(), nil, io.Discard))
	require.Error(t, run(context.Background(), []string{"--endpoint=host:3379", "--duration=0"}, io.Discard))
}
