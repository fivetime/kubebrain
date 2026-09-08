package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	logbackuppb "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type probeLogBackupServer struct {
	logbackuppb.UnimplementedLogBackupServer
	probe func(context.Context, *logbackuppb.GetLastFlushTSOfRegionRequest) (*logbackuppb.GetLastFlushTSOfRegionResponse, error)
}

func (s *probeLogBackupServer) GetLastFlushTSOfRegion(ctx context.Context, req *logbackuppb.GetLastFlushTSOfRegionRequest) (*logbackuppb.GetLastFlushTSOfRegionResponse, error) {
	return s.probe(ctx, req)
}

// Accept the TCP connection but withhold it from gRPC until the test releases
// it. This exercises the real HTTP/2 readiness barrier without a startup sleep.
type probeGatedListener struct {
	net.Listener
	accepted chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (l *probeGatedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.once.Do(func() { close(l.accepted) })
	<-l.release
	return conn, nil
}

func TestProbeLogBackupWaitsForTransportReady(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gated := &probeGatedListener{Listener: listener, accepted: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gated.release) }) }
	server := grpc.NewServer()
	requests := make(chan *logbackuppb.GetLastFlushTSOfRegionRequest, 1)
	logbackuppb.RegisterLogBackupServer(server, &probeLogBackupServer{probe: func(_ context.Context, req *logbackuppb.GetLastFlushTSOfRegionRequest) (*logbackuppb.GetLastFlushTSOfRegionResponse, error) {
		requests <- req
		return &logbackuppb.GetLastFlushTSOfRegionResponse{}, nil
	}})
	served := make(chan error, 1)
	go func() { served <- server.Serve(gated) }()
	t.Cleanup(func() {
		release()
		server.Stop()
		require.NoError(t, <-served)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- probeLogBackup(ctx, listener.Addr().String(), insecure.NewCredentials()) }()
	select {
	case <-gated.accepted:
	case <-ctx.Done():
		t.Fatal("probe did not establish TCP connection")
	}
	select {
	case err := <-result:
		t.Fatalf("probe returned before transport became ready: %v", err)
	default:
	}
	release()
	require.NoError(t, <-result)
	select {
	case req := <-requests:
		require.Equal(t, &logbackuppb.GetLastFlushTSOfRegionRequest{}, req)
	default:
		t.Fatal("probe succeeded without sending the empty checkpoint request")
	}
}

func TestProbeLogBackupBoundsTransportWait(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, listener.Close()) })
			// Never serve HTTP/2: connecting TCP alone must not admit the probe.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- probeLogBackup(ctx, listener.Addr().String(), insecure.NewCredentials()) }()
			require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(5*time.Second)))
			conn, err := listener.Accept()
			require.NoError(t, err)
			defer conn.Close()
			want := context.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				want = context.Canceled
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, want)
				require.ErrorContains(t, err, "connect log-backup service")
			case <-time.After(5 * time.Second):
				t.Fatal("transport wait ignored caller context")
			}
		})
	}
}

func TestProbeLogBackupValidatesRPCResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *logbackuppb.GetLastFlushTSOfRegionResponse
		err      error
		want     string
	}{
		{name: "empty response", response: &logbackuppb.GetLastFlushTSOfRegionResponse{}},
		{name: "unexpected checkpoint", response: &logbackuppb.GetLastFlushTSOfRegionResponse{Checkpoints: []*logbackuppb.RegionCheckpoint{{}}}, want: "1 checkpoints for an empty request"},
		{name: "RPC failure", err: status.Error(codes.Unavailable, "backup unavailable"), want: "probe log-backup service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			logbackuppb.RegisterLogBackupServer(server, &probeLogBackupServer{probe: func(context.Context, *logbackuppb.GetLastFlushTSOfRegionRequest) (*logbackuppb.GetLastFlushTSOfRegionResponse, error) {
				return tc.response, tc.err
			}})
			served := make(chan error, 1)
			go func() { served <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-served)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = probeLogBackup(ctx, listener.Addr().String(), insecure.NewCredentials())
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}
