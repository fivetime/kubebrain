package etcdproxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type cancelObservedSnapshotServer struct {
	etcdserverpb.UnimplementedMaintenanceServer
	sendFrame bool
	started   chan struct{}
	canceled  chan struct{}
}

func (s *cancelObservedSnapshotServer) Snapshot(_ *etcdserverpb.SnapshotRequest, stream etcdserverpb.Maintenance_SnapshotServer) error {
	if s.sendFrame {
		if err := stream.Send(&etcdserverpb.SnapshotResponse{Blob: []byte("partial"), RemainingBytes: 1}); err != nil {
			return err
		}
	}
	close(s.started)
	<-stream.Context().Done()
	close(s.canceled)
	return stream.Context().Err()
}

func TestSnapshotProxyCancellationReachesLeaderAndClosesResults(t *testing.T) {
	for _, sendFrame := range []bool{false, true} {
		name := "before_first_frame"
		if sendFrame {
			name = "after_partial_frame"
		}
		t.Run(name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			leader := &cancelObservedSnapshotServer{sendFrame: sendFrame, started: make(chan struct{}), canceled: make(chan struct{})}
			server := grpc.NewServer()
			registerServingHealth(server)
			etcdserverpb.RegisterMaintenanceServer(server, leader)
			go func() { _ = server.Serve(lis) }()
			t.Cleanup(func() { server.Stop(); _ = lis.Close() })
			endpoint := lis.Addr().String()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
			require.NoError(t, err)
			t.Cleanup(func() { _ = cli.Close() })
			require.NoError(t, checkClientConn(cli, nil, time.Second))
			proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: endpoint}, client: cli, curLeader: endpoint}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			results, err := proxy.Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
			require.NoError(t, err)
			select {
			case <-leader.started:
			case <-time.After(5 * time.Second):
				t.Fatal("leader did not start the Snapshot")
			}
			// Do not consume results until cancellation: exercise a slow downstream
			// as well as a leader blocked before its first frame.
			cancel()
			select {
			case <-leader.canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("proxy cancellation did not reach leader")
			}
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for {
				select {
				case _, ok := <-results:
					if !ok {
						return
					}
				case <-deadline.C:
					t.Fatal("proxy result channel remained open after cancellation")
				}
			}
		})
	}
}
