package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Execute the actual CLI parser/probe in an owned subprocess, without rebuilding
// a binary inside the test. No test-runner stdout is allowed into probe evidence.
func TestSupervisedProbeHelper(t *testing.T) {
	if os.Getenv("KUBEBRAIN_SUPERVISED_PROBE_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			if err := run(context.Background(), os.Args[i+1:], os.Stdout); err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	os.Exit(2)
}

type heldLease struct {
	*fixture
	received chan struct{}
	release  chan struct{}
}

func (s *heldLease) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	s.streams.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	close(s.received)
	select {
	case <-s.release:
		return stream.Send(s.response)
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func TestSupervisedProbeOverMutualTLS(t *testing.T) {
	ca, cert, key, tlsConfig := probeTestTLS(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counted := &countingListener{Listener: listener}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	defer server.Stop()
	defer counted.Close()
	header := &pb.ResponseHeader{ClusterId: 11, MemberId: 22, Revision: 3, RaftTerm: 4}
	f := &fixture{header: header,
		ttl:      &pb.LeaseTimeToLiveResponse{Header: header, ID: 33, TTL: -1, GrantedTTL: 3, Keys: [][]byte{[]byte("owned")}},
		response: &pb.LeaseKeepAliveResponse{Header: &pb.ResponseHeader{ClusterId: 11, MemberId: 22, Revision: 3, RaftTerm: 5}, ID: 33, TTL: 3}}
	held := &heldLease{fixture: f, received: make(chan struct{}), release: make(chan struct{})}
	pb.RegisterMaintenanceServer(server, f)
	pb.RegisterLeaseServer(server, held)
	go func() { _ = server.Serve(counted) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exe, err := os.Executable()
	require.NoError(t, err)
	log, err := os.OpenFile(filepath.Join(t.TempDir(), "stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer log.Close()
	args := []string{"-test.run=^TestSupervisedProbeHelper$", "--", "--endpoint=" + listener.Addr().String(), "--cacert=" + ca,
		"--cert=" + cert, "--key=" + key, "--tls-server-name=probe.test", "--cluster-id=11", "--member-id=22", "--lease-id=33", "--leased-key=owned", "--duration=8s"}
	err = leasefault.WithOriginalProbe(ctx, metricsworker.Command{Executable: exe, Args: args, Env: []string{"KUBEBRAIN_SUPERVISED_PROBE_HELPER=1"}, Stderr: log}, func(p *leasefault.OriginalProbe) error {
		b := leasefault.Binding{LeaseID: 33, ClusterID: 11, InitialMemberID: 22, InitialTerm: 4}
		if _, err := p.AwaitRequest(ctx, b); err != nil {
			return err
		}
		b.Origin = time.Now()
		faultCtx, faultCancel := context.WithDeadline(ctx, b.Origin.Add(5*time.Second))
		defer faultCancel()
		if _, err := p.Pending(faultCtx, b, func(ctx context.Context) error {
			// Test server barrier proves receipt only; not real protected stack evidence.
			select {
			case <-held.received:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			return err
		}
		close(held.release)
		data, err := p.Finish(faultCtx, b.Origin)
		if err != nil {
			return err
		}
		b.SuccessorTerm = 5 // Independently fixed test-server behavior, not parsed from response.
		_, err = leasefault.ValidateOriginalResponse(data, b)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), counted.accepted.Load())
	require.Equal(t, int32(1), f.streams.Load())
	require.Equal(t, int32(1), f.ttlReads.Load())
}
