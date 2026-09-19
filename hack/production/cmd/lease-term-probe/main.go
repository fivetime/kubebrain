// lease-term-probe observes one expired renewal without clientv3 stream retries.
// It does not inject faults, create leases, change alarms, or clean up fixtures.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	f := flag.NewFlagSet("lease-term-probe", flag.ContinueOnError)
	endpoint := f.String("endpoint", "", "single direct member host:port, not a load-balanced service")
	ca := f.String("cacert", "", "trusted CA file")
	cert := f.String("cert", "", "client certificate file")
	key := f.String("key", "", "client private key file")
	serverName := f.String("tls-server-name", "", "expected TLS server name")
	id := f.Int64("lease-id", 0, "existing owned expired lease ID (decimal)")
	cluster := f.Uint64("cluster-id", 0, "expected cluster ID")
	member := f.Uint64("member-id", 0, "expected current leader member ID")
	ownedKey := f.String("leased-key", "", "expected key attached to owned lease")
	duration := f.Duration("duration", 2*time.Minute, "whole probe deadline, at most 10m")
	waitExpiry := f.Bool("wait-for-expiry", false, "poll the owned retained lease before the single renewal, within the same connection/deadline")
	if err := f.Parse(args); err != nil {
		return err
	}
	host, port, addressErr := net.SplitHostPort(*endpoint)
	if f.NArg() != 0 || addressErr != nil || host == "" || port == "" || *ca == "" || *cert == "" || *key == "" || *serverName == "" || *id == 0 || *cluster == 0 || *member == 0 || *ownedKey == "" || *duration <= 0 || *duration > 10*time.Minute {
		return errors.New("require direct host:port, full TLS identity, nonzero lease/cluster/member IDs, owned key and bounded duration")
	}
	tlsConfig, err := (transport.TLSInfo{TrustedCAFile: *ca, CertFile: *cert, KeyFile: *key, ServerName: *serverName}).ClientConfig()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	var dials atomic.Int32
	conn, err := grpc.NewClient("passthrough:///"+*endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithDisableRetry(),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			if dials.Add(1) != 1 {
				return nil, errors.New("probe forbids a second connection attempt")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}))
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := probeWithExpiryWait(ctx, conn, *id, *cluster, *member, *ownedKey, out, *waitExpiry); err != nil {
		return err
	}
	if dials.Load() != 1 {
		return fmt.Errorf("connection attempts=%d, expected 1", dials.Load())
	}
	return nil
}

func probe(ctx context.Context, conn grpc.ClientConnInterface, id int64, cluster, member uint64, key string, out io.Writer) error {
	return probeWithExpiryWait(ctx, conn, id, cluster, member, key, out, false)
}

func probeWithExpiryWait(ctx context.Context, conn grpc.ClientConnInterface, id int64, cluster, member uint64, key string, out io.Writer, waitExpiry bool) error {
	if waitExpiry {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("expiry wait requires an existing probe deadline")
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	status, err := pb.NewMaintenanceClient(conn).Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("status preflight: %w", err)
	}
	term := status.GetHeader().GetRaftTerm()
	if status.GetHeader().GetClusterId() != cluster || status.GetHeader().GetMemberId() != member || status.Leader != member || term == 0 {
		return errors.New("preflight cluster/member/leader mismatch")
	}
	lease := pb.NewLeaseClient(conn)
	var ttl *pb.LeaseTimeToLiveResponse
	var granted int64
	for {
		ttl, err = lease.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: id, Keys: true})
		if err != nil {
			return fmt.Errorf("expiry preflight: %w", err)
		}
		if ttl.GetID() != id || ttl.GetGrantedTTL() <= 0 || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != key || ttl.GetHeader().GetClusterId() != cluster || ttl.GetHeader().GetMemberId() != member || ttl.GetHeader().GetRaftTerm() != term {
			return errors.New("preflight requires retained lease with sole owned attachment and matching cluster/member/term")
		}
		if granted != 0 && ttl.GrantedTTL != granted {
			return errors.New("lease grant changed during expiry wait")
		}
		granted = ttl.GrantedTTL
		if err := ctx.Err(); err != nil {
			return err
		}
		if ttl.TTL < 0 {
			break
		}
		if !waitExpiry {
			return errors.New("preflight requires expired retained lease")
		}
		// Read-only polling is not an RPC-error retry or a replacement renewal.
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	enc := json.NewEncoder(out)
	if err := enc.Encode(map[string]any{"phase": "expired_preflight", "at": time.Now().UTC(), "lease_id": id, "ttl": ttl.TTL, "member_id": member, "raft_term": term, "cluster_id": cluster}); err != nil {
		return err
	}
	// Raw generated RPC: no KeepAliveOnce wrapper or application retry loop.
	stream, err := lease.LeaseKeepAlive(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&pb.LeaseKeepAliveRequest{ID: id}); err != nil {
		return err
	}
	// Send success is not proof that the remote handler reached expired wait.
	if err := enc.Encode(map[string]any{"phase": "request_sent", "at": time.Now().UTC(), "lease_id": id}); err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("original keepalive stream: %w", err)
	}
	if response.ID != id || response.TTL < 0 || response.GetHeader().GetClusterId() != cluster || response.GetHeader().GetMemberId() == 0 {
		return errors.New("invalid original-stream response identity or TTL")
	}
	return enc.Encode(map[string]any{"phase": "response", "at": time.Now().UTC(), "lease_id": id, "ttl": response.TTL, "header": response.Header})
}
