package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

type credentialExpiryTxnServer struct {
	etcdserverpb.UnimplementedKVServer
	effects atomic.Int32
	respond atomic.Bool
	headers bool
}

func (s *credentialExpiryTxnServer) Txn(ctx context.Context, _ *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// Model an admitted write whose result is withheld after its side effect.
	// This is deliberately not a real TiKV commit-proof fixture.
	s.effects.Add(1)
	if s.respond.Load() {
		return &etcdserverpb.TxnResponse{Succeeded: true}, nil
	}
	if s.headers {
		_ = grpc.SendHeader(ctx, metadata.Pairs("test-admitted", "true"))
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestCredentialExpiryDoesNotReplayAdmittedProxyTxn(t *testing.T) {
	for _, mode := range []string{"before response headers", "after response headers"} {
		t.Run(mode, func(t *testing.T) {
			pool, certs, issuer, signer := retirementTestCertificatesAndIssuer(t)
			leaf := *certs[0].Leaf
			leaf.NotAfter = time.Now().Add(3 * time.Second)
			der, err := x509.CreateCertificate(rand.Reader, &leaf, issuer, leaf.PublicKey, signer)
			require.NoError(t, err)
			certs[0].Certificate = [][]byte{der}
			certs[0].Leaf, err = x509.ParseCertificate(der)
			require.NoError(t, err)
			auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{
				"local": {retirementTestPin(certs[0]), retirementTestPin(certs[2])}, "remote": {retirementTestPin(certs[1])},
			})
			require.NoError(t, err)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			upstream := &credentialExpiryTxnServer{headers: mode == "after response headers"}
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certs[1]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})))
			etcdserverpb.RegisterKVServer(server, upstream)
			healthpb.RegisterHealthServer(server, health.NewServer())
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			defer func() { server.Stop(); require.NoError(t, <-done) }()
			endpoint := "https://" + listener.Addr().String()
			base := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
			sender, err := newPeerRetirementSender("scope", "local", []string{endpoint}, base, time.Second)
			require.NoError(t, err)
			discovery, err := newPeerSuccessorDiscovery(sender, auth, map[string]string{endpoint: "remote"})
			require.NoError(t, err)
			var loads atomic.Int32
			discovery.proxySource = networkCredentialSource(func(context.Context) (transportidentity.ClientCredentialMaterial, error) {
				certificate := certs[0]
				if loads.Add(1) > 1 {
					certificate = certs[2]
				}
				return transportidentity.ClientCredentialMaterial{Roots: pool.Clone(), Certificate: certificate}, nil
			})
			view := &successorCredentialRoutingView{discovery: discovery, successorRoutingView: &successorRoutingView{
				LeaderElection: &successorRoutingElection{address: endpoint, refresh: func(context.Context) error { return nil }},
				peerTLS:        func(endpoint string) (*tls.Config, error) { return discovery.proxyTLS(base, endpoint) },
			}}
			proxy := etcdproxy.NewEtcdProxy(t.Context(), view, base, false, 0)
			defer func() { require.NoError(t, proxy.(interface{ Close() error }).Close()) }()
			require.Eventually(t, func() bool { return proxy.Ready() == nil }, time.Second, 10*time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			response, err := proxy.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("expiry-ambiguous"), Value: []byte("value")},
			}}}})
			require.Error(t, err)
			require.Nil(t, response)
			require.NoError(t, ctx.Err(), "failure must be connection expiration, not caller deadline")
			require.Equal(t, int32(1), upstream.effects.Load())
			require.Eventually(t, func() bool { return loads.Load() >= 2 && proxy.Ready() == nil }, 3*time.Second, 10*time.Millisecond)
			require.Equal(t, int32(1), upstream.effects.Load(), "reconnection must not replay the admitted transaction")
			upstream.respond.Store(true)
			response, err = proxy.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("new-request-after-expiry"), Value: []byte("value")},
			}}}})
			require.NoError(t, err)
			require.True(t, response.Succeeded)
			require.Equal(t, int32(2), upstream.effects.Load(), "only the explicitly new transaction may execute")
		})
	}
}
