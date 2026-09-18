package endpoint

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// Each Endpoint owns its backend but not the shared test storage lifetime.
type sharedPeerEndpointStorage struct{ peerEndpointStorage }

func (sharedPeerEndpointStorage) Close() error { return nil }

func TestExperimentalEndpointPairForwardsThroughDynamicPeerCredentials(t *testing.T) {
	ca := newRotationCA(t)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	writeRotationCA(t, caFile, ca)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	ports := map[int]bool{}
	port := func() int {
		for {
			p := freeTCPPort(t)
			if !ports[p] {
				ports[p] = true
				return p
			}
		}
	}
	var clientPorts, peerPorts [2]int
	var urls [2]string
	var paths [2][2]string
	var certificates [2]tls.Certificate
	pins := make(map[string][]string)
	for i := range urls {
		clientPorts[i], peerPorts[i] = port(), port()
		urls[i] = fmt.Sprintf("https://127.0.0.1:%d", peerPorts[i])
		cert, key := writeRotationCertificate(t, t.TempDir(), fmt.Sprintf("member-%d", i), ca, int64(200+i), "peer.test")
		paths[i] = [2]string{cert, key}
		var err error
		certificates[i], err = tls.LoadX509KeyPair(cert, key)
		require.NoError(t, err)
		leaf, err := x509.ParseCertificate(certificates[i].Certificate[0])
		require.NoError(t, err)
		require.Empty(t, leaf.IPAddresses, "IP dialing must exercise the dynamic source's ServerName policy")
		hash := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
		pins[urls[i]] = []string{hex.EncodeToString(hash[:])}
	}
	storage := memkv.NewKvStorage()
	defer storage.Close()
	metrics := metricmock.NewMinimalMetrics(gomock.NewController(t))
	var clients [2]*clientv3.Client
	for i := range urls {
		b := backend.NewBackend(sharedPeerEndpointStorage{peerEndpointStorage{storage}}, backend.Config{Prefix: "/endpoint-pair", Identity: urls[i], EnableEtcdCompatibility: true}, metrics)
		security := func() *SecurityConfig {
			return &SecurityConfig{CertFile: paths[i][0], KeyFile: paths[i][1], CA: caFile, ClientAuth: true, ServerName: "peer.test"}
		}
		config := &Config{Port: clientPorts[i], PeerPort: peerPorts[i], ClientSecurityConfig: security(), PeerSecurityConfig: security(), InfoSecurityConfig: &SecurityConfig{},
			EnableEtcdCompatibility: true, LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 100 * time.Millisecond,
			ExperimentalPeerRetirement: &PeerRetirementOptions{Scope: b.GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope(),
				HolderPins: pins, EndpointHolders: map[string]string{urls[1-i]: urls[1-i]}, ReadBudget: time.Second,
				OperationBudget: time.Second, SendBudget: time.Second, Concurrency: 2, RequestsPerSecond: 100}}
		require.NoError(t, config.Validate())
		endpoint := NewEndpoint(b, metrics, config)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- endpoint.Run(ctx) }()
		defer func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					require.ErrorIs(t, err, context.DeadlineExceeded, "whole-fixture shutdown may exhaust successor waiting, but other errors are unexpected")
				}
			case <-time.After(10 * time.Second):
				t.Error("endpoint shutdown did not return")
			}
		}()
		var err error
		clients[i], err = clientv3.New(clientv3.Config{Endpoints: []string{fmt.Sprintf("https://127.0.0.1:%d", clientPorts[i])}, DialTimeout: time.Second, DialOptions: []grpc.DialOption{grpc.WithAuthority("peer.test")},
			TLS: &tls.Config{RootCAs: roots, ServerName: "peer.test", Certificates: []tls.Certificate{certificates[0]}}})
		require.NoError(t, err)
		defer clients[i].Close()
		// Start the second member only after the first has become authoritative.
		require.Eventually(t, func() bool {
			probeCtx, finish := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer finish()
			_, err := clients[i].Get(probeCtx, "/endpoint-pair/ready")
			return err == nil
		}, 8*time.Second, 50*time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := clients[1].Status(ctx, clients[1].Endpoints()[0])
	require.NoError(t, err)
	primary, err := clients[0].Status(ctx, clients[0].Endpoints()[0])
	require.NoError(t, err)
	require.NotZero(t, status.Leader)
	require.Equal(t, primary.Header.MemberId, status.Leader)
	require.Equal(t, primary.Header.MemberId, primary.Leader)
	require.NotEqual(t, status.Header.MemberId, status.Leader, "the second endpoint must be a follower")
	put, err := clients[1].Put(ctx, "/endpoint-pair/forwarded", "through-pinned-peer")
	require.NoError(t, err)
	got, err := clients[0].Get(ctx, "/endpoint-pair/forwarded")
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, "through-pinned-peer", string(got.Kvs[0].Value))
	require.Equal(t, put.Header.Revision, got.Kvs[0].ModRevision)
}
