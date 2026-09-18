package server

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestPeerRetirementConfigurationBindsLocalCredentials(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	lock := election.NewResourceLockManager(election.Config{Prefix: "/config-retirement", Keyspace: "tenant", Identity: "old", Timeout: time.Second}, retirementStorageFixture{kv, 42}).GetResourceLock()
	scope := lock.(election.RetiredOwnershipReleaser).RetirementScope()
	pool, certs := retirementTestCertificates(t)
	makeConfig := func() PeerRetirementConfig {
		return PeerRetirementConfig{Scope: scope, HolderPins: map[string][]string{"old": {retirementTestPin(certs[0])}, "helper": {retirementTestPin(certs[1])}},
			PeerURLs: []string{"https://peer.invalid:3380"}, TLS: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}},
			ReadBudget: time.Second, OperationBudget: time.Second, SendBudget: time.Second, Concurrency: 2, RequestsPerSecond: 10}
	}
	for _, name := range []string{"scope", "shared key", "missing local", "wrong local cert", "wrong signing key", "plaintext", "zero budget", "one member"} {
		t.Run(name, func(t *testing.T) {
			config := makeConfig()
			switch name {
			case "scope":
				config.Scope = "wrong"
			case "shared key":
				config.HolderPins["helper"] = config.HolderPins["old"]
			case "missing local":
				config.HolderPins["other"] = config.HolderPins["old"]
				delete(config.HolderPins, "old")
			case "wrong local cert":
				config.TLS.Certificates = []tls.Certificate{certs[1]}
			case "wrong signing key":
				config.TLS.Certificates[0].PrivateKey = certs[1].PrivateKey
			case "plaintext":
				config.PeerURLs[0] = "http://peer.invalid:3380"
			case "zero budget":
				config.SendBudget = 0
			case "one member":
				delete(config.HolderPins, "helper")
			}
			protocol, err := preparePeerRetirement(lock, config)
			require.Error(t, err)
			require.Nil(t, protocol)
			// The fake backend panics if any operation other than GetResourceLock
			// is invoked: invalid configuration must not construct/start services.
			s, err := NewServerWithPeerRetirement(context.Background(), &retirementLifecycleBackend{lock: lock}, nil, Config{}, config)
			require.Error(t, err)
			require.Nil(t, s)
		})
	}
	config := makeConfig()
	protocol, err := preparePeerRetirement(lock, config)
	require.NoError(t, err)
	config.PeerURLs[0] = "http://modified.invalid"
	delete(config.HolderPins, "old")
	config.TLS.Certificates[0] = certs[1]
	require.Contains(t, protocol.handler.auth.holders, "old")
	require.Equal(t, "https://peer.invalid:3380"+peerRetirementPath, protocol.sender.endpoints[0])
	require.Equal(t, retirementTestPin(certs[0]), retirementTestPin(protocol.sender.client.Transport.(*http.Transport).TLSClientConfig.Certificates[0]))
	for _, s := range []*server{{}, {retirement: protocol}} {
		_, present := s.GetPeerHttpHandlers()[peerRetirementPath]
		require.Equal(t, s.retirement != nil, present)
		require.NotContains(t, s.GetClientHttpHandlers(), peerRetirementPath)
		require.NotContains(t, s.GetInfoHttpHandlers(), peerRetirementPath)
	}
}

func TestRetirementServerConstructorStartsAfterValidationAndCloses(t *testing.T) {
	kv := memkv.NewKvStorage()
	metrics := metricmock.NewMinimalMetrics(gomock.NewController(t))
	b := backend.NewBackend(retirementStorageFixture{kv, 42}, backend.Config{Prefix: "/constructor-retirement", Keyspace: "tenant", Identity: "old", EnableEtcdCompatibility: true}, metrics)
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	// Model a joining follower in an established cluster. A completely absent
	// election record takes the existing fail-closed voluntary-drain timeout;
	// this fixture is testing constructor isolation, not changing that policy.
	helper := election.NewResourceLockManager(election.Config{Prefix: "/constructor-retirement", Keyspace: "tenant", Identity: "helper", Timeout: time.Second}, retirementStorageFixture{kv, 42}).GetResourceLock()
	require.NoError(t, helper.Create(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "helper", LeaseDurationSeconds: 30}))
	blocking := &blockingLeadershipPrevalidationBackend{Backend: b, entered: make(chan struct{}), release: make(chan struct{})}
	pool, certs := retirementTestCertificates(t)
	config := PeerRetirementConfig{Scope: b.GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope(),
		HolderPins: map[string][]string{"old": {retirementTestPin(certs[0])}, "helper": {retirementTestPin(certs[1])}},
		PeerURLs:   []string{"https://peer.invalid:3380"}, TLS: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}},
		ReadBudget: time.Second, OperationBudget: time.Second, SendBudget: time.Second, Concurrency: 2, RequestsPerSecond: 10}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewServerWithPeerRetirement(ctx, blocking, metrics, Config{}, config)
	require.NoError(t, err)
	defer func() { cancel(); require.NoError(t, s.Close()) }()
	select {
	case <-blocking.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("server never reached prevalidation")
	}
	require.Contains(t, s.GetPeerHttpHandlers(), peerRetirementPath)
	require.NotContains(t, s.GetClientHttpHandlers(), peerRetirementPath)
	require.NotContains(t, s.GetInfoHttpHandlers(), peerRetirementPath)
}
