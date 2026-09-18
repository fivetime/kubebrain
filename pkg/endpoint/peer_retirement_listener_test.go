package endpoint

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type peerEndpointStorage struct{ storage.KvStorage }

func (peerEndpointStorage) ClusterID() uint64 { return 42 }

func TestExperimentalPeerEndpointStartsAndIsolatesControlRoutes(t *testing.T) {
	t.Run("multiplexed", func(t *testing.T) { runExperimentalPeerEndpointRoutes(t, false) })
	t.Run("native gRPC aging", func(t *testing.T) { runExperimentalPeerEndpointRoutes(t, true) })
}

func runExperimentalPeerEndpointRoutes(t *testing.T, aging bool) {
	ports := make(map[int]bool)
	port := func() int {
		for {
			p := freeTCPPort(t)
			if !ports[p] {
				ports[p] = true
				return p
			}
		}
	}
	clientPort, peerPort, infoPort := port(), port(), port()
	self := fmt.Sprintf("https://127.0.0.1:%d", peerPort)
	ca := newRotationCA(t)
	paths := make([][2]string, 3)
	certificates := make([]tls.Certificate, 3)
	pins := make([]string, 3)
	for i := range paths {
		cert, key := writeRotationCertificate(t, t.TempDir(), fmt.Sprintf("holder-%d", i), ca, int64(100+i), "peer.test")
		paths[i] = [2]string{cert, key}
		var err error
		certificates[i], err = tls.LoadX509KeyPair(cert, key)
		require.NoError(t, err)
		leaf, err := x509.ParseCertificate(certificates[i].Certificate[0])
		require.NoError(t, err)
		hash := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
		pins[i] = hex.EncodeToString(hash[:])
	}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	writeRotationCA(t, caFile, ca)
	security := func() *SecurityConfig {
		return &SecurityConfig{CertFile: paths[0][0], KeyFile: paths[0][1], CA: caFile, ClientAuth: true, ServerName: "peer.test"}
	}
	metrics := metricmock.NewMinimalMetrics(gomock.NewController(t))
	b := backend.NewBackend(peerEndpointStorage{memkv.NewKvStorage()}, backend.Config{Prefix: "/endpoint-retirement", Identity: self, EnableEtcdCompatibility: true}, metrics)
	scope := b.GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope()
	config := &Config{Port: clientPort, PeerPort: peerPort, InfoPort: infoPort,
		ClientSecurityConfig: security(), PeerSecurityConfig: security(), InfoSecurityConfig: &SecurityConfig{}, EnableEtcdCompatibility: true,
		LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 100 * time.Millisecond,
		ExperimentalPeerRetirement: &PeerRetirementOptions{Scope: scope, HolderPins: map[string][]string{self: {pins[0]}, "remote": {pins[1]}},
			EndpointHolders: map[string]string{"https://127.0.0.1:1": "remote"}, ReadBudget: time.Second, OperationBudget: time.Second,
			SendBudget: 250 * time.Millisecond, Concurrency: 2, RequestsPerSecond: 100}}
	if aging {
		config.GRPCMaxConnectionAge = time.Hour
		config.GRPCMaxConnectionAgeGrace = time.Second
	}
	require.NoError(t, config.Validate())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	endpoint := NewEndpoint(b, metrics, config)
	go func() { done <- endpoint.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			// This single-member fixture intentionally has no serving successor.
			// Preserve and check the fail-closed shutdown result; route isolation
			// is not evidence of a successful production leadership handoff.
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(10 * time.Second):
			t.Error("endpoint did not stop")
		}
	}()
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	client := func(cert *tls.Certificate) *http.Client {
		tlsConfig := &tls.Config{RootCAs: roots, ServerName: "peer.test", MinVersion: tls.VersionTLS12}
		if cert != nil {
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		c := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: tlsConfig, Protocols: protocols, DisableKeepAlives: true}}
		t.Cleanup(c.CloseIdleConnections)
		return c
	}
	valid := client(&certificates[1])
	requestIdentity := func(c *http.Client, base, path, instance, holder string) (int, error) {
		r, err := http.NewRequest(http.MethodPost, base+path, nil)
		if err != nil {
			return 0, err
		}
		r.Header.Set("X-Kubebrain-Retirement-Instance", instance)
		r.Header.Set("X-Kubebrain-Retirement-Holder", holder)
		response, err := c.Do(r)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		return response.StatusCode, nil
	}
	request := func(c *http.Client, base, path string) (int, error) {
		return requestIdentity(c, base, path, scope, "remote")
	}
	require.Eventually(t, func() bool {
		code, err := request(valid, self, "/internal/successor/v1")
		return err == nil && code == http.StatusNoContent
	}, 8*time.Second, 50*time.Millisecond)
	code, err := request(valid, self, "/internal/retirement/v1")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, code, "authenticated retirement route must reject the missing ownership condition")
	code, err = request(client(&certificates[2]), self, "/internal/successor/v1")
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, code)
	_, err = request(client(nil), self, "/internal/successor/v1")
	require.Error(t, err, "missing client certificate must fail TLS")
	for _, path := range []string{"/internal/retirement/v1", "/internal/successor/v1"} {
		code, err := requestIdentity(valid, self, path, scope+":probe-wrong-scope", "remote")
		require.NoError(t, err)
		require.Equal(t, http.StatusForbidden, code, "verified member must not cross backend scope")
		code, err = requestIdentity(valid, self, path, scope, self)
		require.NoError(t, err)
		require.Equal(t, http.StatusForbidden, code, "remote member key must not impersonate receiver")
		code, err = request(valid, fmt.Sprintf("https://127.0.0.1:%d", clientPort), path)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, code)
		code, err = request(valid, fmt.Sprintf("http://127.0.0.1:%d", infoPort), path)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, code)
	}
}
