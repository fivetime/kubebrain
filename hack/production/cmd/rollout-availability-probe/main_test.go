package main

import (
	"context"
	"crypto/x509"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type fakePDTimestampClient struct {
	delay    time.Duration
	physical int64
	logical  int64
	err      error
}

type fakeTiKVRegionReader struct {
	delay time.Duration
	err   error
}

func (f fakeTiKVRegionReader) Read(ctx context.Context) error {
	if f.delay <= 0 {
		return f.err
	}
	timer := time.NewTimer(f.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return f.err
	}
}

func TestSampleTiKVRegionAcceptsRead(t *testing.T) {
	latency, err := sampleTiKVRegion(context.Background(), fakeTiKVRegionReader{}, time.Second)
	require.NoError(t, err)
	require.Less(t, latency, time.Second)
}

func TestSampleTiKVRegionRejectsSlowAndFailedReads(t *testing.T) {
	latency, err := sampleTiKVRegion(context.Background(), fakeTiKVRegionReader{delay: 50 * time.Millisecond}, 10*time.Millisecond)
	require.ErrorContains(t, err, "TiKV Region read failed")
	require.GreaterOrEqual(t, latency, 10*time.Millisecond)

	wantErr := errors.New("region unavailable")
	_, err = sampleTiKVRegion(context.Background(), fakeTiKVRegionReader{err: wantErr}, time.Second)
	require.ErrorIs(t, err, wantErr)
}

func (f fakePDTimestampClient) GetTS(ctx context.Context) (int64, int64, error) {
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-timer.C:
		}
	}
	return f.physical, f.logical, f.err
}

func TestSamplePDTimestampAcceptsValidTimestamp(t *testing.T) {
	latency, err := samplePDTimestamp(context.Background(), fakePDTimestampClient{physical: 1, logical: 0}, time.Second)
	require.NoError(t, err)
	require.Less(t, latency, time.Second)
}

func TestSamplePDTimestampRejectsSlowAndFailedRequests(t *testing.T) {
	latency, err := samplePDTimestamp(context.Background(), fakePDTimestampClient{delay: 50 * time.Millisecond, physical: 1}, 10*time.Millisecond)
	require.ErrorContains(t, err, "PD TSO request failed")
	require.GreaterOrEqual(t, latency, 10*time.Millisecond)

	wantErr := errors.New("tso unavailable")
	_, err = samplePDTimestamp(context.Background(), fakePDTimestampClient{err: wantErr}, time.Second)
	require.ErrorIs(t, err, wantErr)
}

func TestSamplePDTimestampRejectsInvalidTimestamp(t *testing.T) {
	_, err := samplePDTimestamp(context.Background(), fakePDTimestampClient{physical: 0, logical: 1}, time.Second)
	require.ErrorContains(t, err, "invalid timestamp")
	_, err = samplePDTimestamp(context.Background(), fakePDTimestampClient{physical: 1, logical: -1}, time.Second)
	require.ErrorContains(t, err, "invalid timestamp")
	_, err = samplePDTimestamp(context.Background(), fakePDTimestampClient{physical: 1, logical: 1 << 18}, time.Second)
	require.ErrorContains(t, err, "invalid timestamp")
	_, err = samplePDTimestamp(context.Background(), fakePDTimestampClient{physical: math.MaxInt64>>18 + 1}, time.Second)
	require.ErrorContains(t, err, "invalid timestamp")
}

func TestReadPDLeaderFallsBackAndValidatesIdentity(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/pd/api/v1/leader", request.URL.Path)
		_, _ = w.Write([]byte(`{"name":"pd-1","member_id":42}`))
	}))
	defer good.Close()

	leader, err := readPDLeader(context.Background(), []string{bad.URL, good.URL}, time.Second)
	require.NoError(t, err)
	require.Equal(t, pdLeader{Name: "pd-1", MemberID: 42}, leader)
}

func TestVerifyPDStoresRejectsStaleHeartbeat(t *testing.T) {
	heartbeat := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/pd/api/v1/stores", request.URL.Path)
		_, _ = w.Write([]byte(`{"count":1,"stores":[{"store":{"id":1,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]}`))
	}))
	defer server.Close()

	err := verifyPDStores(context.Background(), []string{server.URL}, time.Second, 20*time.Second, 1)
	require.ErrorContains(t, err, "TiKV store unhealthy")
}

func TestVerifyPDStoresAcceptsExactHealthySet(t *testing.T) {
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"count":1,"stores":[{"store":{"id":1,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]}`))
	}))
	defer server.Close()
	require.NoError(t, verifyPDStores(context.Background(), []string{server.URL}, time.Second, 20*time.Second, 1))
}

func TestVerifyPDStoresRejectsDuplicateIdentityAndMalformedAddress(t *testing.T) {
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	for name, stores := range map[string]string{
		"duplicate id":      `[{"store":{"id":1,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}},{"store":{"id":1,"address":"tikv-1:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]`,
		"duplicate address": `[{"store":{"id":1,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}},{"store":{"id":2,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]`,
		"invalid port":      `[{"store":{"id":1,"address":"tikv-0:0","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}},{"store":{"id":2,"address":"tikv-1:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"count":2,"stores":` + stores + `}`))
			}))
			defer server.Close()
			require.ErrorContains(t, verifyPDStores(context.Background(), []string{server.URL}, time.Second, 20*time.Second, 2), "TiKV store unhealthy")
		})
	}
}

func TestConfigValidation(t *testing.T) {
	snapshotDir := t.TempDir()
	valid := config{endpoint: "http://etcd:2379", directEndpoints: []string{"http://kb-0:3379", "http://kb-1:3379", "http://kb-2:3379"}, prefix: "/probe/", iterations: 1, interval: time.Millisecond, commandTimeout: time.Second, dialTimeout: time.Second, maxLatency: time.Second, maxDirectLatency: 20 * time.Second, leaseTTL: 15, pdEndpoints: []string{"http://pd:2379"}, expectedStores: 3, maxHeartbeatAge: 20 * time.Second, maxTSOLatency: 500 * time.Millisecond, maxRegionLatency: 500 * time.Millisecond, rangeInterval: time.Second, snapshotDelay: time.Second, streamTimeout: time.Second, streamBackoff: time.Millisecond, streamMaxBackoff: time.Second, snapshotDir: snapshotDir, minPublicTCPDials: 1, minDirectTCPDials: 1}
	require.NoError(t, valid.validate())
	entries, err := os.ReadDir(snapshotDir)
	require.NoError(t, err)
	require.Empty(t, entries, "the pre-start write canary must be removed")

	for name, mutate := range map[string]func(*config){
		"endpoint":                  func(cfg *config) { cfg.endpoint = "" },
		"prefix":                    func(cfg *config) { cfg.prefix = "" },
		"iterations":                func(cfg *config) { cfg.iterations = 0 },
		"interval":                  func(cfg *config) { cfg.interval = 0 },
		"command":                   func(cfg *config) { cfg.commandTimeout = 0 },
		"dial":                      func(cfg *config) { cfg.dialTimeout = 0 },
		"latency":                   func(cfg *config) { cfg.maxLatency = 0 },
		"latency cap":               func(cfg *config) { cfg.maxLatency = 2 * cfg.commandTimeout },
		"direct latency":            func(cfg *config) { cfg.maxDirectLatency = 0 },
		"direct latency floor":      func(cfg *config) { cfg.maxDirectLatency = cfg.maxLatency / 2 },
		"lease TTL":                 func(cfg *config) { cfg.leaseTTL = 0 },
		"public TCP dials":          func(cfg *config) { cfg.minPublicTCPDials = 0 },
		"direct TCP dials":          func(cfg *config) { cfg.minDirectTCPDials = 0 },
		"PD endpoints":              func(cfg *config) { cfg.pdEndpoints = nil },
		"direct endpoints":          func(cfg *config) { cfg.directEndpoints = nil },
		"direct endpoint count":     func(cfg *config) { cfg.directEndpoints = cfg.directEndpoints[:2] },
		"direct endpoint scheme":    func(cfg *config) { cfg.directEndpoints[0] = "kb-0:3379" },
		"direct endpoint duplicate": func(cfg *config) { cfg.directEndpoints[1] = cfg.directEndpoints[0] },
		"PD scheme":                 func(cfg *config) { cfg.pdEndpoints = []string{"pd:2379"} },
		"stores":                    func(cfg *config) { cfg.expectedStores = 0 },
		"heartbeat":                 func(cfg *config) { cfg.maxHeartbeatAge = 0 },
		"TSO latency":               func(cfg *config) { cfg.maxTSOLatency = 0 },
		"TSO cap":                   func(cfg *config) { cfg.maxTSOLatency = 2 * cfg.maxLatency },
		"Region latency":            func(cfg *config) { cfg.maxRegionLatency = 0 },
		"Region cap":                func(cfg *config) { cfg.maxRegionLatency = 2 * cfg.maxLatency },
		"RangeStream interval":      func(cfg *config) { cfg.rangeInterval = 0 },
		"Snapshot delay":            func(cfg *config) { cfg.snapshotDelay = 0 },
		"stream timeout":            func(cfg *config) { cfg.streamTimeout = 0 },
		"stream retry backoff":      func(cfg *config) { cfg.streamBackoff = 0 },
		"stream retry backoff cap":  func(cfg *config) { cfg.streamMaxBackoff = cfg.streamBackoff / 2 },
		"snapshot artifact dir":     func(cfg *config) { cfg.snapshotDir = filepath.Join(t.TempDir(), "missing") },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.directEndpoints = append([]string(nil), valid.directEndpoints...)
			mutate(&candidate)
			require.Error(t, candidate.validate())
		})
	}
}

func TestConfigRejectsHTTPSWithoutExplicitTLSIdentity(t *testing.T) {
	cfg := config{endpoint: "https://etcd:2379", directEndpoints: []string{"https://kb-0:3379", "https://kb-1:3379", "https://kb-2:3379"}, prefix: "/probe/", iterations: 1, interval: time.Millisecond, commandTimeout: time.Second, dialTimeout: time.Second, maxLatency: time.Second, maxDirectLatency: 20 * time.Second, leaseTTL: 15, pdEndpoints: []string{"http://pd:2379"}, expectedStores: 3, maxHeartbeatAge: 20 * time.Second, maxTSOLatency: 500 * time.Millisecond, maxRegionLatency: 500 * time.Millisecond, rangeInterval: time.Second, snapshotDelay: time.Second, streamTimeout: time.Second, streamBackoff: time.Millisecond, streamMaxBackoff: time.Second, minPublicTCPDials: 1, minDirectTCPDials: 1}
	require.ErrorContains(t, cfg.validate(), "TLS")
}

func TestCleanupClientConfigRetainsAllDirectEndpoints(t *testing.T) {
	cfg := config{dialTimeout: 3 * time.Second}
	endpoints := []string{"http://kb-0:3379", "http://kb-1:3379", "http://kb-2:3379"}
	clientConfig := cfg.kubeBrainClientConfigForEndpoints(endpoints, nil)
	require.Equal(t, endpoints, clientConfig.Endpoints)
	require.Equal(t, cfg.dialTimeout, clientConfig.DialTimeout)

	endpoints[0] = "http://mutated:3379"
	require.Equal(t, "http://kb-0:3379", clientConfig.Endpoints[0], "cleanup endpoints must be defensively copied")
}

func TestSuccessfulTCPDialCounterCountsOnlyEstablishedConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	counter := &successfulTCPDialCounter{timeout: time.Second}
	connection, err := counter.dialContext(context.Background(), listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, connection.Close())
	serverConnection := <-accepted
	require.NoError(t, serverConnection.Close())
	require.EqualValues(t, 1, counter.count.Load())
	require.NoError(t, listener.Close())

	_, err = counter.dialContext(context.Background(), listener.Addr().String())
	require.Error(t, err)
	require.EqualValues(t, 1, counter.count.Load(), "failed TCP attempts must not become replacement evidence")
}

func TestValidateTCPDialEvidenceRequiresEveryDirectEndpoint(t *testing.T) {
	probes := []*directStreamProbe{
		{endpoint: "one", dialCount: &successfulTCPDialCounter{}},
		{endpoint: "two", dialCount: &successfulTCPDialCounter{}},
		{endpoint: "three", dialCount: &successfulTCPDialCounter{}},
	}
	probes[0].dialCount.count.Store(3)
	probes[1].dialCount.count.Store(2)
	probes[2].dialCount.count.Store(4)
	minimum, err := validateTCPDialEvidence(2, 2, probes, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, minimum)

	_, err = validateTCPDialEvidence(1, 2, probes, 2)
	require.ErrorContains(t, err, "public TCP dial evidence 1")
	probes[1].dialCount.count.Store(1)
	_, err = validateTCPDialEvidence(2, 2, probes, 2)
	require.ErrorContains(t, err, "endpoint=two count=1")
}

func TestConfigRequiresCompleteTLSIdentityAndMatchingSchemes(t *testing.T) {
	valid := config{endpoint: "https://etcd:2379", directEndpoints: []string{"https://kb-0:3379", "https://kb-1:3379", "https://kb-2:3379"}, prefix: "/probe/", iterations: 1, interval: time.Millisecond, commandTimeout: time.Second, dialTimeout: time.Second, maxLatency: time.Second, maxDirectLatency: 20 * time.Second, leaseTTL: 15, pdEndpoints: []string{"http://pd:2379"}, expectedStores: 3, maxHeartbeatAge: 20 * time.Second, maxTSOLatency: 500 * time.Millisecond, maxRegionLatency: 500 * time.Millisecond, rangeInterval: time.Second, snapshotDelay: time.Second, streamTimeout: time.Second, streamBackoff: time.Millisecond, streamMaxBackoff: time.Second, caFile: "/tls/ca.crt", certFile: "/tls/tls.crt", keyFile: "/tls/tls.key", tlsServerName: "kubebrain-client.example", minPublicTCPDials: 1, minDirectTCPDials: 1}
	require.NoError(t, valid.validate())

	for name, mutate := range map[string]func(*config){
		"missing CA":          func(cfg *config) { cfg.caFile = "" },
		"missing cert":        func(cfg *config) { cfg.certFile = "" },
		"missing key":         func(cfg *config) { cfg.keyFile = "" },
		"missing server name": func(cfg *config) { cfg.tlsServerName = "" },
		"public HTTP":         func(cfg *config) { cfg.endpoint = "http://etcd:2379" },
		"direct HTTP":         func(cfg *config) { cfg.directEndpoints[0] = "http://kb-0:3379" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.directEndpoints = append([]string(nil), valid.directEndpoints...)
			mutate(&candidate)
			require.ErrorContains(t, candidate.validate(), "TLS")
		})
	}

	_, err := valid.clientTLSConfig()
	require.ErrorContains(t, err, "open /tls")
}

func TestClientTLSConfigLoadsMutualTLSIdentity(t *testing.T) {
	identity, err := transport.SelfCert(zap.NewNop(), t.TempDir(), []string{"kubebrain-client.example:443"}, 1)
	require.NoError(t, err)
	cfg := config{caFile: identity.CertFile, certFile: identity.CertFile, keyFile: identity.KeyFile, tlsServerName: "kubebrain-client.example"}
	tlsConfig, err := cfg.clientTLSConfig()
	require.NoError(t, err)
	require.Equal(t, "kubebrain-client.example", tlsConfig.ServerName)
	require.NotNil(t, tlsConfig.RootCAs)
	require.NotNil(t, tlsConfig.GetClientCertificate)

	plaintext, err := (config{}).clientTLSConfig()
	require.NoError(t, err)
	require.Nil(t, plaintext)
}

func TestMutualTLSClientHonorsExplicitServerName(t *testing.T) {
	identity, err := transport.SelfCert(zap.NewNop(), t.TempDir(), []string{"kubebrain-client.example:443"}, 1, x509.ExtKeyUsageClientAuth)
	require.NoError(t, err)
	serverInfo := identity
	serverInfo.TrustedCAFile = identity.CertFile
	serverInfo.ClientCertAuth = true
	serverTLS, err := serverInfo.ServerConfig()
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	cfg := config{caFile: identity.CertFile, certFile: identity.CertFile, keyFile: identity.KeyFile, tlsServerName: "kubebrain-client.example"}
	clientTLS, err := cfg.clientTLSConfig()
	require.NoError(t, err)
	cfg.dialTimeout = time.Second
	dialCount := &successfulTCPDialCounter{timeout: cfg.dialTimeout}
	client, err := clientv3.New(cfg.kubeBrainClientConfigWithDialCounter("https://"+listener.Addr().String(), clientTLS, dialCount))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := healthpb.NewHealthClient(client.ActiveConnection()).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
	require.GreaterOrEqual(t, dialCount.count.Load(), int64(1))
}

func TestValidateDirectWatchLatencyAllowsOnlyOneRollingEndpoint(t *testing.T) {
	observations := []directWatchObservation{
		{endpoint: "http://kb-0:3379", latency: time.Second},
		{endpoint: "http://kb-1:3379", latency: 2 * time.Second},
		{endpoint: "http://kb-2:3379", latency: 12 * time.Second},
	}
	require.NoError(t, validateDirectWatchLatency(observations, 5*time.Second, 20*time.Second))

	twoSlow := append([]directWatchObservation(nil), observations...)
	twoSlow[1].latency = 6 * time.Second
	require.ErrorContains(t, validateDirectWatchLatency(twoSlow, 5*time.Second, 20*time.Second), "fewer than 2 direct endpoints")

	unbounded := append([]directWatchObservation(nil), observations...)
	unbounded[2].latency = 21 * time.Second
	require.ErrorContains(t, validateDirectWatchLatency(unbounded, 5*time.Second, 20*time.Second), "exceeds bounded recovery latency")
}

func TestReceiveDirectWatchResponsesDoesNotHeadOfLineBlockFastEndpoints(t *testing.T) {
	slow := make(chan clientv3.WatchResponse, 1)
	fastOne := make(chan clientv3.WatchResponse, 1)
	fastTwo := make(chan clientv3.WatchResponse, 1)
	fastOne <- clientv3.WatchResponse{}
	fastTwo <- clientv3.WatchResponse{}
	probes := []*directStreamProbe{
		{endpoint: "slow", watch: slow},
		{endpoint: "fast-one", watch: fastOne},
		{endpoint: "fast-two", watch: fastTwo},
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		slow <- clientv3.WatchResponse{}
	}()

	results, err := receiveDirectWatchResponses(context.Background(), probes, time.Second)
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.NotEqual(t, "slow", results[0].endpoint, "a slow first ordinal must not hide fast follower responses")
	require.Equal(t, "slow", results[2].endpoint)

	blocked := make(chan clientv3.WatchResponse)
	_, err = receiveDirectWatchResponses(context.Background(), []*directStreamProbe{{endpoint: "blocked", watch: blocked}}, 10*time.Millisecond)
	require.ErrorContains(t, err, "direct watch recovery exceeded")
}
