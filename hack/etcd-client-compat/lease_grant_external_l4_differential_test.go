package compat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type externalL4ProxyResponse struct {
	OK                 bool           `json:"ok"`
	Error              string         `json:"error"`
	Endpoint           string         `json:"endpoint"`
	DroppedBytes       int64          `json:"droppedBytes"`
	DroppedConnections int64          `json:"droppedConnections"`
	Dials              map[string]int `json:"dials"`
}

// TestLeaseRevokeResponseLossAcrossExternalL4ProxyDifferential repeats the
// cross-replica Revoke replay through a standalone proxy process. It proves
// that the typed LeaseNotFound replay result does not depend on shared Go state
// in the in-process tcpBridge.
func TestLeaseRevokeResponseLossAcrossExternalL4ProxyDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_TCP_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_TCP_SWITCH_PROXY_BINARY to run the external L4 response-loss differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseRevokeCrossReplicaReplayOutcome{
		ResponseDiscarded:   true,
		ConnectionDropped:   true,
		SecondReplicaDialed: true,
		LeaseNotFound:       true,
		KeyDeleted:          true,
		LeaseMissing:        true,
		LeaseUnlisted:       true,
		RevisionDelta:       1,
	}
	reference := runExternalL4LeaseRevokeResponseLossScenario(t, binary, referenceEndpoints, "etcd", nil)
	require.Equal(t, want, reference)
	require.Equal(t, reference, runExternalL4LeaseRevokeResponseLossScenario(t, binary, kubeBrainEndpoints, "kubebrain", nil))
}

// TestLeaseRevokeResponseLossAcrossExternalL4TLSPassthroughDifferential fixes
// the same replay contract while the standalone proxy forwards opaque TLS
// records. The proxy receives no certificates and therefore cannot terminate,
// inspect, or synthesize the gRPC response.
func TestLeaseRevokeResponseLossAcrossExternalL4TLSPassthroughDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_TCP_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_TCP_SWITCH_PROXY_BINARY to run the external L4 TLS passthrough differential")
	}
	referenceTLS := loadExternalL4ClientTLS(t, "REFERENCE_ETCD")
	kubeBrainTLS := loadExternalL4ClientTLS(t, "KUBEBRAIN")
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopologyWithTLS(t, referenceEndpoints, referenceTLS)
	requireDistinctDirectReplicaTopologyWithTLS(t, kubeBrainEndpoints, kubeBrainTLS)

	want := leaseRevokeCrossReplicaReplayOutcome{
		ResponseDiscarded:   true,
		ConnectionDropped:   true,
		SecondReplicaDialed: true,
		LeaseNotFound:       true,
		KeyDeleted:          true,
		LeaseMissing:        true,
		LeaseUnlisted:       true,
		RevisionDelta:       1,
	}
	reference := runExternalL4LeaseRevokeResponseLossScenario(t, binary, referenceEndpoints, "etcd-tls", referenceTLS)
	require.Equal(t, want, reference)
	require.Equal(t, reference, runExternalL4LeaseRevokeResponseLossScenario(t, binary, kubeBrainEndpoints, "kubebrain-tls", kubeBrainTLS))
}

func runExternalL4LeaseRevokeResponseLossScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
	tlsConfig *tls.Config,
) leaseRevokeCrossReplicaReplayOutcome {
	t.Helper()
	proxy, endpoint := startExternalL4Proxy(t, binary, endpoints[0])
	throughProxy, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, TLS: tlsConfig,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughProxy.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second, TLS: tlsConfig,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-external-l4-revoke-response-loss/%s/%d", instance, time.Now().UnixNano())
	grant, err := throughProxy.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = throughProxy.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	before, err := observer.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)
	initialStats := proxy.command(t, "stats")
	require.Positive(t, initialStats.Dials[grpcTarget(endpoints[0])])

	require.True(t, proxy.command(t, "blackhole-responses").OK)
	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := throughProxy.Revoke(ctx, grant.ID)
		revokeDone <- revokeErr
	}()
	require.Eventually(t, func() bool {
		response, ttlErr := observer.TimeToLive(ctx, grant.ID)
		return ttlErr == nil && response.TTL == -1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return proxy.command(t, "stats").DroppedBytes > initialStats.DroppedBytes
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "drop-connections").OK)
	require.True(t, proxy.command(t, "resume").OK)

	var revokeErr error
	select {
	case revokeErr = <-revokeDone:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.Eventually(t, func() bool {
		return proxy.command(t, "stats").Dials[grpcTarget(endpoints[1])] > 0
	}, 5*time.Second, 10*time.Millisecond)
	after, err := observer.Get(ctx, key)
	require.NoError(t, err)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	listed, err := observer.Leases(ctx)
	require.NoError(t, err)
	leaseUnlisted := true
	for _, lease := range listed.Leases {
		if lease.ID == grant.ID {
			leaseUnlisted = false
		}
	}
	stats := proxy.command(t, "stats")

	return leaseRevokeCrossReplicaReplayOutcome{
		ResponseDiscarded:   stats.DroppedBytes > initialStats.DroppedBytes,
		ConnectionDropped:   stats.DroppedConnections > initialStats.DroppedConnections,
		SecondReplicaDialed: stats.Dials[grpcTarget(endpoints[1])] > 0,
		LeaseNotFound:       errors.Is(revokeErr, rpctypes.ErrLeaseNotFound),
		KeyDeleted:          len(after.Kvs) == 0,
		LeaseMissing:        ttl.TTL == -1,
		LeaseUnlisted:       leaseUnlisted,
		RevisionDelta:       after.Header.Revision - before.Header.Revision,
	}
}

func loadExternalL4ClientTLS(t *testing.T, prefix string) *tls.Config {
	t.Helper()
	caPath := strings.TrimSpace(os.Getenv(prefix + "_TLS_CA_FILE"))
	certPath := strings.TrimSpace(os.Getenv(prefix + "_TLS_CERT_FILE"))
	keyPath := strings.TrimSpace(os.Getenv(prefix + "_TLS_KEY_FILE"))
	serverName := strings.TrimSpace(os.Getenv(prefix + "_TLS_SERVER_NAME"))
	if caPath == "" || certPath == "" || keyPath == "" || serverName == "" {
		t.Skipf("set %s_TLS_CA_FILE, %s_TLS_CERT_FILE, %s_TLS_KEY_FILE, and %s_TLS_SERVER_NAME", prefix, prefix, prefix, prefix)
	}
	caPEM, err := os.ReadFile(caPath)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM), "parse %s TLS CA", prefix)
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      roots,
		Certificates: []tls.Certificate{certificate},
		ServerName:   serverName,
	}
}

type externalL4Proxy struct {
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	cmd    *exec.Cmd
	stderr lockedBuffer
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// TestLeaseGrantResponseLossAcrossExternalL4ProxyDifferential repeats the
// automatic-ID ambiguity through a standalone proxy process. This proves that
// the result is not an artifact of the in-process tcpBridge or shared Go state.
func TestLeaseGrantResponseLossAcrossExternalL4ProxyDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_TCP_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_TCP_SWITCH_PROXY_BINARY to run the external L4 response-loss differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseGrantResponseLossReplayOutcome{
		ResponseDiscarded:    true,
		SecondReplicaDialed:  true,
		GrantReturnedSuccess: true,
		NewLeaseCount:        2,
		ReturnedLeasePresent: true,
		OrphanLeaseCount:     1,
		AllNewLeasesLive:     true,
		RevisionDelta:        0,
	}
	reference := runExternalL4LeaseGrantResponseLossScenario(t, binary, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runExternalL4LeaseGrantResponseLossScenario(t, binary, kubeBrainEndpoints, "kubebrain"))
}

func runExternalL4LeaseGrantResponseLossScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
) leaseGrantResponseLossReplayOutcome {
	t.Helper()
	proxy, endpoint := startExternalL4Proxy(t, binary, endpoints[0])
	throughProxy, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughProxy.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	baseline, err := observer.Leases(ctx)
	require.NoError(t, err)
	baselineIDs := leaseIDSet(baseline)
	warm, err := throughProxy.Leases(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineIDs, leaseIDSet(warm))
	initialStats := proxy.command(t, "stats")
	require.Positive(t, initialStats.Dials[grpcTarget(endpoints[0])])
	probeKey := fmt.Sprintf("/dbaas-external-l4-grant-response-loss/%s/%d", instance, time.Now().UnixNano())
	before, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	require.True(t, proxy.command(t, "blackhole-responses").OK)
	grantDone := make(chan *clientv3.LeaseGrantResponse, 1)
	grantErrDone := make(chan error, 1)
	go func() {
		response, grantErr := throughProxy.Grant(ctx, 60)
		grantDone <- response
		grantErrDone <- grantErr
	}()
	require.Eventually(t, func() bool {
		current, listErr := observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(current, baselineIDs) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return proxy.command(t, "stats").DroppedBytes > initialStats.DroppedBytes
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "drop-connections").OK)
	require.True(t, proxy.command(t, "resume").OK)

	var grant *clientv3.LeaseGrantResponse
	var grantErr error
	select {
	case grant = <-grantDone:
		grantErr = <-grantErrDone
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	var final *clientv3.LeaseLeasesResponse
	require.Eventually(t, func() bool {
		var listErr error
		final, listErr = observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(final, baselineIDs) == 2
	}, 5*time.Second, 10*time.Millisecond)
	stats := proxy.command(t, "stats")
	newIDs := newLeaseIDs(final, baselineIDs)
	returnedPresent := false
	allLive := len(newIDs) == 2
	for _, id := range newIDs {
		returnedPresent = returnedPresent || grant != nil && id == grant.ID
		ttl, ttlErr := observer.TimeToLive(ctx, id)
		allLive = allLive && ttlErr == nil && ttl.TTL > 0
	}
	after, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range newIDs {
			_, _ = observer.Revoke(cleanupCtx, id)
		}
	})
	orphans := len(newIDs)
	if returnedPresent {
		orphans--
	}
	return leaseGrantResponseLossReplayOutcome{
		ResponseDiscarded:    stats.DroppedBytes > initialStats.DroppedBytes,
		SecondReplicaDialed:  stats.Dials[grpcTarget(endpoints[1])] > 0,
		GrantReturnedSuccess: grantErr == nil && grant != nil,
		NewLeaseCount:        len(newIDs),
		ReturnedLeasePresent: returnedPresent,
		OrphanLeaseCount:     orphans,
		AllNewLeasesLive:     allLive,
		RevisionDelta:        after.Header.Revision - before.Header.Revision,
	}
}

func startExternalL4Proxy(t *testing.T, binary, target string) (*externalL4Proxy, string) {
	t.Helper()
	cmd := exec.Command(binary, "--listen=127.0.0.1:0", "--target="+grpcTarget(target))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	proxy := &externalL4Proxy{stdin: stdin, stdout: bufio.NewScanner(stdout), cmd: cmd}
	cmd.Stderr = &proxy.stderr
	require.NoError(t, cmd.Start())
	require.True(t, proxy.stdout.Scan(), "external proxy exited before ready: %s", proxy.stderr.String())
	var ready externalL4ProxyResponse
	require.NoError(t, json.Unmarshal(proxy.stdout.Bytes(), &ready))
	require.True(t, ready.OK, ready.Error)
	require.NotEmpty(t, ready.Endpoint)
	t.Cleanup(func() {
		_ = proxy.stdin.Close()
		done := make(chan error, 1)
		go func() { done <- proxy.cmd.Wait() }()
		select {
		case waitErr := <-done:
			require.NoError(t, waitErr, proxy.stderr.String())
		case <-time.After(5 * time.Second):
			_ = proxy.cmd.Process.Kill()
			<-done
			t.Errorf("external proxy did not exit after stdin close: %s", proxy.stderr.String())
		}
	})
	return proxy, ready.Endpoint
}

func (p *externalL4Proxy) command(t *testing.T, command string) externalL4ProxyResponse {
	t.Helper()
	_, err := io.WriteString(p.stdin, command+"\n")
	require.NoError(t, err, p.stderr.String())
	require.True(t, p.stdout.Scan(), "external proxy exited after %q: %s", command, p.stderr.String())
	var response externalL4ProxyResponse
	require.NoError(t, json.Unmarshal(p.stdout.Bytes(), &response))
	require.Empty(t, response.Error)
	return response
}
