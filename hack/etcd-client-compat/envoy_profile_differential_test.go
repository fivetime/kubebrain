package compat

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type envoyProfileOutcome struct {
	UnaryRoundTrip       bool
	WatchCreated         bool
	WatchDelivered       bool
	KeepAliveInitial     bool
	LeaseSurvived        bool
	LeaseTTLPositive     bool
	MemberListComplete   bool
	StatusMemberPositive bool
	EnvoyUpstreamTraffic bool
	UpstreamMigrated     bool
	WatchStayedOpen      bool
	KeepAliveStayedOpen  bool
	ClientCertRequired   bool
	PlaintextRejected    bool
}

func TestEnvoyPlaintextProfileDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_ENVOY_BINARY"))
	bootstrap := strings.TrimSpace(os.Getenv("ENVOY_BOOTSTRAP_TEMPLATE"))
	if binary == "" || bootstrap == "" {
		t.Skip("set EXTERNAL_ENVOY_BINARY and ENVOY_BOOTSTRAP_TEMPLATE to run the real Envoy differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := envoyProfileOutcome{
		UnaryRoundTrip: true, WatchCreated: true, WatchDelivered: true,
		KeepAliveInitial: true, LeaseSurvived: true, LeaseTTLPositive: true,
		MemberListComplete: true, StatusMemberPositive: true, EnvoyUpstreamTraffic: true,
		UpstreamMigrated: true,
		WatchStayedOpen:  true, KeepAliveStayedOpen: true,
	}
	reference := runEnvoyProfileScenario(t, binary, bootstrap, referenceEndpoints, "etcd", nil, 2379,
		"cluster.kubebrain.upstream_rq_total")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runEnvoyProfileScenario(t, binary, bootstrap, kubeBrainEndpoints, "kubebrain", nil, 2379,
		"cluster.kubebrain.upstream_rq_total"))
}

func TestEnvoyTLSPassthroughProfileDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_ENVOY_BINARY"))
	bootstrap := strings.TrimSpace(os.Getenv("ENVOY_BOOTSTRAP_TEMPLATE"))
	if binary == "" || bootstrap == "" {
		t.Skip("set EXTERNAL_ENVOY_BINARY and ENVOY_BOOTSTRAP_TEMPLATE to run the real Envoy TLS passthrough differential")
	}
	referenceTLS := loadExternalL4ClientTLS(t, "REFERENCE_ETCD")
	kubeBrainTLS := loadExternalL4ClientTLS(t, "KUBEBRAIN")
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopologyWithTLS(t, referenceEndpoints, referenceTLS)
	requireDistinctDirectReplicaTopologyWithTLS(t, kubeBrainEndpoints, kubeBrainTLS)

	want := envoyProfileOutcome{
		UnaryRoundTrip: true, WatchCreated: true, WatchDelivered: true,
		KeepAliveInitial: true, LeaseSurvived: true, LeaseTTLPositive: true,
		MemberListComplete: true, StatusMemberPositive: true, EnvoyUpstreamTraffic: true,
		UpstreamMigrated: true,
		WatchStayedOpen:  true, KeepAliveStayedOpen: true,
		ClientCertRequired: true, PlaintextRejected: true,
	}
	reference := runEnvoyProfileScenario(t, binary, bootstrap, referenceEndpoints, "etcd-tls", referenceTLS, 2380,
		"cluster.kubebrain.upstream_cx_total")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runEnvoyProfileScenario(t, binary, bootstrap, kubeBrainEndpoints, "kubebrain-tls",
		kubeBrainTLS, 2380, "cluster.kubebrain.upstream_cx_total"))
}

func runEnvoyProfileScenario(
	t *testing.T,
	binary string,
	bootstrap string,
	endpoints []string,
	instance string,
	tlsConfig *tls.Config,
	listenerTemplatePort int,
	trafficStat string,
) envoyProfileOutcome {
	t.Helper()
	clientPort := reserveLocalPort(t)
	adminPort := reserveLocalPort(t)
	for adminPort == clientPort {
		adminPort = reserveLocalPort(t)
	}
	unusedListenerPort := reserveLocalPort(t)
	for unusedListenerPort == clientPort || unusedListenerPort == adminPort {
		unusedListenerPort = reserveLocalPort(t)
	}
	upstreamHost, upstreamPortText, err := net.SplitHostPort(endpoints[0])
	require.NoError(t, err)
	upstreamPort, err := strconv.Atoi(upstreamPortText)
	require.NoError(t, err)
	configData, err := os.ReadFile(bootstrap)
	require.NoError(t, err)
	config := string(configData)
	config = replaceEnvoyConfigValue(t, config,
		"address: kubebrain-envoy-upstream.kubebrain-system.svc.cluster.local", "address: "+upstreamHost)
	config = replaceEnvoyConfigValue(t, config, "port_value: 3379", fmt.Sprintf("port_value: %d", upstreamPort))
	config = replaceEnvoyConfigValue(t, config, fmt.Sprintf("port_value: %d", listenerTemplatePort),
		fmt.Sprintf("port_value: %d", clientPort))
	unusedTemplatePort := 2379
	if listenerTemplatePort == unusedTemplatePort {
		unusedTemplatePort = 2380
	}
	config = replaceEnvoyConfigValue(t, config, fmt.Sprintf("port_value: %d", unusedTemplatePort),
		fmt.Sprintf("port_value: %d", unusedListenerPort))
	config = replaceEnvoyConfigValue(t, config, "port_value: 9901", fmt.Sprintf("port_value: %d", adminPort))
	configPath := filepath.Join(t.TempDir(), "bootstrap.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0600))

	cmd, stderr := startEnvoyProcess(t, binary, configPath)
	t.Cleanup(func() { stopEnvoyProcess(t, cmd) })
	adminURL := fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	waitForEnvoyReady(t, adminURL, stderr)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	proxyEndpoint := fmt.Sprintf("127.0.0.1:%d", clientPort)
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{proxyEndpoint}, DialTimeout: 3 * time.Second, TLS: tlsConfig,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second, TLS: tlsConfig,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	prefix := fmt.Sprintf("/dbaas-envoy-plaintext/%s/%d", instance, time.Now().UnixNano())
	unaryKey, watchKey, leaseKey := prefix+"/unary", prefix+"/watch", prefix+"/lease"
	_, err = client.Put(ctx, unaryKey, "through-envoy")
	require.NoError(t, err)
	unary, err := client.Get(ctx, unaryKey)
	require.NoError(t, err)
	require.Len(t, unary.Kvs, 1)

	watch := client.Watch(ctx, watchKey, clientv3.WithCreatedNotify())
	created := receiveExternalL7WatchResponse(t, ctx, watch)
	grant, err := client.Grant(ctx, 3)
	require.NoError(t, err)
	_, err = client.Put(ctx, leaseKey, "kept-alive", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	keepAlive, err := client.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initialKeepAlive := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	initialTime := time.Now()

	// Restart the real Envoy process on the same downstream address with its
	// upstream changed from replica 0 to replica 1. clientv3 must rebuild both
	// long streams through the new Envoy/upstream connection pools.
	stopEnvoyProcess(t, cmd)
	secondHost, secondPortText, err := net.SplitHostPort(endpoints[1])
	require.NoError(t, err)
	secondPort, err := strconv.Atoi(secondPortText)
	require.NoError(t, err)
	secondConfig := replaceEnvoyConfigValue(t, config, "address: "+upstreamHost, "address: "+secondHost)
	secondConfig = replaceEnvoyConfigValue(t, secondConfig,
		fmt.Sprintf("port_value: %d", upstreamPort), fmt.Sprintf("port_value: %d", secondPort))
	require.NoError(t, os.WriteFile(configPath, []byte(secondConfig), 0600))
	cmd, stderr = startEnvoyProcess(t, binary, configPath)
	waitForEnvoyReady(t, adminURL, stderr)
	_, err = observer.Put(ctx, watchKey, "watched")
	require.NoError(t, err)
	watchResponse := receiveExternalL7WatchEvent(t, ctx, watch)
	require.Len(t, watchResponse.Events, 1)
	recoveredKeepAlive := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	for time.Now().Before(initialTime.Add(5 * time.Second)) {
		receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	}
	leaseGet, err := observer.Get(ctx, leaseKey)
	require.NoError(t, err)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	members, err := client.MemberList(ctx)
	require.NoError(t, err)
	status, err := client.Status(ctx, proxyEndpoint)
	require.NoError(t, err)
	statsResponse, err := http.Get(adminURL + "/stats?filter=" + trafficStat)
	require.NoError(t, err)
	statsBody, err := io.ReadAll(io.LimitReader(statsResponse.Body, 4096))
	require.NoError(t, err)
	require.NoError(t, statsResponse.Body.Close())
	clientCertRequired, plaintextRejected := false, false
	if tlsConfig != nil {
		withoutCertificate := tlsConfig.Clone()
		withoutCertificate.Certificates = nil
		clientCertRequired = envoyProfileRPCRejected(t, proxyEndpoint, withoutCertificate)
		plaintextRejected = envoyProfileRPCRejected(t, proxyEndpoint, nil)
	}

	return envoyProfileOutcome{
		UnaryRoundTrip:       string(unary.Kvs[0].Value) == "through-envoy",
		WatchCreated:         created.Created,
		WatchDelivered:       string(watchResponse.Events[0].Kv.Value) == "watched",
		KeepAliveInitial:     initialKeepAlive.TTL > 0,
		LeaseSurvived:        time.Since(initialTime) > 3*time.Second && len(leaseGet.Kvs) == 1,
		LeaseTTLPositive:     ttl.TTL > 0,
		MemberListComplete:   len(members.Members) == 3,
		StatusMemberPositive: status.Header.MemberId > 0,
		EnvoyUpstreamTraffic: statsResponse.StatusCode == http.StatusOK &&
			strings.Contains(string(statsBody), trafficStat),
		UpstreamMigrated: recoveredKeepAlive.TTL > 0 &&
			string(watchResponse.Events[0].Kv.Value) == "watched",
		WatchStayedOpen:     channelStillOpenWatch(t, watch),
		KeepAliveStayedOpen: channelStillOpenKeepAlive(t, keepAlive, grant.ID),
		ClientCertRequired:  clientCertRequired,
		PlaintextRejected:   plaintextRejected,
	}
}

func envoyProfileRPCRejected(t *testing.T, endpoint string, tlsConfig *tls.Config) bool {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second, TLS: tlsConfig})
	if err != nil {
		return true
	}
	defer func() { require.NoError(t, client.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.Get(ctx, "/dbaas-envoy-tls/rejected-client-probe")
	return err != nil
}

func startEnvoyProcess(t *testing.T, binary, configPath string) (*exec.Cmd, *lockedBuffer) {
	t.Helper()
	cmd := exec.Command(binary, "-c", configPath, "--disable-hot-restart", "--log-level", "warning")
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())
	return cmd, stderr
}

func stopEnvoyProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-waitDone
	}
}

func waitForEnvoyReady(t *testing.T, adminURL string, stderr *lockedBuffer) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, requestErr := http.Get(adminURL + "/ready")
		if requestErr != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond, "Envoy did not become ready: %s", stderr.String())
}

func reserveLocalPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func replaceEnvoyConfigValue(t *testing.T, config, old, replacement string) string {
	t.Helper()
	require.Equal(t, 1, strings.Count(config, old), "Envoy bootstrap replacement target %q must be unique", old)
	return strings.Replace(config, old, replacement, 1)
}
