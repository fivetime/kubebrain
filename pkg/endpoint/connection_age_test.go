package endpoint

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kubewharf/kubebrain/pkg/backend"
	mockmetrics "github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForWatchValue(t *testing.T, watch clientv3.WatchChan, key, value string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed before receiving %q", value)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if string(event.Kv.Key) == key && string(event.Kv.Value) == value {
					return
				}
			}
		case <-deadline:
			t.Fatalf("watch did not receive %s=%s after connection aging", key, value)
		}
	}
}

func TestGRPCMaxConnectionAgeReconnectsWatchAndLeaseKeepAlive(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("skipping under -race: vendored cmux data race, not KubeBrain code")
	}
	clientPort, peerPort := freeTCPPort(t), freeTCPPort(t)
	for peerPort == clientPort {
		peerPort = freeTCPPort(t)
	}
	mockCtrl := gomock.NewController(t)
	mockMetrics := mockmetrics.NewMinimalMetrics(mockCtrl)
	testBackend := backend.NewBackend(newBadgerStorage(t, assert.New(t)), backend.Config{
		Prefix:                  "/kubebrain-internal",
		Identity:                fmt.Sprintf("127.0.0.1:%d", peerPort),
		EnableEtcdCompatibility: true,
	}, mockMetrics)
	config := &Config{
		Port:                      clientPort,
		PeerPort:                  peerPort,
		ClientSecurityConfig:      &SecurityConfig{},
		PeerSecurityConfig:        &SecurityConfig{},
		InfoSecurityConfig:        &SecurityConfig{},
		EnableEtcdCompatibility:   true,
		GRPCMaxConnectionAge:      750 * time.Millisecond,
		GRPCMaxConnectionAgeGrace: 250 * time.Millisecond,
	}
	ep := NewEndpoint(testBackend, mockMetrics, config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ep.Run(ctx) }()

	endpointURL := fmt.Sprintf("http://127.0.0.1:%d", clientPort)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpointURL}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	require.Eventually(t, func() bool {
		requestCtx, requestCancel := context.WithTimeout(ctx, time.Second)
		defer requestCancel()
		_, err := client.Put(requestCtx, "/connection-age/ready", "ready")
		return err == nil
	}, 30*time.Second, 250*time.Millisecond, "endpoint never became writable")

	watchKey := "/connection-age/watch"
	watch := client.Watch(ctx, watchKey)
	_, err = client.Put(ctx, watchKey, "before")
	require.NoError(t, err)
	waitForWatchValue(t, watch, watchKey, "before")
	lease, err := client.Grant(ctx, 5)
	require.NoError(t, err)
	keepAlive, err := client.KeepAlive(ctx, lease.ID)
	require.NoError(t, err)
	select {
	case response := <-keepAlive:
		require.NotNil(t, response)
	case <-time.After(5 * time.Second):
		t.Fatal("initial lease keepalive response timed out")
	}

	require.Eventually(t, func() bool {
		return ep.tlsIdentities.TotalConnections() >= 2
	}, 5*time.Second, 50*time.Millisecond, "client did not establish a replacement gRPC transport")
	// Wait past MaxConnectionAgeGrace so the original watch/lease streams must
	// have migrated rather than merely coexisting with the replacement transport.
	time.Sleep(500 * time.Millisecond)
	_, err = client.Put(ctx, watchKey, "after")
	require.NoError(t, err)
	waitForWatchValue(t, watch, watchKey, "after")
	select {
	case response := <-keepAlive:
		require.NotNil(t, response)
		require.Positive(t, response.TTL)
	case <-time.After(5 * time.Second):
		t.Fatal("lease keepalive did not resume after connection aging")
	}
	ttl, err := client.TimeToLive(ctx, lease.ID)
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
}
