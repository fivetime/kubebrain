package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseKeepAliveExternalL7ResetOutcome struct {
	InitialResponse     bool
	StreamFailed        bool
	SecondReplicaDialed bool
	RecoveredResponse   bool
	SurvivedOriginalTTL bool
	KeyValue            string
	LeaseTTLPositive    bool
	RevisionDelta       int64
	KeepAliveStayedOpen bool
}

// TestLeaseKeepAliveResumesAcrossExternalL7ResetDifferential fixes clientv3's
// streaming LeaseKeepAlive recovery contract through a standalone gRPC-aware
// proxy. The lease must keep renewing on another replica after only the L7 RPC
// is reset, long enough that the original grant would otherwise have expired.
func TestLeaseKeepAliveResumesAcrossExternalL7ResetDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_GRPC_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_GRPC_SWITCH_PROXY_BINARY to run the external L7 KeepAlive reset differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseKeepAliveExternalL7ResetOutcome{
		InitialResponse: true, StreamFailed: true, SecondReplicaDialed: true,
		RecoveredResponse: true, SurvivedOriginalTTL: true, KeyValue: "kept-alive",
		LeaseTTLPositive: true, RevisionDelta: 0, KeepAliveStayedOpen: true,
	}
	reference := runLeaseKeepAliveExternalL7ResetScenario(t, binary, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runLeaseKeepAliveExternalL7ResetScenario(t, binary, kubeBrainEndpoints, "kubebrain"))
}

func runLeaseKeepAliveExternalL7ResetScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
) leaseKeepAliveExternalL7ResetOutcome {
	t.Helper()
	proxy, endpoint := startExternalL7Proxy(t, binary, endpoints[0])
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
	key := fmt.Sprintf("/dbaas-external-l7-keepalive-reset/%s/%d", instance, time.Now().UnixNano())
	grant, err := throughProxy.Grant(ctx, 3)
	require.NoError(t, err)
	put, err := throughProxy.Put(ctx, key, "kept-alive", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	keepAlive, err := throughProxy.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initial := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	initialTime := time.Now()
	initialStats := proxy.command(t, "stats")
	require.Positive(t, initialStats.Dials[grpcTarget(endpoints[0])])
	require.Equal(t, 1, initialStats.ActiveStreams)

	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "fail-streams").OK)
	require.Eventually(t, func() bool {
		stats := proxy.command(t, "stats")
		return stats.Dials[grpcTarget(endpoints[1])] > 0 && stats.ActiveStreams == 1
	}, 5*time.Second, 10*time.Millisecond)
	resetStats := proxy.command(t, "stats")
	recovered := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)

	// Continue consuming renewals beyond the original three-second grant. A
	// merely reconnected but non-forwarding stream cannot keep the attached key.
	for time.Now().Before(initialTime.Add(5 * time.Second)) {
		receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	}
	get, err := observer.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, get.Kvs, 1)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	keepAliveOpen := true
	select {
	case response, open := <-keepAlive:
		keepAliveOpen = open
		if open {
			require.NotNil(t, response)
			require.Equal(t, grant.ID, response.ID)
			require.Positive(t, response.TTL)
		}
	default:
	}

	return leaseKeepAliveExternalL7ResetOutcome{
		InitialResponse:     initial != nil && initial.TTL > 0,
		StreamFailed:        resetStats.FailedStreams > initialStats.FailedStreams,
		SecondReplicaDialed: resetStats.Dials[grpcTarget(endpoints[1])] > 0,
		RecoveredResponse:   recovered != nil && recovered.ID == grant.ID && recovered.TTL > 0,
		SurvivedOriginalTTL: time.Since(initialTime) > 3*time.Second,
		KeyValue:            string(get.Kvs[0].Value), LeaseTTLPositive: ttl.TTL > 0,
		RevisionDelta:       get.Header.Revision - put.Header.Revision,
		KeepAliveStayedOpen: keepAliveOpen,
	}
}

func receiveExternalL7KeepAlive(
	t *testing.T,
	ctx context.Context,
	responses <-chan *clientv3.LeaseKeepAliveResponse,
	id clientv3.LeaseID,
) *clientv3.LeaseKeepAliveResponse {
	t.Helper()
	select {
	case response, open := <-responses:
		require.True(t, open, "KeepAlive channel closed")
		require.NotNil(t, response)
		require.Equal(t, id, response.ID)
		require.Positive(t, response.TTL)
		return response
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
		return nil
	}
}
