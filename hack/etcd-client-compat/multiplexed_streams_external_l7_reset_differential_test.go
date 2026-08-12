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

type multiplexedStreamsExternalL7ResetOutcome struct {
	WatchCreated           bool
	SameDownstreamPeer     bool
	TwoStreamsFailed       bool
	SecondTargetTwoDials   bool
	WatchFirstValue        string
	WatchSecondValue       string
	WatchRevisionDelta     int64
	KeepAliveInitial       bool
	KeepAliveRecovered     bool
	LeaseSurvived          bool
	LeaseValue             string
	LeaseTTLPositive       bool
	LeaseKeyRevisionStable bool
	WatchStayedOpen        bool
	KeepAliveStayedOpen    bool
}

// TestMultiplexedStreamsResumeAcrossExternalL7ResetDifferential proves that a
// proxy-wide L7 reset can interrupt Watch and LeaseKeepAlive streams sharing a
// single downstream HTTP/2 connection without coupling their recovery.
func TestMultiplexedStreamsResumeAcrossExternalL7ResetDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_GRPC_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_GRPC_SWITCH_PROXY_BINARY to run the multiplexed L7 reset differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := multiplexedStreamsExternalL7ResetOutcome{
		WatchCreated: true, SameDownstreamPeer: true, TwoStreamsFailed: true,
		SecondTargetTwoDials: true, WatchFirstValue: "before-reset",
		WatchSecondValue: "after-reset", WatchRevisionDelta: 1,
		KeepAliveInitial: true, KeepAliveRecovered: true, LeaseSurvived: true,
		LeaseValue: "kept-alive", LeaseTTLPositive: true, LeaseKeyRevisionStable: true,
		WatchStayedOpen: true, KeepAliveStayedOpen: true,
	}
	reference := runMultiplexedStreamsExternalL7ResetScenario(t, binary, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runMultiplexedStreamsExternalL7ResetScenario(t, binary, kubeBrainEndpoints, "kubebrain"))
}

func runMultiplexedStreamsExternalL7ResetScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
) multiplexedStreamsExternalL7ResetOutcome {
	t.Helper()
	proxy, endpoint := startExternalL7Proxy(t, binary, endpoints[0])
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-external-l7-multiplex/%s/%d", instance, time.Now().UnixNano())
	watchKey, leaseKey := prefix+"/watch", prefix+"/lease"
	grant, err := client.Grant(ctx, 3)
	require.NoError(t, err)
	leasePut, err := client.Put(ctx, leaseKey, "kept-alive", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	watch := client.Watch(ctx, watchKey, clientv3.WithCreatedNotify())
	created := receiveExternalL7WatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	keepAlive, err := client.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initialKeepAlive := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	initialTime := time.Now()
	initialStats := proxy.command(t, "stats")
	require.Equal(t, 2, initialStats.ActiveStreams)
	require.Equal(t, 1, initialStats.ActivePeers)

	beforePut, err := observer.Put(ctx, watchKey, "before-reset")
	require.NoError(t, err)
	before := receiveExternalL7WatchEvent(t, ctx, watch)
	require.Len(t, before.Events, 1)
	require.Equal(t, beforePut.Header.Revision, before.Events[0].Kv.ModRevision)

	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "fail-streams").OK)
	require.Eventually(t, func() bool {
		stats := proxy.command(t, "stats")
		return stats.Dials[grpcTarget(endpoints[1])] >= 2 &&
			stats.ActiveStreams == 2 && stats.ActivePeers == 1
	}, 5*time.Second, 10*time.Millisecond)
	resetStats := proxy.command(t, "stats")

	afterPut, err := observer.Put(ctx, watchKey, "after-reset")
	require.NoError(t, err)
	after := receiveExternalL7WatchEvent(t, ctx, watch)
	require.Len(t, after.Events, 1)
	recoveredKeepAlive := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	for time.Now().Before(initialTime.Add(5 * time.Second)) {
		receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	}

	leaseGet, err := observer.Get(ctx, leaseKey)
	require.NoError(t, err)
	require.Len(t, leaseGet.Kvs, 1)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	t.Logf(
		"%s multiplex revisions: lease-put=%d lease-mod=%d after-put=%d final-header=%d",
		instance, leasePut.Header.Revision, leaseGet.Kvs[0].ModRevision,
		afterPut.Header.Revision, leaseGet.Header.Revision,
	)
	watchOpen := channelStillOpenWatch(t, watch)
	keepAliveOpen := channelStillOpenKeepAlive(t, keepAlive, grant.ID)

	return multiplexedStreamsExternalL7ResetOutcome{
		WatchCreated: created.Created, SameDownstreamPeer: initialStats.ActivePeers == 1,
		TwoStreamsFailed:     resetStats.FailedStreams-initialStats.FailedStreams == 2,
		SecondTargetTwoDials: resetStats.Dials[grpcTarget(endpoints[1])] >= 2,
		WatchFirstValue:      string(before.Events[0].Kv.Value),
		WatchSecondValue:     string(after.Events[0].Kv.Value),
		WatchRevisionDelta:   after.Events[0].Kv.ModRevision - before.Events[0].Kv.ModRevision,
		KeepAliveInitial:     initialKeepAlive.TTL > 0,
		KeepAliveRecovered:   recoveredKeepAlive.ID == grant.ID && recoveredKeepAlive.TTL > 0,
		LeaseSurvived:        time.Since(initialTime) > 3*time.Second,
		LeaseValue:           string(leaseGet.Kvs[0].Value), LeaseTTLPositive: ttl.TTL > 0,
		LeaseKeyRevisionStable: leaseGet.Kvs[0].ModRevision == leasePut.Header.Revision,
		WatchStayedOpen:        watchOpen, KeepAliveStayedOpen: keepAliveOpen,
	}
}

func channelStillOpenWatch(t *testing.T, responses clientv3.WatchChan) bool {
	t.Helper()
	select {
	case response, open := <-responses:
		if open {
			require.NoError(t, response.Err())
		}
		return open
	default:
		return true
	}
}

func channelStillOpenKeepAlive(
	t *testing.T,
	responses <-chan *clientv3.LeaseKeepAliveResponse,
	id clientv3.LeaseID,
) bool {
	t.Helper()
	select {
	case response, open := <-responses:
		if open {
			require.NotNil(t, response)
			require.Equal(t, id, response.ID)
			require.Positive(t, response.TTL)
		}
		return open
	default:
		return true
	}
}
