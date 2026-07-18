package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type getCancelConnectionOutcome struct {
	Attempts               int
	CanceledGets           int
	ResponseBytesDropped   bool
	ConnectionObjectReused bool
	TransportReused        bool
	EveryFollowUpSucceeded bool
}

func TestGetCancelConnectionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run Get cancel connection differential tests")
	}

	require.Equal(t,
		runGetCancelConnectionScenario(t, reference, "etcd"),
		runGetCancelConnectionScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runGetCancelConnectionScenario(t *testing.T, endpoint, instance string) getCancelConnectionOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-get-cancel-connection/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	_, err = client.Put(ctx, key, "seed")
	require.NoError(t, err)
	activeConnection := client.ActiveConnection()
	require.Equal(t, int64(1), bridge.AcceptedConnections())

	const attempts = 4
	canceledGets := 0
	responseBytesDropped := true
	connectionObjectReused := true
	transportReused := true
	everyFollowUpSucceeded := true
	for attempt := 0; attempt < attempts; attempt++ {
		droppedBytesBefore := bridge.DroppedBytes()
		droppedConnectionsBefore := bridge.DroppedConnections()
		acceptedConnectionsBefore := bridge.AcceptedConnections()
		bridge.BlackholeResponses()

		callCtx, callCancel := context.WithCancel(ctx)
		result := make(chan error, 1)
		go func() {
			_, getErr := client.Get(callCtx, key)
			result <- getErr
		}()
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBytesBefore
		}, 2*time.Second, 10*time.Millisecond)
		responseBytesDropped = responseBytesDropped && bridge.DroppedBytes() > droppedBytesBefore
		callCancel()
		getErr := <-result
		require.ErrorIs(t, getErr, context.Canceled)
		canceledGets++

		bridge.Resume()
		sameConnection := client.ActiveConnection() == activeConnection
		require.True(t, sameConnection, "attempt %d replaced the active gRPC connection", attempt)
		connectionObjectReused = connectionObjectReused && sameConnection
		notDropped := bridge.DroppedConnections() == droppedConnectionsBefore
		require.True(t, notDropped, "attempt %d dropped the TCP transport", attempt)

		value := fmt.Sprintf("after-cancel-%d", attempt)
		followUpCtx, followUpCancel := context.WithTimeout(ctx, 5*time.Second)
		_, putErr := client.Put(followUpCtx, key, value)
		read, readErr := client.Get(followUpCtx, key)
		followUpCancel()
		succeeded := putErr == nil && readErr == nil && len(read.Kvs) == 1 &&
			string(read.Kvs[0].Value) == value
		require.True(t, succeeded,
			"attempt %d follow-up failed: put=%v get=%v response=%v",
			attempt, putErr, readErr, read,
		)
		everyFollowUpSucceeded = everyFollowUpSucceeded && succeeded
		sameTransport := bridge.AcceptedConnections() == acceptedConnectionsBefore
		require.True(t, sameTransport, "attempt %d opened a replacement TCP transport", attempt)
		transportReused = transportReused && notDropped && sameTransport
	}

	return getCancelConnectionOutcome{
		Attempts:               attempts,
		CanceledGets:           canceledGets,
		ResponseBytesDropped:   responseBytesDropped,
		ConnectionObjectReused: connectionObjectReused,
		TransportReused:        transportReused,
		EveryFollowUpSucceeded: everyFollowUpSucceeded,
	}
}
