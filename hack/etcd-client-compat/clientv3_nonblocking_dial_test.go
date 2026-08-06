package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClientV3NewDoesNotWaitForEndpointConnectivity pins upstream etcd
// 0d20d7da. clientv3.New must only create the client/connection objects; it
// must not wait for the endpoint to become reachable or probe the health API.
func TestClientV3NewDoesNotWaitForEndpointConnectivity(t *testing.T) {
	start := time.Now()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://254.0.0.1:12345"},
		DialTimeout: 5 * time.Second,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Less(t, elapsed, 500*time.Millisecond, "client creation must not wait for DialTimeout or endpoint connectivity")
}
