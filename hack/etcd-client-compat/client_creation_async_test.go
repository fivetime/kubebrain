package compat

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClientCreationDoesNotWaitForConnection pins upstream etcd commit
// 0d20d7da7: constructing a client is asynchronous even when its initial
// endpoint accepts TCP connections but never completes the gRPC handshake.
// The same client must remain usable after the DBaaS endpoint set is replaced.
func TestClientCreationDoesNotWaitForConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	started := time.Now()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{listener.Addr().String()},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Less(t, time.Since(started), time.Second,
		"client creation waited for the configured dial timeout")

	client.SetEndpoints(compatEndpoint(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.Get(ctx, "/a3740/client-creation/missing")
	require.NoError(t, err)
	require.Empty(t, response.Kvs)
	require.NotNil(t, response.Header)
	require.Positive(t, response.Header.Revision)
}
