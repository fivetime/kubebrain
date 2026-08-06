package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClientResolverPrebuildDifferentialAgainstReferenceEtcd pins upstream
// etcd fa38b54dd. clientv3.New installs its initial endpoints before gRPC has
// built the manual resolver; consulting resolver.ClientConn in that window
// used to panic. Replacing the endpoint set immediately after construction
// must remain panic-free and leave the client usable against either dataplane.
func TestClientResolverPrebuildDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			runClientResolverPrebuildScenario(t, endpoint)
		})
	}
}

func runClientResolverPrebuildScenario(t *testing.T, endpoint string) {
	t.Helper()
	for iteration := 0; iteration < 25; iteration++ {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: 5 * time.Second,
		})
		require.NoError(t, err)

		// Exercise updates while grpc.NewClient may still be lazily building the
		// resolver, then prove that the final resolver state reaches the server.
		for update := 0; update < 20; update++ {
			client.SetEndpoints(endpoint)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, getErr := client.Get(ctx, "/a3747/client-resolver-prebuild/missing")
		cancel()
		require.NoError(t, getErr)
		require.Empty(t, response.Kvs)
		require.NotNil(t, response.Header)
		require.Positive(t, response.Header.Revision)
		require.Equal(t, []string{endpoint}, client.Endpoints())
		require.NoError(t, client.Close())
	}
}
