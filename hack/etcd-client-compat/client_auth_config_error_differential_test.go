package compat

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClientAuthConfigErrorDifferentialAgainstReferenceEtcd pins upstream
// etcd cd42de79d. Mutually exclusive credentials must fail with the exported
// sentinel before the asynchronous client attempts any network access.
func TestClientAuthConfigErrorDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			runClientAuthConfigErrorScenario(t, endpoint)
		})
	}
}

func runClientAuthConfigErrorScenario(t *testing.T, endpoint string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	accepted := make(chan struct{}, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = connection.Close()
			accepted <- struct{}{}
		}
	}()

	client, configErr := clientv3.New(clientv3.Config{
		Endpoints: []string{listener.Addr().String()},
		Username:  "user",
		Password:  "password",
		Token:     "token",
	})
	require.Nil(t, client)
	require.ErrorIs(t, configErr, clientv3.ErrMutuallyExclusiveCfg)
	require.Equal(t, "Username/Password and Token configurations are mutually exclusive", configErr.Error())
	select {
	case <-accepted:
		t.Fatal("invalid auth configuration attempted a network connection")
	case <-time.After(100 * time.Millisecond):
	}

	valid, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, valid.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := valid.Get(ctx, "/dbaas-client-auth-config-error/missing")
	require.NoError(t, err)
	require.Empty(t, response.Kvs)
	require.NotNil(t, response.Header)
	require.Positive(t, response.Header.Revision)

	// Keep the import-level contract explicit even if ErrorIs internals change.
	require.True(t, errors.Is(configErr, clientv3.ErrMutuallyExclusiveCfg))
}
