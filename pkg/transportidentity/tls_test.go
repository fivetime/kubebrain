package transportidentity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/stats"
)

func TestRegistryCarriesTLSStateForConnectionLifetime(t *testing.T) {
	registry := &Registry{}
	local := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2379}
	remote := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43210}
	state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	unregister := registry.Register(local, remote, state)

	ctx := registry.TagConn(context.Background(), &stats.ConnTagInfo{LocalAddr: local, RemoteAddr: remote})
	actual, ok := TLSStateFromContext(ctx)
	require.True(t, ok)
	require.Len(t, actual.VerifiedChains, 1)
	require.Equal(t, uint64(1), registry.TotalConnections())

	unregister()
	ctx = registry.TagConn(context.Background(), &stats.ConnTagInfo{LocalAddr: local, RemoteAddr: remote})
	_, ok = TLSStateFromContext(ctx)
	require.False(t, ok)
}

func TestRegistryStaleUnregisterPreservesReplacement(t *testing.T) {
	registry := &Registry{}
	local := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2379}
	remote := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43210}
	oldUnregister := registry.Register(local, remote, tls.ConnectionState{})
	want := tls.ConnectionState{ServerName: "replacement.example"}
	newUnregister := registry.Register(local, remote, want)

	oldUnregister()
	ctx := registry.TagConn(context.Background(), &stats.ConnTagInfo{LocalAddr: local, RemoteAddr: remote})
	actual, ok := TLSStateFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, want.ServerName, actual.ServerName)

	newUnregister()
	ctx = registry.TagConn(context.Background(), &stats.ConnTagInfo{LocalAddr: local, RemoteAddr: remote})
	_, ok = TLSStateFromContext(ctx)
	require.False(t, ok)
}
