// Package transportidentity carries verified TLS identity across transports
// that terminate TLS before handing a connection to gRPC.
package transportidentity

import (
	"context"
	"crypto/tls"
	"net"
	"sync"

	"google.golang.org/grpc/stats"
)

type tlsStateContextKey struct{}

// WithTLSState attaches an already-verified TLS connection state to an RPC
// context. Callers must only provide state produced by a trusted TLS listener.
func WithTLSState(ctx context.Context, state tls.ConnectionState) context.Context {
	return context.WithValue(ctx, tlsStateContextKey{}, state)
}

// TLSStateFromContext returns the verified TLS state attached to the gRPC
// connection that owns ctx.
func TLSStateFromContext(ctx context.Context) (tls.ConnectionState, bool) {
	state, ok := ctx.Value(tlsStateContextKey{}).(tls.ConnectionState)
	return state, ok
}

// Registry bridges an outer TLS listener and an inner plaintext gRPC server.
type Registry struct {
	states sync.Map
}

type registryEntry struct {
	state tls.ConnectionState
}

func addressKey(local, remote net.Addr) string {
	if local == nil || remote == nil {
		return ""
	}
	return local.Network() + ":" + local.String() + "<-" + remote.Network() + ":" + remote.String()
}

// Register stores state for a live connection and returns its unregister hook.
func (r *Registry) Register(local, remote net.Addr, state tls.ConnectionState) func() {
	key := addressKey(local, remote)
	if key == "" {
		return func() {}
	}
	entry := &registryEntry{state: state}
	r.states.Store(key, entry)
	return func() { r.states.CompareAndDelete(key, entry) }
}

func (r *Registry) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (r *Registry) HandleRPC(context.Context, stats.RPCStats)                       {}

func (r *Registry) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if info == nil {
		return ctx
	}
	state, ok := r.states.Load(addressKey(info.LocalAddr, info.RemoteAddr))
	if !ok {
		return ctx
	}
	return WithTLSState(ctx, state.(*registryEntry).state)
}

func (r *Registry) HandleConn(context.Context, stats.ConnStats) {}
