package server

import (
	"context"
	"net"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"google.golang.org/grpc/credentials"
)

// reloadedPeerGRPC is client-only and bound to one operator-selected member.
// Reconnects load new material; established connections are not revalidated by
// this adapter. Callers must implement a separate connection retirement policy.
type reloadedPeerGRPC struct {
	source                  transportidentity.ClientCredentialSource
	auth                    *peerRetirementAuthorizer
	local, remote, hostname string
	budget                  time.Duration
}

var _ credentials.TransportCredentials = (*reloadedPeerGRPC)(nil)

func (c *reloadedPeerGRPC) ClientHandshake(parent context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	failed := true
	defer func() {
		if failed && raw != nil {
			_ = raw.Close()
		}
	}()
	name, _, err := net.SplitHostPort(authority)
	if err != nil {
		name = authority
	}
	if parent == nil || raw == nil || name != c.hostname || c.budget <= 0 || c.budget > time.Minute {
		return nil, nil, errPeerCredentialVerification
	}
	ctx, cancel := context.WithTimeout(parent, c.budget)
	defer cancel()
	config, err := reloadedPeerTLSConfig(ctx, c.source, c.auth, c.local, c.remote, c.hostname, []string{"h2"})
	if err != nil {
		return nil, nil, errPeerCredentialVerification
	}
	// grpc-go replaces config.ServerName with authority. Pass the verified
	// operator policy, not an arbitrary caller-provided authority override.
	conn, info, err := credentials.NewTLS(config).ClientHandshake(ctx, config.ServerName, raw)
	if err != nil || ctx.Err() != nil {
		return nil, nil, errPeerCredentialVerification
	}
	state, ok := info.(credentials.TLSInfo)
	if !ok || state.State.NegotiatedProtocol != "h2" {
		_ = conn.Close()
		return nil, nil, errPeerCredentialVerification
	}
	failed = false
	return conn, info, nil
}

func (c *reloadedPeerGRPC) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if raw != nil {
		_ = raw.Close()
	}
	return nil, nil, errPeerCredentialVerification
}

func (c *reloadedPeerGRPC) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.2", ServerName: c.hostname}
}

func (c *reloadedPeerGRPC) Clone() credentials.TransportCredentials {
	clone := *c
	return &clone
}

func (c *reloadedPeerGRPC) OverrideServerName(string) error { return errPeerCredentialVerification }
