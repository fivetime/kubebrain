package server

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

// handshakeReloadedPeer takes ownership of raw, closing it on every failure.
// The caller supplies an operator-resolved endpoint/holder and a bounded context.
// Each new connection loads fresh material; neither callbacks nor cached sessions
// are inherited from the normal endpoint's TLS configuration. Successful existing
// connections still require a separate rotation/retirement policy.
func handshakeReloadedPeer(ctx context.Context, raw net.Conn, source transportidentity.ClientCredentialSource, auth *peerRetirementAuthorizer, localHolder, remoteHolder, hostname string, protocols []string) (conn *tls.Conn, err error) {
	success := false
	defer func() {
		if !success && raw != nil {
			_ = raw.Close()
		}
	}()
	if raw == nil {
		return nil, errPeerCredentialVerification
	}
	config, err := reloadedPeerTLSConfig(ctx, source, auth, localHolder, remoteHolder, hostname, protocols)
	if err != nil {
		return nil, err
	}
	conn = tls.Client(raw, config)
	if err := conn.HandshakeContext(ctx); err != nil || ctx.Err() != nil {
		return nil, errPeerCredentialVerification
	}
	success = true
	return conn, nil
}

func reloadedPeerTLSConfig(ctx context.Context, source transportidentity.ClientCredentialSource, auth *peerRetirementAuthorizer, localHolder, remoteHolder, hostname string, protocols []string) (*tls.Config, error) {
	if ctx == nil || source == nil || auth == nil || hostname == "" || ctx.Err() != nil {
		return nil, errPeerCredentialVerification
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return nil, errPeerCredentialVerification
	}
	material, err := source.LoadClientCredentialMaterial(ctx)
	if err != nil || ctx.Err() != nil {
		return nil, errPeerCredentialVerification
	}
	certificate, err := validateLocalPeerMaterial(material, auth, localHolder, time.Now())
	if err != nil {
		return nil, errPeerCredentialVerification
	}
	serverName := hostname
	if material.ServerName != "" {
		serverName = material.ServerName
	}
	config := &tls.Config{
		MinVersion: max(material.MinVersion, tls.VersionTLS12), MaxVersion: material.MaxVersion,
		RootCAs: material.Roots.Clone(), ServerName: serverName, Certificates: []tls.Certificate{certificate},
		CipherSuites: append([]uint16(nil), material.CipherSuites...), NextProtos: append([]string(nil), protocols...),
		VerifyConnection: func(state tls.ConnectionState) error {
			_, err := auth.verifyReloadedPeer(material, state, remoteHolder, hostname, time.Now())
			return err
		},
	}
	return config, nil
}
