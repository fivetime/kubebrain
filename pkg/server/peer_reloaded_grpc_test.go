package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestReloadedGRPCHandshakePolicies(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local": {retirementTestPin(certs[0])}, "remote": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	for _, mode := range []string{"valid", "removed CA", "wrong pin", "wrong authority", "no h2", "load error"} {
		t.Run(mode, func(t *testing.T) {
			peer := retirementHandlerServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), pool, []tls.Certificate{certs[1]}, mode != "no h2")
			source := &handshakeMaterialSource{material: transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}}
			creds := &reloadedPeerGRPC{source: source, auth: auth, local: "local", remote: "remote", hostname: "127.0.0.1", budget: time.Second}
			authority := "127.0.0.1"
			switch mode {
			case "removed CA":
				source.material.Roots = x509.NewCertPool()
			case "wrong pin":
				creds.remote = "local"
			case "wrong authority":
				authority = "unconfigured.invalid"
			case "load error":
				source.err = errPeerCredentialVerification
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(peer.URL, "https://"))
			require.NoError(t, err)
			tracked := &handshakeTrackedConn{Conn: raw}
			defer tracked.Close()
			conn, info, err := creds.Clone().ClientHandshake(ctx, authority, tracked)
			if mode == "valid" {
				require.NoError(t, err)
				tlsInfo, ok := info.(credentials.TLSInfo)
				require.True(t, ok)
				require.Equal(t, credentials.PrivacyAndIntegrity, tlsInfo.SecurityLevel)
				require.Equal(t, "h2", tlsInfo.State.NegotiatedProtocol)
				require.NoError(t, auth.authorize(&tlsInfo.State, "scope", "remote", time.Now()))
				require.NoError(t, conn.Close())
				// The same credentials object must not retain the trusted CA
				// snapshot for its next connection.
				source.material.Roots = x509.NewCertPool()
				nextRaw, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(peer.URL, "https://"))
				require.NoError(t, err)
				nextTracked := &handshakeTrackedConn{Conn: nextRaw}
				nextConn, nextInfo, err := creds.ClientHandshake(ctx, authority, nextTracked)
				require.ErrorIs(t, err, errPeerCredentialVerification)
				require.Nil(t, nextConn)
				require.Nil(t, nextInfo)
				require.True(t, nextTracked.closed.Load())
				require.Equal(t, 2, source.loads)
			} else {
				require.ErrorIs(t, err, errPeerCredentialVerification)
				require.Nil(t, conn)
				require.Nil(t, info)
				require.True(t, tracked.closed.Load())
			}
			require.Error(t, creds.OverrideServerName("other"))
			require.Equal(t, "127.0.0.1", creds.Info().ServerName)
		})
	}
}

func TestReloadedGRPCRealHealthRPC(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local": {retirementTestPin(certs[0])}, "remote": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certs[1]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})))
	healthpb.RegisterHealthServer(server, health.NewServer())
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); require.NoError(t, <-done) }()
	source := &handshakeMaterialSource{material: transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}}
	creds := &reloadedPeerGRPC{source: source, auth: auth, local: "local", remote: "remote", hostname: "127.0.0.1", budget: time.Second}
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
}
