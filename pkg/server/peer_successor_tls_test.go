package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
)

func TestSuccessorProxyTLSBindsExactEndpoint(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	for _, mode := range []string{"correct", "other holder same CA and SAN"} {
		t.Run(mode, func(t *testing.T) {
			served := certs[1]
			if mode != "correct" {
				served = certs[2]
			}
			peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), pool, []tls.Certificate{served}, true)
			base := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
			auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"old": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[1])}, "other": {retirementTestPin(certs[2])}})
			require.NoError(t, err)
			sender, err := newPeerRetirementSender("scope", "old", []string{peer.URL}, base, time.Second)
			require.NoError(t, err)
			d, err := newPeerSuccessorDiscovery(sender, auth, map[string]string{peer.URL: "next"})
			require.NoError(t, err)
			bound, err := d.proxyTLS(base, peer.URL)
			require.NoError(t, err)
			require.Nil(t, base.VerifyConnection, "per-target binding must not mutate shared TLS config")
			address := strings.TrimPrefix(peer.URL, "https://")
			// First demonstrate that the wrong holder's certificate is otherwise
			// valid under ordinary chain and hostname verification.
			plain, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, base)
			require.NoError(t, err)
			require.NoError(t, plain.Close())
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, bound)
			if mode == "correct" {
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			} else {
				require.Error(t, err)
				require.Nil(t, conn)
			}
			_, err = d.proxyTLS(base, "https://unconfigured.invalid")
			require.Error(t, err)
			_, err = d.proxyTLS(nil, peer.URL)
			require.Error(t, err)
			d.proxySource = &handshakeMaterialSource{material: transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}}
			view := &successorCredentialRoutingView{discovery: d}
			credential, err := view.ProxyCredentialsForEndpoint(peer.URL)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			require.NoError(t, err)
			reloaded, _, err := credential.ClientHandshake(ctx, "127.0.0.1", raw)
			if mode == "correct" {
				require.NoError(t, err)
				require.NoError(t, reloaded.Close())
			} else {
				require.Error(t, err)
				require.Nil(t, reloaded)
			}
			_, err = view.ProxyCredentialsForEndpoint("https://unconfigured.invalid")
			require.Error(t, err)
		})
	}
}

func TestSuccessorRoutingViewRequiresEndpointTLS(t *testing.T) {
	view := &successorRoutingView{}
	_, err := view.ProxyTLSForEndpoint("https://unknown.invalid")
	require.Error(t, err)
	// A failed provider must not enable discovery or touch its context path.
	view.discover = func(context.Context) (string, error) { panic("unexpected discovery") }
	view.peerTLS = func(string) (*tls.Config, error) { return nil, errPeerSuccessorUnavailable }
	_, err = view.ProxyTLSForEndpoint("https://unknown.invalid")
	require.ErrorIs(t, err, errPeerSuccessorUnavailable)
}

func TestSuccessorProxyAcceptsOnlyConfiguredCLIAddress(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	base := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
	for _, host := range []string{"peer.test:3380", "[::1]:3380"} {
		t.Run(host, func(t *testing.T) {
			address := "https://" + host
			auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local:3380": {retirementTestPin(certs[0])}, host: {retirementTestPin(certs[1])}})
			require.NoError(t, err)
			sender, err := newPeerRetirementSender("scope", "local:3380", []string{address}, base, time.Second)
			require.NoError(t, err)
			d, err := newPeerSuccessorDiscovery(sender, auth, map[string]string{address: host})
			require.NoError(t, err)
			d.proxySource = &handshakeMaterialSource{material: transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}}
			view := &successorCredentialRoutingView{discovery: d}
			for _, endpoint := range []string{address, host, "local:3380"} {
				bound, err := d.proxyTLS(base, endpoint)
				require.NoError(t, err)
				require.NotNil(t, bound.VerifyConnection)
				credential, err := view.ProxyCredentialsForEndpoint(endpoint)
				require.NoError(t, err)
				want := host
				if endpoint == "local:3380" {
					want = endpoint
				}
				require.Equal(t, want, credential.(*reloadedPeerGRPC).remote)
			}
			for _, endpoint := range []string{"http://" + host, address + "/", address + "?x=1", "other:3380", "https://other:3380", " " + host, "dns:///" + host} {
				_, err := d.proxyTLS(base, endpoint)
				require.Error(t, err, endpoint)
				_, err = view.ProxyCredentialsForEndpoint(endpoint)
				require.Error(t, err, endpoint)
			}
		})
	}
}
