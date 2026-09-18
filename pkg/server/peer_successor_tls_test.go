package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

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
