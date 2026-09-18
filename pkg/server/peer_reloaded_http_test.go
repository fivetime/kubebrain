package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
)

func TestReloadedControlHTTPRefreshesEveryRequest(t *testing.T) {
	_, pool, certs, _, condition := retirementHandlerFixture(t)
	auth, err := newPeerRetirementAuthorizer("instance", map[string][]string{
		"old": {retirementTestPin(certs[0]), retirementTestPin(certs[2])}, "next": {retirementTestPin(certs[1])},
	})
	require.NoError(t, err)
	var releases, requests atomic.Int32
	limits, err := newPeerRetirementHandler(auth, func(context.Context, election.OwnershipCondition) error {
		releases.Add(1)
		return nil
	}, time.Second, time.Second, 2, 100)
	require.NoError(t, err)
	successor := &peerSuccessorHandler{limits: limits, holder: "next", ready: func() bool { return true }}
	serials := make(chan string, 16)
	peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.ProtoMajor != 1 {
			t.Error("reloadable control transport must not multiplex HTTP/2")
		}
		serials <- r.TLS.PeerCertificates[0].SerialNumber.String()
		if r.URL.Path == peerRetirementPath {
			limits.ServeHTTP(w, r)
		} else {
			successor.ServeHTTP(w, r)
		}
	}), pool, []tls.Certificate{certs[1]}, true)
	sender, err := newPeerRetirementSender("instance", "old", []string{peer.URL}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, time.Second)
	require.NoError(t, err)
	source := &handshakeMaterialSource{}
	for _, mode := range []string{"initial", "rotated", "removed CA", "load error", "wrong local pin", "wrong remote pin", "recovered"} {
		t.Run(mode, func(t *testing.T) {
			source.material = transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}
			source.err = nil
			switch mode {
			case "rotated":
				source.material.Certificate = certs[2]
			case "removed CA":
				source.material.Roots = x509.NewCertPool()
			case "load error":
				source.err = errors.New("must not fall back")
			case "wrong local pin":
				source.material.Certificate = certs[3]
			}
			clientAuth := auth
			if mode == "wrong remote pin" {
				clientAuth, err = newPeerRetirementAuthorizer("instance", map[string][]string{
					"old": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[3])},
				})
				require.NoError(t, err)
			}
			discovery, err := newPeerSuccessorDiscovery(sender, clientAuth, map[string]string{peer.URL: "next"})
			require.NoError(t, err)
			require.NoError(t, discovery.useCredentialSource(source))
			beforeLoads, beforeRequests, beforeReleases := source.loads, requests.Load(), releases.Load()
			sendErr := sender.send(context.Background(), condition)
			target, discoverErr := discovery.discover(context.Background())
			require.Equal(t, beforeLoads+2, source.loads, "both calls require fresh material and distinct handshakes")
			if mode == "initial" || mode == "rotated" || mode == "recovered" {
				require.NoError(t, sendErr)
				require.NoError(t, discoverErr)
				require.Equal(t, peer.URL, target)
				require.Equal(t, beforeRequests+2, requests.Load())
				require.Equal(t, beforeReleases+1, releases.Load())
				for i := 0; i < 2; i++ {
					select {
					case serial := <-serials:
						require.Equal(t, source.material.Certificate.Leaf.SerialNumber.String(), serial)
					case <-time.After(time.Second):
						t.Fatal("missing observed certificate")
					}
				}
			} else {
				require.ErrorIs(t, sendErr, errPeerRetirementUnconfirmed)
				require.ErrorIs(t, discoverErr, errPeerSuccessorUnavailable)
				require.Empty(t, target)
				require.Equal(t, beforeRequests, requests.Load())
				require.Equal(t, beforeReleases, releases.Load())
			}
		})
	}
	for _, endpoint := range []string{peer.URL + "/other", peer.URL + peerRetirementPath + "?extra=1", "http://unconfigured.invalid"} {
		request, err := http.NewRequest(http.MethodPost, endpoint, nil)
		require.NoError(t, err)
		before := source.loads
		response, err := sender.client.Do(request)
		require.Error(t, err)
		require.Nil(t, response)
		require.Equal(t, before, source.loads)
	}
}
