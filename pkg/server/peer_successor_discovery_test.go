package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/stretchr/testify/require"
)

func TestPeerSuccessorDiscoveryAuthenticatedCandidate(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, mode := range []string{"ready", "follower", "wrong receiver pin", "wrong caller pin", "wrong scope", "bare release ack", "redirect", "wrong response holder"} {
			t.Run(fmt.Sprintf("h2=%v/%s", h2, mode), func(t *testing.T) {
				pool, certs := retirementTestCertificates(t)
				pins := map[string][]string{"old": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[1])}}
				auth, err := newPeerRetirementAuthorizer("scope", pins)
				require.NoError(t, err)
				var releaseCalls, probes atomic.Int32
				limits, err := newPeerRetirementHandler(auth, func(context.Context, election.OwnershipCondition) error {
					releaseCalls.Add(1)
					return nil
				}, time.Second, time.Second, 1, 10)
				require.NoError(t, err)
				handler := &peerSuccessorHandler{limits: limits, holder: "next", ready: func() bool { return mode != "follower" }}
				peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					probes.Add(1)
					if r.URL.Path != peerSuccessorPath {
						t.Error("wrong discovery path")
					}
					switch mode {
					case "bare release ack":
						w.WriteHeader(http.StatusNoContent)
					case "redirect":
						w.Header().Set("Location", "https://unconfigured.invalid")
						w.WriteHeader(http.StatusTemporaryRedirect)
					case "wrong response holder":
						w.Header().Set(successorScopeHeader, "scope")
						w.Header().Set(successorHolderHeader, "old")
						w.WriteHeader(http.StatusNoContent)
					default:
						handler.ServeHTTP(w, r)
					}
				}), pool, []tls.Certificate{certs[1]}, h2)
				scope := "scope"
				if mode == "wrong scope" {
					scope = "other"
				}
				credentials := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
				if mode == "wrong caller pin" {
					credentials.Certificates = []tls.Certificate{certs[3]}
				}
				sender, err := newPeerRetirementSender(scope, "old", []string{peer.URL}, credentials, time.Second)
				require.NoError(t, err)
				if mode == "wrong receiver pin" {
					pins["next"] = []string{retirementTestPin(certs[2])}
				}
				clientAuth, err := newPeerRetirementAuthorizer(scope, pins)
				require.NoError(t, err)
				mapping := map[string]string{peer.URL: "next"}
				discovery, err := newPeerSuccessorDiscovery(sender, clientAuth, mapping)
				require.NoError(t, err)
				mapping[peer.URL] = "old" // Caller mutation cannot redirect the probe.
				target, err := discovery.discover(context.Background())
				if mode == "ready" {
					require.NoError(t, err)
					require.Equal(t, peer.URL, target)
				} else {
					require.ErrorIs(t, err, errPeerSuccessorUnavailable)
					require.Empty(t, target)
				}
				require.Equal(t, int32(1), probes.Load(), "redirects must not be followed")
				require.Zero(t, releaseCalls.Load(), "discovery must never mutate ownership")
				_, err = discovery.discover(context.Background())
				require.ErrorIs(t, err, errPeerSuccessorUnavailable, "immediate retry must be rate limited")
				require.Equal(t, int32(1), probes.Load())
			})
		}
	}
}

func TestPeerSuccessorDiscoveryFixedMappingAndDeadline(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"old": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	entered := make(chan struct{}, 1)
	peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-r.Context().Done()
	}), pool, []tls.Certificate{certs[1]}, true)
	sender, err := newPeerRetirementSender("scope", "old", []string{peer.URL}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, 100*time.Millisecond)
	require.NoError(t, err)
	for _, mapping := range []map[string]string{nil, {peer.URL: "old"}, {peer.URL: "unknown"}, {"https://other.invalid": "next"}, {peer.URL: "next", "https://extra.invalid": "next"}} {
		_, err := newPeerSuccessorDiscovery(sender, auth, mapping)
		require.Error(t, err)
	}
	d, err := newPeerSuccessorDiscovery(sender, auth, map[string]string{peer.URL: "next"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = d.discover(ctx)
	require.ErrorIs(t, err, errPeerSuccessorUnavailable)
	start := time.Now()
	done := make(chan error, 1)
	go func() { _, err := d.discover(context.Background()); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe never arrived")
	}
	_, err = d.discover(context.Background())
	require.ErrorIs(t, err, errPeerSuccessorUnavailable, "only one flight is permitted")
	select {
	case err := <-done:
		require.ErrorIs(t, err, errPeerSuccessorUnavailable)
	case <-time.After(time.Second):
		t.Fatal("probe ignored deadline")
	}
	require.Less(t, time.Since(start), time.Second)

	// The deadline belongs to the whole search, not to each peer. Reserve a
	// share for the second peer without multiplying the total budget; its bare
	// 204 still cannot impersonate a valid discovery response.
	var secondCalls atomic.Int32
	second := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}), pool, []tls.Certificate{certs[1]}, true)
	multiSender, err := newPeerRetirementSender("scope", "old", []string{peer.URL, second.URL},
		&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, 300*time.Millisecond)
	require.NoError(t, err)
	multi, err := newPeerSuccessorDiscovery(multiSender, auth, map[string]string{peer.URL: "next", second.URL: "next"})
	require.NoError(t, err)
	start = time.Now()
	_, err = multi.discover(context.Background())
	require.ErrorIs(t, err, errPeerSuccessorUnavailable)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, int32(1), secondCalls.Load(), "each remaining candidate must receive a bounded opportunity")
}

func TestPeerSuccessorDiscoveryStalledFirstPeerDoesNotStarveReadyPeer(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"old": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[1])}, "stalled": {retirementTestPin(certs[2])}})
	require.NoError(t, err)
	stalled := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}), pool, []tls.Certificate{certs[2]}, true)
	limits, err := newPeerRetirementHandler(auth, func(context.Context, election.OwnershipCondition) error {
		t.Error("discovery must not release ownership")
		return nil
	}, time.Second, time.Second, 1, 10)
	require.NoError(t, err)
	ready := retirementHandlerServer(t, &peerSuccessorHandler{limits: limits, holder: "next", ready: func() bool { return true }}, pool, []tls.Certificate{certs[1]}, true)
	sender, err := newPeerRetirementSender("scope", "old", []string{stalled.URL, ready.URL},
		&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, 400*time.Millisecond)
	require.NoError(t, err)
	d, err := newPeerSuccessorDiscovery(sender, auth, map[string]string{stalled.URL: "stalled", ready.URL: "next"})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := d.discover(ctx)
	require.NoError(t, err, "a stalled first candidate must not starve a ready later candidate")
	require.Equal(t, ready.URL, endpoint)
}

func TestPeerSuccessorDiscoveryAllStalledKeepOneTotalDeadline(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"old": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	var calls atomic.Int32
	var urls []string
	mapping := map[string]string{}
	for i := 0; i < 4; i++ {
		peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			<-r.Context().Done()
		}), pool, []tls.Certificate{certs[1]}, true)
		urls = append(urls, peer.URL)
		mapping[peer.URL] = "next"
	}
	sender, err := newPeerRetirementSender("scope", "old", urls,
		&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, 200*time.Millisecond)
	require.NoError(t, err)
	d, err := newPeerSuccessorDiscovery(sender, auth, mapping)
	require.NoError(t, err)
	start := time.Now()
	endpoint, err := d.discover(context.Background())
	require.ErrorIs(t, err, errPeerSuccessorUnavailable)
	require.Empty(t, endpoint)
	require.Equal(t, int32(4), calls.Load())
	require.Less(t, time.Since(start), 600*time.Millisecond, "four peers must not each receive the full 200ms budget")
}
