package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func retirementHandlerFixture(t *testing.T) (*peerRetirementAuthorizer, *x509.CertPool, []tls.Certificate, []byte, election.OwnershipCondition) {
	t.Helper()
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("instance", map[string][]string{"old": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	lock := election.NewResourceLockManager(election.Config{Prefix: "/handler", Identity: "old", Timeout: time.Second}, kv).GetResourceLock()
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 1}
	require.NoError(t, lock.Create(context.Background(), record))
	condition, ok := lock.(election.OwnershipConditionProvider).OwnershipConditionFor(record)
	require.True(t, ok)
	payload, err := condition.MarshalBinary()
	require.NoError(t, err)
	return auth, pool, certs, payload, condition
}

func retirementHandlerServer(t *testing.T, h http.Handler, pool *x509.CertPool, certs []tls.Certificate, h2 bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = h2
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certs[0]}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func retirementHandlerRequest(t *testing.T, url string, body io.Reader) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, url, body)
	require.NoError(t, err)
	r.Header.Set(retirementInstanceHeader, "instance")
	r.Header.Set(retirementHolderHeader, "old")
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestPeerRetirementHandlerTLSAndOutcome(t *testing.T) {
	auth, pool, certs, payload, _ := retirementHandlerFixture(t)
	for _, h2 := range []bool{false, true} {
		for _, mode := range []string{"success", "unauthorized", "malformed", "empty condition", "deployment probe", "backend error", "canceled backend"} {
			t.Run(fmt.Sprintf("h2=%v/%s", h2, mode), func(t *testing.T) {
				var calls atomic.Int32
				h, err := newPeerRetirementHandler(auth, func(ctx context.Context, c election.OwnershipCondition) error {
					calls.Add(1)
					wire, err := c.MarshalBinary()
					if err != nil || !bytes.Equal(wire, payload) {
						return errors.New("wrong condition")
					}
					if _, ok := ctx.Deadline(); !ok {
						return errors.New("missing deadline")
					}
					if mode == "backend error" {
						return errors.New("secret backend failure")
					}
					if mode == "canceled backend" {
						<-ctx.Done()
						return nil
					}
					return nil
				}, time.Second, 50*time.Millisecond, 1, 10)
				require.NoError(t, err)
				srv := retirementHandlerServer(t, h, pool, certs, h2)
				cert := certs[0]
				if mode == "unauthorized" {
					cert = certs[1]
				}
				transport := &http.Transport{ForceAttemptHTTP2: h2, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{cert}}}
				t.Cleanup(transport.CloseIdleConnections)
				client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
				body := payload
				if mode == "malformed" {
					body = []byte(`{}`)
				}
				if mode == "empty condition" || mode == "deployment probe" {
					body = nil
				}
				request := retirementHandlerRequest(t, srv.URL, bytes.NewReader(body))
				if mode == "deployment probe" {
					request.Header.Del("Content-Type")
				}
				res, err := client.Do(request)
				require.NoError(t, err)
				defer res.Body.Close()
				want := http.StatusNoContent
				wantCalls := int32(1)
				switch mode {
				case "unauthorized":
					want, wantCalls = http.StatusForbidden, 0
				case "malformed", "empty condition", "deployment probe":
					want, wantCalls = http.StatusBadRequest, 0
				case "backend error", "canceled backend":
					want = http.StatusServiceUnavailable
				}
				require.Equal(t, want, res.StatusCode)
				require.Equal(t, wantCalls, calls.Load())
				if h2 {
					require.Equal(t, 2, res.ProtoMajor)
				} else {
					require.Equal(t, 1, res.ProtoMajor)
				}
				response, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				require.Empty(t, response, "no condition or backend error reflection")
			})
		}
	}
}

// Real socket, partial declared body, and no client body cancellation: only the
// handler's transport read deadline can make it return before our safety timeout.
func TestPeerRetirementHandlerStopsSlowBody(t *testing.T) {
	auth, pool, certs, _, _ := retirementHandlerFixture(t)
	var calls atomic.Int32
	h, err := newPeerRetirementHandler(auth, func(context.Context, election.OwnershipCondition) error { calls.Add(1); return nil }, 50*time.Millisecond, time.Second, 1, 10)
	require.NoError(t, err)
	done := make(chan struct{}, 1)
	srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r); done <- struct{}{} }), pool, certs, false)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", strings.TrimPrefix(srv.URL, "https://"), &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	_, err = fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: peer\r\nContent-Type: application/json\r\n%s: instance\r\n%s: old\r\nContent-Length: 100\r\n\r\n{", retirementInstanceHeader, retirementHolderHeader)
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stalled body retained handler beyond read deadline")
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Zero(t, calls.Load())
}

func TestPeerRetirementHandlerStopsHTTP2SlowBody(t *testing.T) {
	auth, pool, certs, _, _ := retirementHandlerFixture(t)
	var calls atomic.Int32
	h, err := newPeerRetirementHandler(auth, func(context.Context, election.OwnershipCondition) error { calls.Add(1); return nil }, 50*time.Millisecond, time.Second, 1, 10)
	require.NoError(t, err)
	done := make(chan struct{}, 1)
	srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r); done <- struct{}{} }), pool, certs, true)
	transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	r := retirementHandlerRequest(t, srv.URL, reader)
	// Keep the body open and incomplete until AFTER the handler returns.
	type result struct {
		response *http.Response
		err      error
	}
	results := make(chan result, 1)
	go func() { res, err := client.Do(r); results <- result{res, err} }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP/2 stalled body retained handler beyond read deadline")
	}
	select {
	case result := <-results:
		require.NoError(t, result.err)
		defer result.response.Body.Close()
		require.Equal(t, 2, result.response.ProtoMajor)
		require.Equal(t, http.StatusBadRequest, result.response.StatusCode)
	case <-time.After(time.Second):
		t.Fatal("HTTP/2 response did not finish")
	}
	require.Zero(t, calls.Load())
}

type retirementDeadlineWriter struct{ *httptest.ResponseRecorder }

func (*retirementDeadlineWriter) SetReadDeadline(time.Time) error  { return nil }
func (*retirementDeadlineWriter) SetWriteDeadline(time.Time) error { return nil }

func TestPeerRetirementHandlerAdmission(t *testing.T) {
	auth, pool, certs, payload, _ := retirementHandlerFixture(t)
	chains, err := certs[0].Leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	require.NoError(t, err)
	request := func() *http.Request {
		r := retirementHandlerRequest(t, "https://peer/", bytes.NewReader(payload))
		r.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true, PeerCertificates: []*x509.Certificate{certs[0].Leaf}, VerifiedChains: chains}
		return r
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h, err := newPeerRetirementHandler(auth, func(ctx context.Context, _ election.OwnershipCondition) error {
		calls.Add(1)
		<-ctx.Done()
		close(entered)
		<-release
		return nil
	}, time.Second, 10*time.Millisecond, 1, 0.001)
	require.NoError(t, err)
	// Hidden deadline support must refuse before reading or calling storage.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request())
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	// Invalid identities cannot spend the sole rate token.
	bad := request()
	bad.TLS = nil
	denied := &retirementDeadlineWriter{httptest.NewRecorder()}
	h.ServeHTTP(denied, bad)
	require.Equal(t, http.StatusForbidden, denied.Code)
	done := make(chan struct{})
	first := &retirementDeadlineWriter{httptest.NewRecorder()}
	r := request()
	go func() { h.ServeHTTP(first, r); close(done) }()
	// Always unblock the callback, including assertion failures.
	defer func() {
		close(release)
		<-done
		require.Equal(t, http.StatusServiceUnavailable, first.Code, "late nil result cannot acknowledge success after timeout")
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first callback did not receive cancellation")
	}
	second := &retirementDeadlineWriter{httptest.NewRecorder()}
	h.ServeHTTP(second, request())
	require.Equal(t, http.StatusTooManyRequests, second.Code)
	require.Len(t, h.slots, 1, "noncooperative callback must retain its slot after cancellation")
	require.Equal(t, int32(1), calls.Load())
	// Independent handler isolates the rate limit from occupied-slot rejection.
	limited, err := newPeerRetirementHandler(auth, func(context.Context, election.OwnershipCondition) error { return nil }, time.Second, time.Second, 1, 0.001)
	require.NoError(t, err)
	for _, status := range []int{http.StatusNoContent, http.StatusTooManyRequests} {
		w := &retirementDeadlineWriter{httptest.NewRecorder()}
		limited.ServeHTTP(w, request())
		require.Equal(t, status, w.Code)
	}
}

func TestPeerRetirementHandlerRejectsInvalidBudgets(t *testing.T) {
	auth, _, _, _, _ := retirementHandlerFixture(t)
	callback := func(context.Context, election.OwnershipCondition) error { return nil }
	for _, rate := range []float64{0, -1, math.NaN(), math.Inf(1), 1001} {
		_, err := newPeerRetirementHandler(auth, callback, time.Second, time.Second, 1, rate)
		require.Error(t, err)
	}
	for _, budget := range []time.Duration{0, -1, time.Minute + 1} {
		_, err := newPeerRetirementHandler(auth, callback, budget, time.Second, 1, 1)
		require.Error(t, err)
		_, err = newPeerRetirementHandler(auth, callback, time.Second, budget, 1, 1)
		require.Error(t, err)
	}
	for _, n := range []int{0, -1, 65} {
		_, err := newPeerRetirementHandler(auth, callback, time.Second, time.Second, n, 1)
		require.Error(t, err)
	}
}
