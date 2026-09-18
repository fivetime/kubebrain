package server

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

// Each control request owns a fresh HTTP/1 transport and handshake. HTTP/2
// multiplexing must not silently reuse credentials across control requests.
// This does not change the protocol or lifetime of gRPC proxy connections.
type reloadedPeerHTTP struct {
	source  transportidentity.ClientCredentialSource
	auth    *peerRetirementAuthorizer
	local   string
	targets map[string]string
	budget  time.Duration
}

func (d *peerSuccessorDiscovery) useCredentialSource(source transportidentity.ClientCredentialSource) error {
	if d == nil || source == nil || d.sender == nil || d.auth == nil {
		return errPeerCredentialVerification
	}
	targets := make(map[string]string, len(d.targets)*2)
	for _, target := range d.targets {
		targets[target.endpoint+peerRetirementPath] = target.holder
		targets[target.endpoint+peerSuccessorPath] = target.holder
	}
	d.sender.client.Transport = &reloadedPeerHTTP{source: source, auth: d.auth, local: d.sender.holder, targets: targets, budget: d.sender.budget}
	return nil
}

func (t *reloadedPeerHTTP) RoundTrip(request *http.Request) (*http.Response, error) {
	reject := func() (*http.Response, error) {
		if request != nil && request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, errPeerCredentialVerification
	}
	if request == nil || request.URL == nil || request.Method != http.MethodPost || request.Context().Err() != nil {
		return reject()
	}
	holder, ok := t.targets[request.URL.String()]
	if !ok || request.Host != "" && request.Host != request.URL.Host {
		return reject()
	}
	// Re-parse the approved value instead of retaining the caller's URL pointer.
	target, err := url.Parse(request.URL.String())
	if err != nil || target.Scheme != "https" {
		return reject()
	}
	address := target.Host
	if target.Port() == "" {
		address = net.JoinHostPort(target.Hostname(), "443")
	}
	transport := &http.Transport{DisableKeepAlives: true, MaxResponseHeaderBytes: 8192,
		ResponseHeaderTimeout: t.budget, TLSHandshakeTimeout: t.budget}
	transport.DialTLSContext = func(_ context.Context, network, _ string) (net.Conn, error) {
		// net/http's dial context can outlive a request. This one-shot transport
		// instead binds both dialing and handshaking to the original request.
		ctx, cancel := context.WithTimeout(request.Context(), t.budget)
		defer cancel()
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, errPeerCredentialVerification
		}
		return handshakeReloadedPeer(ctx, raw, t.source, t.auth, t.local, holder, target.Hostname(), []string{"http/1.1"})
	}
	defer transport.CloseIdleConnections()
	return transport.RoundTrip(request)
}
