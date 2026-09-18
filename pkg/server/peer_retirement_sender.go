package server

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
)

const peerRetirementPath = "/internal/retirement/v1"

var errPeerRetirementUnconfirmed = errors.New("peer retirement release unconfirmed")

// A sender is constructed only from operator-owned peer configuration. It must
// be invoked only at the post-join irreversible retirement boundary, never as a
// way to evict a still-active holder. Neither failure nor success reactivates
// that term. Only the explicit opt-in server wires it into Campaign; the normal
// constructor and command-line deployment remain disabled.
type peerRetirementSender struct {
	instance, holder string
	endpoints        []string
	client           *http.Client
	budget           time.Duration
}

// onTermRetired is the adapter for the leader constructor's post-join callback.
// Missing snapshots and shutdown cancellation fall back without any request;
// an unconfirmed release likewise leaves the old term irreversibly retired.
// There is deliberately no goroutine, retry queue or reactivation callback.
func (s *peerRetirementSender) onTermRetired(ctx context.Context, condition election.OwnershipCondition, available bool) {
	if !available {
		return
	}
	_ = s.send(ctx, condition)
}

func newPeerRetirementSender(instance, holder string, endpoints []string, credentials *tls.Config, budget time.Duration) (*peerRetirementSender, error) {
	invalid := errors.New("invalid peer retirement sender configuration")
	validIdentity := func(s string) bool {
		if len(s) == 0 || len(s) > 4096 || strings.TrimSpace(s) != s {
			return false
		}
		for _, c := range s {
			if c < 0x20 || c == 0x7f {
				return false
			}
		}
		return true
	}
	// Construct a narrow TLS config rather than inheriting arbitrary verification,
	// dynamic certificate callbacks, session caches, proxies or insecure options.
	if !validIdentity(instance) || !validIdentity(holder) || len(endpoints) == 0 || len(endpoints) > 16 || budget <= 0 || budget > time.Minute || credentials == nil || credentials.InsecureSkipVerify || credentials.RootCAs == nil || len(credentials.Certificates) != 1 || credentials.GetClientCertificate != nil || credentials.VerifyConnection != nil || credentials.VerifyPeerCertificate != nil || credentials.ServerName != "" {
		return nil, invalid
	}
	minimum := max(credentials.MinVersion, uint16(tls.VersionTLS12))
	if credentials.MaxVersion != 0 && credentials.MaxVersion < minimum {
		return nil, invalid
	}
	certificate := credentials.Certificates[0]
	if len(certificate.Certificate) == 0 || certificate.PrivateKey == nil {
		return nil, invalid
	}
	// Detach certificate bytes and parsed leaf from caller-owned slices. The
	// private signing key must remain immutable for the lifetime of the sender.
	chain := make([][]byte, len(certificate.Certificate))
	for i, der := range certificate.Certificate {
		chain[i] = bytes.Clone(der)
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return nil, invalid
	}
	publicKey, err := retirementSigningPublicKey(certificate.PrivateKey)
	if err != nil || !bytes.Equal(publicKey, leaf.RawSubjectPublicKeyInfo) {
		return nil, invalid
	}
	cert := tls.Certificate{Certificate: chain, PrivateKey: certificate.PrivateKey, Leaf: leaf}
	targets := make([]string, 0, len(endpoints))
	seen := make(map[string]bool)
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
			return nil, invalid
		}
		u.Path = peerRetirementPath
		target := u.String()
		if seen[target] {
			return nil, invalid
		}
		seen[target] = true
		targets = append(targets, target)
	}
	tlsConfig := &tls.Config{MinVersion: minimum, MaxVersion: credentials.MaxVersion, RootCAs: credentials.RootCAs.Clone(), Certificates: []tls.Certificate{cert}}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig, DialContext: (&net.Dialer{Timeout: budget}).DialContext,
		TLSHandshakeTimeout: budget, ResponseHeaderTimeout: budget,
		MaxResponseHeaderBytes: 8192, DisableKeepAlives: true, ForceAttemptHTTP2: true,
		// Proxy remains nil: never send this control request through env proxies.
	}
	return &peerRetirementSender{instance: instance, holder: holder, endpoints: targets, budget: budget,
		client: &http.Client{Transport: transport, Timeout: budget, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// A certificate pin authenticates the public key, not arbitrary PrivateKey
// contents. Validate the configured signer before any server workers start.
// Some malformed standard keys (e.g. short Ed25519 keys) panic in Public;
// contain that configuration failure without reflecting key material.
func retirementSigningPublicKey(key any) (encoded []byte, err error) {
	invalid := errors.New("invalid peer retirement signing key")
	defer func() {
		if recover() != nil {
			encoded, err = nil, invalid
		}
	}()
	signer, ok := key.(crypto.Signer)
	if !ok || signer == nil {
		return nil, invalid
	}
	encoded, err = x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return nil, invalid
	}
	return encoded, nil
}

// send makes at most one attempt per configured peer under ONE shared deadline.
// It returns nil only for an explicit 204. Lost acknowledgements, old peers (404),
// conflicts and deadlines are all unconfirmed; normal lease election remains
// the fallback. Never log the serialized condition or transport errors/URLs.
func (s *peerRetirementSender) send(parent context.Context, condition election.OwnershipCondition) error {
	if s == nil || s.client == nil || parent == nil || parent.Err() != nil {
		return errPeerRetirementUnconfirmed
	}
	payload, err := condition.MarshalBinary()
	if err != nil {
		return errPeerRetirementUnconfirmed
	}
	if _, err := election.ParseOwnershipCondition(payload, s.holder); err != nil {
		return errPeerRetirementUnconfirmed
	}
	ctx, cancel := context.WithTimeout(parent, s.budget)
	defer cancel()
	for _, endpoint := range s.endpoints {
		if ctx.Err() != nil {
			break
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return errPeerRetirementUnconfirmed
		}
		r.Header.Set(retirementInstanceHeader, s.instance)
		r.Header.Set(retirementHolderHeader, s.holder)
		r.Header.Set("Content-Type", "application/json")
		response, err := s.client.Do(r)
		if err != nil {
			continue
		}
		// No response payload is part of this protocol. Closing rather than
		// buffering/draining prevents a malicious peer from streaming forever.
		_ = response.Body.Close()
		if response.StatusCode == http.StatusNoContent && ctx.Err() == nil {
			return nil
		}
	}
	return errPeerRetirementUnconfirmed
}
