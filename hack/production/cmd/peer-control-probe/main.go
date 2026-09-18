// peer-control-probe checks opt-in control routing without submitting an
// ownership condition. It never requests a valid retirement or retries one.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type config struct {
	endpoint, serverName, serverPin, ca, cert, key string
	scope, sender, receiver                        string
}

func (c config) validate() error {
	u, err := url.Parse(c.endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return errors.New("endpoint must be an HTTPS origin without userinfo, path, query or fragment")
	}
	for _, value := range []string{c.scope, c.sender, c.receiver, c.serverName} {
		if value == "" || len(value) > 4000 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("explicit scope, sender, receiver and TLS server name required")
		}
	}
	if c.sender == c.receiver {
		return errors.New("probe requires different sender and receiver identities")
	}
	pin, err := hex.DecodeString(c.serverPin)
	if err != nil || len(pin) != sha256.Size {
		return errors.New("server pin must be SHA-256 SPKI hex")
	}
	return nil
}

func newClient(c config) (*http.Client, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	rootPEM, err := os.ReadFile(c.ca)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		return nil, errors.New("invalid CA bundle")
	}
	identity, err := tls.LoadX509KeyPair(c.cert, c.key)
	if err != nil {
		return nil, err
	}
	pin, _ := hex.DecodeString(c.serverPin)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Protocols: protocols, DisableKeepAlives: true, TLSHandshakeTimeout: 3 * time.Second,
		ResponseHeaderTimeout: 3 * time.Second, MaxResponseHeaderBytes: 16 << 10,
		TLSClientConfig: &tls.Config{
			RootCAs: roots, Certificates: []tls.Certificate{identity}, ServerName: c.serverName, MinVersion: tls.VersionTLS12,
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
					return errors.New("server chain not verified")
				}
				actual := sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo)
				if actual != [sha256.Size]byte(pin) {
					return errors.New("server SPKI mismatch")
				}
				return nil
			},
		},
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

func probe(ctx context.Context, client *http.Client, c config) error {
	if err := c.validate(); err != nil {
		return err
	}
	for _, path := range []string{"/internal/successor/v1", "/internal/retirement/v1"} {
		checks := []struct {
			scope, holder string
			status        int
		}{
			{c.scope + ":probe-wrong-scope", c.sender, http.StatusForbidden},
			{c.scope, c.receiver, http.StatusForbidden}, // sender key may not impersonate receiver
			{c.scope, c.sender, http.StatusBadRequest},
		}
		if path == "/internal/successor/v1" {
			checks[2].status = http.StatusNoContent
		}
		for i, check := range checks {
			// Intentionally no body or Content-Type: a valid OwnershipCondition
			// must never reach the conditional release callback.
			r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, nil)
			if err != nil {
				return err
			}
			r.Header.Set("X-Kubebrain-Retirement-Instance", check.scope)
			r.Header.Set("X-Kubebrain-Retirement-Holder", check.holder)
			response, err := client.Do(r)
			if err != nil {
				return fmt.Errorf("%s check %d: %w", path, i, err)
			}
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 1))
			response.Body.Close()
			if readErr != nil || len(body) != 0 || response.StatusCode != check.status || response.Header.Get("Cache-Control") != "no-store" {
				return fmt.Errorf("%s check %d: unexpected response status=%d", path, i, response.StatusCode)
			}
			if check.status == http.StatusNoContent {
				for name, expected := range map[string]string{"X-Kubebrain-Successor-Scope": c.scope, "X-Kubebrain-Successor-Holder": c.receiver} {
					if values := response.Header.Values(name); len(values) != 1 || values[0] != expected {
						return fmt.Errorf("unexpected %s", name)
					}
				}
			}
		}
	}
	return nil
}

func main() {
	var c config
	flag.StringVar(&c.endpoint, "endpoint", "", "peer HTTPS origin (may use loopback port-forward)")
	flag.StringVar(&c.serverName, "server-name", "", "expected certificate DNS name")
	flag.StringVar(&c.serverPin, "server-pin", "", "expected receiver SHA-256 SPKI hex")
	flag.StringVar(&c.ca, "cacert", "", "trusted CA bundle path")
	flag.StringVar(&c.cert, "cert", "", "sender member certificate path")
	flag.StringVar(&c.key, "key", "", "sender member private key path")
	flag.StringVar(&c.scope, "scope", "", "independently verified backend scope")
	flag.StringVar(&c.sender, "sender", "", "sender holder identity")
	flag.StringVar(&c.receiver, "receiver", "", "receiver holder identity")
	flag.Parse()
	client, err := newClient(c)
	if err == nil {
		defer client.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		err = probe(ctx, client, c)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("PEER_CONTROL_AUTH_AND_ROUTING_VERIFIED_NO_RETIREMENT_SUBMITTED")
}
