// info-diagnostic-probe verifies an info listener before bounded stack capture.
// It is read-only; callers must independently bind the tunnel to a live Pod.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

const stackLimit = 8 << 20
const stackPath = "/debug/pprof/goroutine?debug=2"

type config struct {
	endpoint, anonymousEndpoint, serverName, serverPin, ca, cert, key, mode, stackOutput string
}

func validateOrigin(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.Port() == "" {
		return errors.New("endpoint must be an HTTPS loopback origin with explicit port")
	}
	if ip := net.ParseIP(u.Hostname()); ip == nil || !ip.IsLoopback() {
		return errors.New("endpoint must use a literal loopback address bound by the caller's tunnel")
	}
	return nil
}

func (c config) validate() error {
	if err := validateOrigin(c.endpoint); err != nil {
		return err
	}
	if c.mode == "protected" {
		if err := validateOrigin(c.anonymousEndpoint); err != nil {
			return fmt.Errorf("anonymous endpoint: %w", err)
		}
		if c.endpoint == c.anonymousEndpoint {
			return errors.New("protected mode requires an independent anonymous tunnel")
		}
	} else if c.anonymousEndpoint != "" {
		return errors.New("anonymous-endpoint is only used in protected mode")
	}
	if c.serverName == "" || strings.ContainsAny(c.serverName, " /\r\n\x00") {
		return errors.New("explicit TLS server name required")
	}
	if pin, err := hex.DecodeString(c.serverPin); err != nil || len(pin) != sha256.Size {
		return errors.New("server pin must be SHA-256 SPKI hex")
	}
	if c.mode != "protected" && c.mode != "disabled" {
		return errors.New("mode must be protected or disabled")
	}
	if (c.mode == "protected") != (c.stackOutput != "") {
		return errors.New("stack-output required only in protected mode")
	}
	return nil
}

type handshakeEvidence struct {
	verified, certificateRequested atomic.Bool
}

func newClient(c config, authenticated bool) (*http.Client, *handshakeEvidence, error) {
	if err := c.validate(); err != nil {
		return nil, nil, err
	}
	ca, err := os.ReadFile(c.ca)
	if err != nil {
		return nil, nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("invalid CA bundle")
	}
	var identities []tls.Certificate
	if authenticated {
		identity, err := tls.LoadX509KeyPair(c.cert, c.key)
		if err != nil {
			return nil, nil, err
		}
		identities = []tls.Certificate{identity}
	}
	pin, _ := hex.DecodeString(c.serverPin)
	evidence := new(handshakeEvidence)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	tr := &http.Transport{
		Protocols: protocols, DisableKeepAlives: true, DisableCompression: true,
		TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 3 * time.Second,
		MaxResponseHeaderBytes: 16 << 10,
		TLSClientConfig: &tls.Config{
			RootCAs: roots, Certificates: identities, ServerName: c.serverName, MinVersion: tls.VersionTLS12,
			VerifyConnection: func(s tls.ConnectionState) error {
				if len(s.VerifiedChains) == 0 || len(s.PeerCertificates) == 0 {
					return errors.New("unverified server chain")
				}
				actual := sha256.Sum256(s.PeerCertificates[0].RawSubjectPublicKeyInfo)
				if actual != [sha256.Size]byte(pin) {
					return errors.New("server SPKI mismatch")
				}
				evidence.verified.Store(true)
				return nil
			},
		},
	}
	if !authenticated {
		tr.TLSClientConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			evidence.certificateRequested.Store(true)
			return &tls.Certificate{}, nil
		}
	}
	return &http.Client{Transport: tr, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, evidence, nil
}

func request(ctx context.Context, client *http.Client, endpoint, path string, status, limit int) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != status || response.Header.Get("Content-Encoding") != "" {
		return nil, fmt.Errorf("unexpected HTTP response: status=%d encoding=%q", response.StatusCode, response.Header.Get("Content-Encoding"))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > limit {
		return nil, errors.New("response exceeds byte limit")
	}
	return body, nil
}

func probe(ctx context.Context, c config) ([]byte, error) {
	good, _, err := newClient(c, true)
	if err != nil {
		return nil, err
	}
	defer good.CloseIdleConnections()
	for _, path := range []string{"/ping", "/ready"} {
		if _, err := request(ctx, good, c.endpoint, path, http.StatusOK, 4096); err != nil {
			return nil, fmt.Errorf("authenticated %s: %w", path, err)
		}
	}
	anonymous, evidence, err := newClient(c, false)
	if err != nil {
		return nil, err
	}
	defer anonymous.CloseIdleConnections()
	anonymousEndpoint := c.endpoint
	if c.mode == "protected" {
		anonymousEndpoint = c.anonymousEndpoint
	}
	_, err = request(ctx, anonymous, anonymousEndpoint, stackPath, http.StatusNotFound, 4096)
	if c.mode == "protected" {
		// Connection refusal, timeout, EOF and local trust failures are not
		// authentication proof. Require a verified server, its certificate
		// request and its TLS alert after we supplied an empty certificate.
		// TLS 1.2 reports handshake_failure instead of certificate_required.
		var remote *net.OpError
		if !evidence.verified.Load() || !evidence.certificateRequested.Load() || !errors.As(err, &remote) || remote.Op != "remote error" ||
			(remote.Err.Error() != "tls: certificate required" && remote.Err.Error() != "tls: bad certificate" && remote.Err.Error() != "tls: handshake failure") {
			return nil, fmt.Errorf("missing proven anonymous TLS rejection: %v", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("anonymous disabled profile: %w", err)
	}
	if c.mode == "disabled" {
		_, err := request(ctx, good, c.endpoint, stackPath, http.StatusNotFound, 4096)
		return nil, err
	}
	body, err := request(ctx, good, c.endpoint, stackPath, http.StatusOK, stackLimit)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(body, []byte("goroutine ")) || !bytes.HasSuffix(body, []byte("\n")) {
		return nil, errors.New("unexpected goroutine debug=2 body")
	}
	return body, nil
}

func saveStack(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(body)
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}

func run(c config, output io.Writer) error {
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	body, err := probe(ctx, c)
	if err != nil {
		return err
	}
	if c.mode == "protected" {
		if err := saveStack(c.stackOutput, body); err != nil {
			return err
		}
	}
	sum := sha256.Sum256(body)
	return json.NewEncoder(output).Encode(map[string]any{
		"scope": "info_listener_transport_only", "mode": c.mode,
		"started": started, "completed": time.Now().UTC(),
		"stack_bytes": len(body), "stack_sha256": hex.EncodeToString(sum[:]),
		"pod_identity_proven": false, "fault_acceptance_proven": false,
	})
}

func main() {
	var c config
	flag.StringVar(&c.endpoint, "endpoint", "", "HTTPS literal loopback origin")
	flag.StringVar(&c.anonymousEndpoint, "anonymous-endpoint", "", "independent tunnel to the same Pod for protected-mode negative TLS check")
	flag.StringVar(&c.serverName, "server-name", "", "verified info TLS DNS name")
	flag.StringVar(&c.serverPin, "server-spki-sha256", "", "operator-audited info server SPKI")
	flag.StringVar(&c.ca, "cacert", "", "server CA PEM file")
	flag.StringVar(&c.cert, "cert", "", "client certificate PEM file")
	flag.StringVar(&c.key, "key", "", "client private key PEM file")
	flag.StringVar(&c.mode, "mode", "", "protected or disabled")
	flag.StringVar(&c.stackOutput, "stack-output", "", "new private stack file (protected only)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
