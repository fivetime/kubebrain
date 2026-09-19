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

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const stackLimit = 8 << 20
const stackPath = "/debug/pprof/goroutine?debug=2"

type config struct {
	endpoint, anonymousEndpoint, serverName, serverPin, ca, cert, key, mode, stackOutput string
	metricsOutput                                                                        string
}

func (c config) protectedCapture() bool { return c.capturesStack() || c.mode == "protected-metrics" }

func (c config) capturesStack() bool {
	return c.mode == "protected" || c.mode == "protected-stack"
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
	if c.protectedCapture() {
		if err := validateOrigin(c.anonymousEndpoint); err != nil {
			return fmt.Errorf("anonymous endpoint: %w", err)
		}
		if c.endpoint == c.anonymousEndpoint {
			return errors.New("protected mode requires an independent anonymous tunnel")
		}
	} else if c.anonymousEndpoint != "" {
		return errors.New("anonymous-endpoint is only used in protected capture modes")
	}
	if c.serverName == "" || strings.ContainsAny(c.serverName, " /\r\n\x00") {
		return errors.New("explicit TLS server name required")
	}
	if pin, err := hex.DecodeString(c.serverPin); err != nil || len(pin) != sha256.Size {
		return errors.New("server pin must be SHA-256 SPKI hex")
	}
	if !c.protectedCapture() && c.mode != "disabled" {
		return errors.New("mode must be protected, protected-stack, protected-metrics or disabled")
	}
	if c.capturesStack() != (c.stackOutput != "") {
		return errors.New("stack-output required only in protected capture modes")
	}
	if (c.mode == "protected-metrics") != (c.metricsOutput != "") {
		return errors.New("metrics-output required only in protected-metrics mode")
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
	return probeWithDiagnostics(ctx, c, io.Discard)
}

func probeWithDiagnostics(ctx context.Context, c config, diagnostics io.Writer) ([]byte, error) {
	origin := time.Now()
	// Fixed phase names only: never log credentials, URLs, bodies or error text.
	// Request errors include the expected anonymous TLS denial, not just failures.
	trace := json.NewEncoder(diagnostics)
	req := func(phase string, client *http.Client, endpoint, path string, status, limit int) ([]byte, error) {
		emit := func(event, outcome string) error {
			return trace.Encode(map[string]any{"scope": "info_probe_request_timing", "phase": phase,
				"event": event, "outcome": outcome, "elapsed_ns": time.Since(origin).Nanoseconds()})
		}
		if err := emit("start", "pending"); err != nil {
			return nil, errors.New("cannot write request timing start")
		}
		body, err := request(ctx, client, endpoint, path, status, limit)
		outcome := "response_validated"
		if err != nil {
			outcome = "request_error"
		}
		if traceErr := emit("end", outcome); traceErr != nil {
			return nil, errors.New("cannot write request timing end")
		}
		return body, err
	}
	good, _, err := newClient(c, true)
	if err != nil {
		return nil, err
	}
	defer good.CloseIdleConnections()
	paths := []string{"/ping", "/ready"}
	if c.mode == "protected-stack" || c.mode == "protected-metrics" {
		// The caller intentionally isolates backend access during fault tests.
		// Capture listener evidence without asserting application readiness.
		paths = []string{"/ping"}
	}
	for _, path := range paths {
		if _, err := req("authenticated"+path, good, c.endpoint, path, http.StatusOK, 4096); err != nil {
			return nil, fmt.Errorf("authenticated %s: %w", path, err)
		}
	}
	anonymous, evidence, err := newClient(c, false)
	if err != nil {
		return nil, err
	}
	defer anonymous.CloseIdleConnections()
	anonymousEndpoint := c.endpoint
	if c.protectedCapture() {
		anonymousEndpoint = c.anonymousEndpoint
	}
	path := stackPath
	if c.mode == "protected-metrics" {
		path = "/metrics"
	}
	_, err = req("anonymous-profile", anonymous, anonymousEndpoint, path, http.StatusNotFound, 4096)
	if c.protectedCapture() {
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
		_, err := req("authenticated-profile", good, c.endpoint, stackPath, http.StatusNotFound, 4096)
		return nil, err
	}
	if c.mode == "protected-metrics" {
		body, err := req("authenticated-metrics", good, c.endpoint, "/metrics", http.StatusOK, stackLimit)
		if err != nil {
			return nil, err
		}
		// Validate exposition syntax, not target metric presence/types, process
		// identity, reset-free deltas or the meaning of a reported outcome.
		if len(body) == 0 || !bytes.HasSuffix(body, []byte("\n")) {
			return nil, errors.New("empty or incomplete metrics body")
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, parseErr := parser.TextToMetricFamilies(bytes.NewReader(body))
		hasSamples := false
		for _, family := range families {
			hasSamples = hasSamples || len(family.Metric) > 0
		}
		if parseErr != nil || !hasSamples {
			// Parser errors may embed metric names/values; never echo the body.
			return nil, errors.New("invalid or empty Prometheus text exposition")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return body, nil
	}
	body, err := req("authenticated-profile", good, c.endpoint, stackPath, http.StatusOK, stackLimit)
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
	return runWithDiagnostics(c, output, io.Discard)
}

func runWithDiagnostics(c config, output, diagnostics io.Writer) error {
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	body, err := probeWithDiagnostics(ctx, c, diagnostics)
	if err != nil {
		return err
	}
	if c.capturesStack() {
		if err := saveStack(c.stackOutput, body); err != nil {
			return err
		}
	}
	if c.mode == "protected-metrics" {
		if err := saveStack(c.metricsOutput, body); err != nil {
			return err
		}
	}
	sum := sha256.Sum256(body)
	result := map[string]any{
		"scope": "info_listener_transport_only", "mode": c.mode,
		"started": started, "completed": time.Now().UTC(),
		"stack_bytes": len(body), "stack_sha256": hex.EncodeToString(sum[:]),
		"pod_identity_proven": false, "fault_acceptance_proven": false,
		"readiness_checked": c.mode != "protected-stack" && c.mode != "protected-metrics",
	}
	if c.mode == "protected-metrics" {
		delete(result, "stack_bytes")
		delete(result, "stack_sha256")
		result["metrics_bytes"], result["metrics_sha256"] = len(body), hex.EncodeToString(sum[:])
		result["metric_semantics_proven"] = false
		result["metrics_text_syntax_validated"] = true
	}
	return json.NewEncoder(output).Encode(result)
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
	flag.StringVar(&c.mode, "mode", "", "protected, protected-stack, protected-metrics (fault-time captures without readiness), or disabled")
	flag.StringVar(&c.metricsOutput, "metrics-output", "", "new private raw metrics file (protected-metrics only)")
	flag.StringVar(&c.stackOutput, "stack-output", "", "new private stack file (protected capture modes only)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if err := runWithDiagnostics(c, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
