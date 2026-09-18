package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, auth tls.ClientAuthType, version uint16, handler http.Handler) config {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err = x509.ParseCertificate(caDER)
	require.NoError(t, err)
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0600))
	issue := func(name string, serial int64, usage x509.ExtKeyUsage) (tls.Certificate, string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			DNSNames: []string{name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		certPath, keyPath := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
		require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
		require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return pair, certPath, keyPath
	}
	server, _, _ := issue("info.test", 2, x509.ExtKeyUsageServerAuth)
	_, certPath, keyPath := issue("client", 3, x509.ExtKeyUsageClientAuth)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	s := httptest.NewUnstartedServer(handler)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{server}, ClientAuth: auth, ClientCAs: roots,
		MinVersion: version, MaxVersion: version}
	s.StartTLS()
	t.Cleanup(s.Close)
	pin := sha256.Sum256(s.Certificate().RawSubjectPublicKeyInfo)
	return config{endpoint: s.URL, serverName: "info.test", serverPin: hex.EncodeToString(pin[:]),
		ca: caPath, cert: certPath, key: keyPath, mode: "protected", stackOutput: filepath.Join(dir, "stack.txt")}
}

const sampleStack = "goroutine 1 [select]:\nexample.test()\n\t/example.go:42\n"

func handler(t *testing.T, mode string, calls *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.ProtoMajor != 1 {
			t.Error("expected read-only HTTP/1 request")
		}
		switch r.URL.RequestURI() {
		case "/ping", "/ready":
			io.WriteString(w, "ok")
		case stackPath:
			switch mode {
			case "disabled":
				http.NotFound(w, r)
			case "redirect":
				http.Redirect(w, r, "/should-not-follow", http.StatusFound)
			case "oversize":
				io.WriteString(w, sampleStack+strings.Repeat("x", stackLimit))
			case "truncated":
				io.WriteString(w, strings.TrimSuffix(sampleStack, "\n"))
			case "spoof-alert":
				w.Header().Set("Content-Encoding", "remote error: tls: certificate required")
				w.WriteHeader(404)
			default:
				io.WriteString(w, sampleStack)
			}
		default:
			t.Error("unexpected request path")
			w.WriteHeader(500)
		}
	})
}

func TestProtectedCaptureRequiresVerifiedMTLSAndPreservesPrivateEvidence(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			var calls atomic.Int32
			c := fixture(t, tls.RequireAndVerifyClientCert, version, handler(t, "protected", &calls))
			var result bytes.Buffer
			require.NoError(t, run(c, &result))
			require.EqualValues(t, 3, calls.Load(), "anonymous request must not reach HTTP")
			body, err := os.ReadFile(c.stackOutput)
			require.NoError(t, err)
			require.Equal(t, sampleStack, string(body))
			info, err := os.Stat(c.stackOutput)
			require.NoError(t, err)
			require.EqualValues(t, 0600, info.Mode().Perm())
			var summary map[string]any
			require.NoError(t, json.Unmarshal(result.Bytes(), &summary))
			require.Equal(t, false, summary["pod_identity_proven"])
			require.Equal(t, false, summary["fault_acceptance_proven"])
			require.Error(t, run(c, io.Discard), "must not overwrite evidence")
		})
	}
}

func TestDisabledProfileRequires404ForBothIdentities(t *testing.T) {
	var calls atomic.Int32
	c := fixture(t, tls.NoClientCert, tls.VersionTLS13, handler(t, "disabled", &calls))
	c.mode, c.stackOutput = "disabled", ""
	require.NoError(t, run(c, io.Discard))
	require.EqualValues(t, 4, calls.Load())
}

func TestProbeRejectsUnsafeOrIncompleteResponses(t *testing.T) {
	for _, scenario := range []string{"unprotected", "spoof-alert", "redirect", "oversize", "truncated", "wrong-pin", "wrong-name", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			auth := tls.RequireAndVerifyClientCert
			if scenario == "unprotected" || scenario == "spoof-alert" {
				auth = tls.NoClientCert
			}
			var calls atomic.Int32
			c := fixture(t, auth, tls.VersionTLS13, handler(t, scenario, &calls))
			if scenario == "wrong-pin" {
				c.serverPin = strings.Repeat("0", 64)
			}
			if scenario == "wrong-name" {
				c.serverName = "other.test"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			_, err := probe(ctx, c)
			require.Error(t, err)
			require.NoFileExists(t, c.stackOutput)
			require.LessOrEqual(t, calls.Load(), int32(3), "no redirects or retries")
		})
	}
}

func TestClientTransportAndOriginConstraints(t *testing.T) {
	var calls atomic.Int32
	c := fixture(t, tls.RequireAndVerifyClientCert, tls.VersionTLS13, handler(t, "protected", &calls))
	client, _, err := newClient(c, true)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	tr := client.Transport.(*http.Transport)
	require.Nil(t, tr.Proxy)
	require.True(t, tr.DisableKeepAlives)
	require.True(t, tr.DisableCompression)
	require.False(t, tr.TLSClientConfig.InsecureSkipVerify)
	for _, endpoint := range []string{"http://127.0.0.1:443", "https://example.com:443", "https://127.0.0.1", c.endpoint + "/", c.endpoint + "?", c.endpoint + "#fragment"} {
		bad := c
		bad.endpoint = endpoint
		require.Error(t, bad.validate())
	}
	path := filepath.Join(t.TempDir(), "symlink")
	require.NoError(t, os.Symlink(c.cert, path))
	require.Error(t, saveStack(path, []byte("must-not-overwrite")))
}
