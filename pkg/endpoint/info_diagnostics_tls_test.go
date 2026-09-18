package endpoint

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
)

// Exercise the actual info listener and TLS wrapper, not merely its ServeMux.
// No backend is involved; this does not prove live-cluster readiness or storage.
func TestInfoDiagnosticListenerRequiresMTLSAndExplicitPprof(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("pprof=%v", enabled), func(t *testing.T) {
			ca := newRotationCA(t)
			dir := t.TempDir()
			serverCert, serverKey := writeRotationCertificate(t, dir, "server", ca, 1, "info.test")
			caFile := filepath.Join(dir, "ca.pem")
			writeRotationCA(t, caFile, ca)
			sc := &SecurityConfig{CertFile: serverCert, KeyFile: serverKey, CA: caFile, ClientAuth: true, ServerName: "info.test"}
			require.NoError(t, sc.validate())
			port := freeTCPPort(t)
			endpoint := &Endpoint{metrics: &interceptorOrderMetrics{}, server: rejectingInterceptorServer{}, tlsIdentities: &transportidentity.Registry{}, config: &Config{InfoPort: port, InfoSecurityConfig: sc, EnablePprof: enabled}}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- endpoint.runMetricsServer(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Error("info listener did not stop")
				}
			}()
			identity := func(authority rotationCA, name string) tls.Certificate {
				cert, key := writeRotationCertificate(t, t.TempDir(), name, authority, 2, "client.test")
				pair, err := tls.LoadX509KeyPair(cert, key)
				require.NoError(t, err)
				return pair
			}
			valid := identity(ca, "valid")
			untrusted := identity(newRotationCA(t), "untrusted")
			roots := x509.NewCertPool()
			roots.AddCert(ca.cert)
			client := func(cert *tls.Certificate, name string) *http.Client {
				config := &tls.Config{RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS12}
				if cert != nil {
					config.Certificates = []tls.Certificate{*cert}
				}
				protocols := new(http.Protocols)
				protocols.SetHTTP1(true)
				tr := &http.Transport{TLSClientConfig: config, Protocols: protocols, DisableKeepAlives: true}
				t.Cleanup(tr.CloseIdleConnections)
				return &http.Client{Transport: tr, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			}
			good := client(&valid, "info.test")
			url := fmt.Sprintf("https://127.0.0.1:%d/debug/pprof/goroutine?debug=2", port)
			expected := http.StatusNotFound
			if enabled {
				expected = http.StatusOK
			}
			check := func() bool {
				r, err := good.Get(url)
				if err != nil {
					return false
				}
				defer r.Body.Close()
				data, err := io.ReadAll(io.LimitReader(r.Body, 8388608))
				if err != nil || r.StatusCode != expected {
					return false
				}
				if enabled {
					return len(data) < 8388608 && strings.HasPrefix(string(data), "goroutine ") && strings.HasSuffix(string(data), "\n")
				}
				return true
			}
			require.Eventually(t, check, 5*time.Second, 20*time.Millisecond)
			for _, test := range []struct {
				name string
				c    *http.Client
				url  string
			}{
				{"no certificate", client(nil, "info.test"), url},
				{"untrusted certificate", client(&untrusted, "info.test"), url},
				{"wrong server name", client(&valid, "other.test"), url},
				{"plaintext", client(nil, "info.test"), strings.Replace(url, "https://", "http://", 1)},
			} {
				t.Run(test.name, func(t *testing.T) {
					response, err := test.c.Get(test.url)
					if response != nil {
						response.Body.Close()
					}
					require.Error(t, err, "unauthenticated request must fail before HTTP profile data")
					require.Nil(t, response)
				})
			}
			require.True(t, check(), "rejected clients must not poison later valid requests")
		})
	}
}
