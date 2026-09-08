package tikv

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise the actual remote client-go TLS policy through KubeBrain's wrapper,
// not just flag binding or direct invocation of its verification callback.
// This is a loopback transport contract, not a real PD/TiKV integration test.
func TestTiKVSecurityTLSHandshake(t *testing.T) {
	trusted := newBackendTestCA(t)
	untrusted := newBackendTestCA(t)
	for _, version := range []struct {
		name string
		id   uint16
	}{{"TLS12", tls.VersionTLS12}, {"TLS13", tls.VersionTLS13}} {
		t.Run(version.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, serverCN, serverDNS         string
				allowedCN                         []string
				badServerCA, badClientCA, expired bool
				noClientCert                      bool
				wantError                         string
			}{
				{name: "pd allowed", serverCN: "pd", allowedCN: []string{"tikv", "pd"}},
				{name: "tikv allowed", serverCN: "tikv", allowedCN: []string{"tikv", "pd"}},
				{name: "wrong CN", serverCN: "other", allowedCN: []string{"pd"}, wantError: "common name"},
				{name: "CN is case sensitive", serverCN: "PD", allowedCN: []string{"pd"}, wantError: "common name"},
				{name: "CN does not accept substrings", serverCN: "pd-attacker", allowedCN: []string{"pd"}, wantError: "common name"},
				{name: "no additional CN restriction", serverCN: "other"},
				{name: "CN does not replace SAN", serverCN: "pd", serverDNS: "other.test", allowedCN: []string{"pd"}, wantError: "certificate is valid for"},
				{name: "CN does not replace CA", serverCN: "pd", allowedCN: []string{"pd"}, badServerCA: true, wantError: "unknown authority"},
				{name: "CN does not replace expiry", serverCN: "pd", allowedCN: []string{"pd"}, expired: true, wantError: "expired"},
				{name: "untrusted client", serverCN: "pd", allowedCN: []string{"pd"}, badClientCA: true, wantError: "certificate"},
				{name: "missing client certificate", serverCN: "pd", allowedCN: []string{"pd"}, noClientCert: true, wantError: "certificate"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					serverCA, clientCA := trusted, trusted
					if tc.badServerCA {
						serverCA = untrusted
					}
					if tc.badClientCA {
						clientCA = untrusted
					}
					if tc.serverDNS == "" {
						tc.serverDNS = "backend.test"
					}
					clientCert, clientKey := clientCA.issue(t, "kubebrain-client", "", x509.ExtKeyUsageClientAuth, false)
					serverCert, serverKey := serverCA.issue(t, tc.serverCN, tc.serverDNS, x509.ExtKeyUsageServerAuth, tc.expired)
					serverPair, err := tls.X509KeyPair(serverCert, serverKey)
					require.NoError(t, err)
					security := backendTestSecurity(t, trusted.pem, clientCert, clientKey, tc.allowedCN)
					if tc.noClientCert {
						security.CertPath, security.KeyPath = "", ""
						if version.id == tls.VersionTLS12 {
							// TLS 1.2 has no certificate_required alert.
							tc.wantError = "handshake failure"
						}
					}
					clientTLS, err := security.TLSConfig()
					require.NoError(t, err)
					require.NotNil(t, clientTLS)
					require.False(t, clientTLS.InsecureSkipVerify)
					clientTLS.ServerName = "backend.test"
					clientTLS.MinVersion, clientTLS.MaxVersion = version.id, version.id
					server := newBackendTestServer(t, trusted.pem, serverPair, version.id)
					transport := &http.Transport{TLSClientConfig: clientTLS}
					t.Cleanup(transport.CloseIdleConnections)
					client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
					response, err := client.Get(server.URL)
					if response != nil {
						t.Cleanup(func() { _ = response.Body.Close() })
					}
					if tc.wantError != "" {
						require.ErrorContains(t, err, tc.wantError)
						return
					}
					require.NoError(t, err)
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, response.StatusCode)
					require.Equal(t, "kubebrain-client", string(body), "the server must authenticate the configured client identity")
				})
			}
		})
	}
}

func TestTiKVSecurityTLSReloadsClientCertificate(t *testing.T) {
	ca := newBackendTestCA(t)
	serverCert, serverKey := ca.issue(t, "pd", "backend.test", x509.ExtKeyUsageServerAuth, false)
	serverPair, err := tls.X509KeyPair(serverCert, serverKey)
	require.NoError(t, err)
	server := newBackendTestServer(t, ca.pem, serverPair, tls.VersionTLS13)
	clientCert, clientKey := ca.issue(t, "client-before", "", x509.ExtKeyUsageClientAuth, false)
	security := backendTestSecurity(t, ca.pem, clientCert, clientKey, []string{"pd"})
	clientTLS, err := security.TLSConfig()
	require.NoError(t, err)
	clientTLS.ServerName = "backend.test"
	// Force a new connection for every request: rotation applies to subsequent
	// handshakes, not retroactively to an existing authenticated connection.
	transport := &http.Transport{TLSClientConfig: clientTLS, DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	for _, identity := range []string{"client-before", "client-after"} {
		cert, key := ca.issue(t, identity, "", x509.ExtKeyUsageClientAuth, false)
		require.NoError(t, os.WriteFile(security.CertPath, cert, 0600))
		require.NoError(t, os.WriteFile(security.KeyPath, key, 0600))
		response, err := client.Get(server.URL)
		require.NoError(t, err)
		body, readErr := io.ReadAll(response.Body)
		require.NoError(t, response.Body.Close())
		require.NoError(t, readErr)
		require.Equal(t, identity, string(body))
	}
	// A broken rotation must not silently keep using the old cached identity.
	require.NoError(t, os.WriteFile(security.CertPath, []byte("invalid certificate"), 0600))
	_, err = client.Get(server.URL)
	require.ErrorContains(t, err, "could not load client key pair")
}

type backendTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newBackendTestCA(t *testing.T) backendTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "backend-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return backendTestCA{cert: parsed, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca backendTestCA) issue(t *testing.T, cn, dns string, usage x509.ExtKeyUsage, expired bool) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	if dns != "" {
		cert.DNSNames = []string{dns}
	}
	if expired {
		cert.NotAfter = time.Now().Add(-time.Minute)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	privateDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateDER})
}

func backendTestSecurity(t *testing.T, ca, cert, key []byte, allowedCN []string) Security {
	t.Helper()
	dir := t.TempDir()
	security := Security{CAPath: filepath.Join(dir, "ca.pem"), CertPath: filepath.Join(dir, "client.pem"), KeyPath: filepath.Join(dir, "client.key"), VerifyCN: allowedCN}
	for path, contents := range map[string][]byte{security.CAPath: ca, security.CertPath: cert, security.KeyPath: key} {
		require.NoError(t, os.WriteFile(path, contents, 0600))
	}
	return security
}

func newBackendTestServer(t *testing.T, ca []byte, certificate tls.Certificate, version uint16) *httptest.Server {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, r.TLS.PeerCertificates[0].Subject.CommonName)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: version, MaxVersion: version}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}
