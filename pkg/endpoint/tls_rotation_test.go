package endpoint

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

type rotationCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
	pem  []byte
}

func newRotationCA(t *testing.T) rotationCA {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rotation-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return rotationCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func writeRotationCertificate(t *testing.T, dir, name string, ca rotationCA, serial int64, dnsName string) (string, string) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath
}

func handshakeTLS(t *testing.T, serverConfig, clientConfig *tls.Config) (tls.ConnectionState, tls.ConnectionState) {
	t.Helper()
	serverState, clientState, serverErr, clientErr := tryHandshakeTLS(t, serverConfig, clientConfig)
	require.NoError(t, serverErr)
	require.NoError(t, clientErr)
	return serverState, clientState
}

func tryHandshakeTLS(t *testing.T, serverConfig, clientConfig *tls.Config) (tls.ConnectionState, tls.ConnectionState, error, error) {
	t.Helper()
	serverRaw, clientRaw := net.Pipe()
	require.NoError(t, serverRaw.SetDeadline(time.Now().Add(2*time.Second)))
	require.NoError(t, clientRaw.SetDeadline(time.Now().Add(2*time.Second)))
	server := tls.Server(serverRaw, serverConfig)
	client := tls.Client(clientRaw, clientConfig)
	serverErr := make(chan error, 1)
	clientErr := make(chan error, 1)
	go func() { serverErr <- server.Handshake() }()
	go func() { clientErr <- client.Handshake() }()
	serverHandshakeErr, clientHandshakeErr := <-serverErr, <-clientErr
	serverState, clientState := server.ConnectionState(), client.ConnectionState()
	require.NoError(t, clientRaw.Close())
	require.NoError(t, serverRaw.Close())
	return serverState, clientState, serverHandshakeErr, clientHandshakeErr
}

func writeRotationCA(t *testing.T, path string, cas ...rotationCA) {
	t.Helper()
	var bundle []byte
	for _, ca := range cas {
		bundle = append(bundle, ca.pem...)
	}
	require.NoError(t, os.WriteFile(path, bundle, 0o600))
}

func writeRotationCRL(t *testing.T, path string, ca rotationCA, serials ...*big.Int) {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(serials))
	for _, serial := range serials {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber: new(big.Int).Set(serial), RevocationTime: time.Now(),
		})
	}
	contents, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(time.Now().UnixNano()), ThisUpdate: time.Now().Add(-time.Minute),
		NextUpdate: time.Now().Add(time.Hour), RevokedCertificateEntries: entries,
	}, ca.cert, ca.key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, contents, 0o600))
}

func leafCertificate(t *testing.T, certPath, keyPath string) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	require.NoError(t, err)
	return certificate, leaf
}

func TestServerCertificateReloadedForEveryHandshake(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	certPath, keyPath := writeRotationCertificate(t, dir, "server-one", ca, 101, "rotation.test")
	config := &SecurityConfig{CertFile: certPath, KeyFile: keyPath}
	require.NoError(t, config.validate())
	require.Empty(t, config.getServerTLSConfig().Certificates)

	// No ServerName intentionally exercises the IP-only/no-SNI case that etcd's
	// empty Certificates slice is designed to reload correctly.
	clientConfig := &tls.Config{InsecureSkipVerify: true} //nolint:gosec -- callback behavior test
	_, first := handshakeTLS(t, config.getServerTLSConfig(), clientConfig)
	require.Equal(t, int64(101), first.PeerCertificates[0].SerialNumber.Int64())

	writeRotationCertificate(t, dir, "server-two", ca, 102, "rotation.test")
	_, second := handshakeTLS(t, config.getServerTLSConfig(), clientConfig)
	require.Equal(t, int64(102), second.PeerCertificates[0].SerialNumber.Int64())

	require.NoError(t, os.WriteFile(certPath, []byte("invalid"), 0o600))
	_, err := config.getServerTLSConfig().GetCertificate(nil)
	require.ErrorContains(t, err, "can not reload key pair")
}

func TestClientCertificateReloadedForEveryHandshake(t *testing.T) {
	ca := newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(t, serverDir, "server", ca, 201, "rotation.test")
	serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca.pem))
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}

	clientDir := t.TempDir()
	certPath, keyPath := writeRotationCertificate(t, clientDir, "client-one", ca, 301, "rotation.test")
	config := &SecurityConfig{CertFile: certPath, KeyFile: keyPath, CA: filepath.Join(clientDir, "ca.crt"), ServerName: "rotation.test"}
	require.NoError(t, os.WriteFile(config.CA, ca.pem, 0o600))
	require.NoError(t, config.validate())
	require.Empty(t, config.getClientTLSConfig().Certificates)

	first, _ := handshakeTLS(t, serverConfig, config.getClientTLSConfig())
	require.Equal(t, int64(301), first.PeerCertificates[0].SerialNumber.Int64())

	writeRotationCertificate(t, clientDir, "client-two", ca, 302, "rotation.test")
	second, _ := handshakeTLS(t, serverConfig, config.getClientTLSConfig())
	require.Equal(t, int64(302), second.PeerCertificates[0].SerialNumber.Int64())
}

func TestDistinctOutboundClientCertificateReloadedForEveryHandshake(t *testing.T) {
	ca := newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(t, serverDir, "remote-server", ca, 321, "rotation.test")
	serverCert, _ := leafCertificate(t, serverCertPath, serverKeyPath)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca.pem))
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
	}

	localDir := t.TempDir()
	servingCertPath, servingKeyPath := writeRotationCertificate(t, localDir, "serving", ca, 322, "rotation.test")
	outboundDir := t.TempDir()
	outboundCertPath, outboundKeyPath := writeRotationCertificate(t, outboundDir, "outbound-one", ca, 323, "rotation.test")
	caPath := filepath.Join(localDir, "ca.crt")
	writeRotationCA(t, caPath, ca)
	config := &SecurityConfig{
		CertFile: servingCertPath, KeyFile: servingKeyPath,
		ClientCertFile: outboundCertPath, ClientKeyFile: outboundKeyPath,
		CA: caPath, ServerName: "rotation.test",
	}
	require.NoError(t, config.validate())

	first, _ := handshakeTLS(t, serverConfig, config.getClientTLSConfig())
	require.Equal(t, "outbound-one", first.PeerCertificates[0].Subject.CommonName)
	require.Equal(t, int64(323), first.PeerCertificates[0].SerialNumber.Int64())

	writeRotationCertificate(t, outboundDir, "outbound-two", ca, 324, "rotation.test")
	second, _ := handshakeTLS(t, serverConfig, config.getClientTLSConfig())
	require.Equal(t, "outbound-two", second.PeerCertificates[0].Subject.CommonName)
	require.Equal(t, int64(324), second.PeerCertificates[0].SerialNumber.Int64())
	serving, err := config.getServerTLSConfig().GetCertificate(nil)
	require.NoError(t, err)
	servingLeaf, err := x509.ParseCertificate(serving.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, int64(322), servingLeaf.SerialNumber.Int64())
}

func TestInboundCertificateIdentityAllowlists(t *testing.T) {
	ca, untrustedCA := newRotationCA(t), newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(t, serverDir, "server", ca, 331, "rotation.test")
	caPath := filepath.Join(serverDir, "ca.crt")
	writeRotationCA(t, caPath, ca)
	client := func(issuer rotationCA, cn, hostname string, serial int64) *tls.Config {
		dir := t.TempDir()
		certPath, keyPath := writeRotationCertificate(t, dir, cn, issuer, serial, hostname)
		cert, _ := leafCertificate(t, certPath, keyPath)
		return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true} //nolint:gosec -- server-side identity test
	}
	assertRejected := func(t *testing.T, serverConfig, clientConfig *tls.Config) {
		t.Helper()
		_, _, serverErr, _ := tryHandshakeTLS(t, serverConfig, clientConfig)
		require.Error(t, serverErr)
	}

	t.Run("common name", func(t *testing.T) {
		config := &SecurityConfig{
			CertFile: serverCertPath, KeyFile: serverKeyPath, CA: caPath, ClientAuth: true,
			AllowedCNs: []string{"allowed-client"},
		}
		require.NoError(t, config.validate())
		handshakeTLS(t, config.getServerTLSConfig(), client(ca, "allowed-client", "irrelevant.test", 332))
		assertRejected(t, config.getServerTLSConfig(), client(ca, "other-client", "irrelevant.test", 333))
		assertRejected(t, config.getServerTLSConfig(), client(untrustedCA, "allowed-client", "irrelevant.test", 334))
	})

	t.Run("hostname", func(t *testing.T) {
		config := &SecurityConfig{
			CertFile: serverCertPath, KeyFile: serverKeyPath, CA: caPath, ClientAuth: true,
			AllowedHostnames: []string{"workload.example"},
		}
		require.NoError(t, config.validate())
		handshakeTLS(t, config.getServerTLSConfig(), client(ca, "irrelevant", "workload.example", 335))
		assertRejected(t, config.getServerTLSConfig(), client(ca, "irrelevant", "other.example", 336))
	})
}

func TestCertificateIdentityConfigurationValidation(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	certPath, keyPath := writeRotationCertificate(t, dir, "server", ca, 341, "rotation.test")
	caPath := filepath.Join(dir, "ca.crt")
	writeRotationCA(t, caPath, ca)

	tests := []struct {
		name   string
		mutate func(*SecurityConfig)
		want   string
	}{
		{name: "client cert without key", mutate: func(c *SecurityConfig) { c.ClientCertFile = certPath }, want: "must both be present"},
		{name: "client key without cert", mutate: func(c *SecurityConfig) { c.ClientKeyFile = keyPath }, want: "must both be present"},
		{name: "CN and hostname", mutate: func(c *SecurityConfig) {
			c.AllowedCNs = []string{"client"}
			c.AllowedHostnames = []string{"client.example"}
		}, want: "mutually exclusive"},
		{name: "allowlist without client auth", mutate: func(c *SecurityConfig) {
			c.AllowedCNs = []string{"client"}
		}, want: "require client cert auth"},
		{name: "allowlist without CA", mutate: func(c *SecurityConfig) {
			c.CA = ""
			c.ClientAuth = true
			c.AllowedCNs = []string{"client"}
		}, want: "trusted CA"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SecurityConfig{CertFile: certPath, KeyFile: keyPath, CA: caPath}
			tt.mutate(config)
			require.ErrorContains(t, config.validate(), tt.want)
		})
	}
}

func TestInboundClientCATrustPoolRotation(t *testing.T) {
	oldCA, newCA := newRotationCA(t), newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(t, serverDir, "server", oldCA, 401, "rotation.test")
	caPath := filepath.Join(serverDir, "ca.crt")
	writeRotationCA(t, caPath, oldCA)
	config := &SecurityConfig{CertFile: serverCertPath, KeyFile: serverKeyPath, CA: caPath, ClientAuth: true}
	require.NoError(t, config.validate())

	clientConfig := func(ca rotationCA, serial int64) *tls.Config {
		dir := t.TempDir()
		certPath, keyPath := writeRotationCertificate(t, dir, "client", ca, serial, "rotation.test")
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		require.NoError(t, err)
		return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true} //nolint:gosec -- server trust test
	}
	oldClient, newClient := clientConfig(oldCA, 402), clientConfig(newCA, 403)
	handshakeTLS(t, config.getServerTLSConfig(), oldClient)

	writeRotationCA(t, caPath, oldCA, newCA)
	handshakeTLS(t, config.getServerTLSConfig(), oldClient)
	handshakeTLS(t, config.getServerTLSConfig(), newClient)

	writeRotationCA(t, caPath, newCA)
	handshakeTLS(t, config.getServerTLSConfig(), newClient)
	_, _, serverErr, clientErr := tryHandshakeTLS(t, config.getServerTLSConfig(), oldClient)
	require.Error(t, serverErr)
	_ = clientErr // TLS 1.3 may receive the server alert after its local handshake returns.

	require.NoError(t, os.WriteFile(caPath, []byte("invalid"), 0o600))
	_, err := config.getServerTLSConfig().GetConfigForClient(nil)
	require.ErrorContains(t, err, "no certificates found")
}

func TestOutboundServerCATrustPoolRotation(t *testing.T) {
	oldCA, newCA := newRotationCA(t), newRotationCA(t)
	clientDir := t.TempDir()
	clientCertPath, clientKeyPath := writeRotationCertificate(t, clientDir, "client", oldCA, 501, "rotation.test")
	caPath := filepath.Join(clientDir, "ca.crt")
	writeRotationCA(t, caPath, oldCA)
	config := &SecurityConfig{
		CertFile: clientCertPath, KeyFile: clientKeyPath, CA: caPath, ServerName: "rotation.test",
	}
	require.NoError(t, config.validate())

	serverConfig := func(ca rotationCA, serial int64) *tls.Config {
		dir := t.TempDir()
		certPath, keyPath := writeRotationCertificate(t, dir, "server", ca, serial, "rotation.test")
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		require.NoError(t, err)
		return &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	oldServer, newServer := serverConfig(oldCA, 502), serverConfig(newCA, 503)
	handshakeTLS(t, oldServer, config.getClientTLSConfig())

	writeRotationCA(t, caPath, oldCA, newCA)
	handshakeTLS(t, oldServer, config.getClientTLSConfig())
	handshakeTLS(t, newServer, config.getClientTLSConfig())

	writeRotationCA(t, caPath, newCA)
	handshakeTLS(t, newServer, config.getClientTLSConfig())
	_, _, serverErr, clientErr := tryHandshakeTLS(t, oldServer, config.getClientTLSConfig())
	_ = serverErr // TLS 1.3 may receive the client alert after its local handshake returns.
	require.Error(t, clientErr)

	wrongNameDir := t.TempDir()
	wrongCertPath, wrongKeyPath := writeRotationCertificate(t, wrongNameDir, "server", newCA, 504, "wrong.test")
	wrongCert, err := tls.LoadX509KeyPair(wrongCertPath, wrongKeyPath)
	require.NoError(t, err)
	_, _, _, clientErr = tryHandshakeTLS(t,
		&tls.Config{Certificates: []tls.Certificate{wrongCert}}, config.getClientTLSConfig())
	require.ErrorContains(t, clientErr, "rotation.test")

	require.NoError(t, os.WriteFile(caPath, []byte("invalid"), 0o600))
	_, _, _, clientErr = tryHandshakeTLS(t, newServer, config.getClientTLSConfig())
	require.ErrorContains(t, clientErr, "no certificates found")
}

func TestIdentityTLSListenerContinuesAfterRejectedHandshake(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	certPath, keyPath := writeRotationCertificate(t, dir, "server", ca, 601, "rotation.test")
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	rawListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer rawListener.Close()
	listener := &identityTLSListener{
		Listener: rawListener,
		config: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
		identities: &transportidentity.Registry{},
	}
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	rejected, err := net.Dial("tcp", rawListener.Addr().String())
	require.NoError(t, err)
	_, err = rejected.Write([]byte("not a TLS handshake"))
	require.NoError(t, err)
	require.NoError(t, rejected.Close())

	valid, err := tls.Dial("tcp", rawListener.Addr().String(), &tls.Config{InsecureSkipVerify: true}) //nolint:gosec -- listener resilience test
	require.NoError(t, err)
	defer valid.Close()
	select {
	case conn := <-accepted:
		require.NoError(t, conn.Close())
	case err := <-acceptErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not accept a valid connection after rejecting a malformed handshake")
	}
}

func TestTLSPolicyControlsNegotiatedVersionAndCipher(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	certPath, keyPath := writeRotationCertificate(t, dir, "server", ca, 701, "rotation.test")
	security := &SecurityConfig{CertFile: certPath, KeyFile: keyPath}
	config := &Config{
		Port: 2379, PeerPort: 2380, TLSMinVersion: "TLS1.3", TLSMaxVersion: "TLS1.3",
		ClientSecurityConfig: security, PeerSecurityConfig: &SecurityConfig{CertFile: certPath, KeyFile: keyPath},
	}
	require.NoError(t, config.Validate())
	_, _, serverErr, clientErr := tryHandshakeTLS(t, security.getServerTLSConfig(), &tls.Config{
		InsecureSkipVerify: true, MaxVersion: tls.VersionTLS12,
	})
	require.Error(t, serverErr)
	require.Error(t, clientErr)
	_, clientState := handshakeTLS(t, security.getServerTLSConfig(), &tls.Config{
		InsecureSkipVerify: true, MinVersion: tls.VersionTLS13,
	})
	require.Equal(t, uint16(tls.VersionTLS13), clientState.Version)

	security = &SecurityConfig{CertFile: certPath, KeyFile: keyPath}
	config = &Config{
		Port: 2379, PeerPort: 2380, TLSMinVersion: "TLS1.2", TLSMaxVersion: "TLS1.2",
		CipherSuites:         []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"},
		ClientSecurityConfig: security, PeerSecurityConfig: &SecurityConfig{CertFile: certPath, KeyFile: keyPath},
	}
	require.NoError(t, config.Validate())
	_, clientState = handshakeTLS(t, security.getServerTLSConfig(), &tls.Config{
		InsecureSkipVerify: true, MaxVersion: tls.VersionTLS12,
		CipherSuites: []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
	})
	require.Equal(t, uint16(tls.VersionTLS12), clientState.Version)
	require.Equal(t, uint16(tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256), clientState.CipherSuite)
	_, _, serverErr, clientErr = tryHandshakeTLS(t, security.getServerTLSConfig(), &tls.Config{
		InsecureSkipVerify: true, MaxVersion: tls.VersionTLS12,
		CipherSuites: []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384},
	})
	require.Error(t, serverErr)
	require.Error(t, clientErr)
}

func TestCertificateRevocationListReloadedForEveryHandshake(t *testing.T) {
	ca := newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(t, serverDir, "server", ca, 801, "rotation.test")
	caPath, crlPath := filepath.Join(serverDir, "ca.crt"), filepath.Join(serverDir, "revoked.crl")
	writeRotationCA(t, caPath, ca)
	writeRotationCRL(t, crlPath, ca)
	security := &SecurityConfig{
		CertFile: serverCertPath, KeyFile: serverKeyPath, CA: caPath, CRL: crlPath,
		ServerName: "rotation.test", ClientAuth: true,
	}
	require.NoError(t, security.validate())

	clientDir := t.TempDir()
	clientCertPath, clientKeyPath := writeRotationCertificate(t, clientDir, "client", ca, 802, "rotation.test")
	clientCertificate, clientLeaf := leafCertificate(t, clientCertPath, clientKeyPath)
	inboundClient := &tls.Config{
		Certificates: []tls.Certificate{clientCertificate}, InsecureSkipVerify: true,
	}
	handshakeTLS(t, security.getServerTLSConfig(), inboundClient)
	writeRotationCRL(t, crlPath, ca, clientLeaf.SerialNumber)
	_, _, serverErr, _ := tryHandshakeTLS(t, security.getServerTLSConfig(), inboundClient)
	require.ErrorContains(t, serverErr, "revoked")

	serverCertificate, serverLeaf := leafCertificate(t, serverCertPath, serverKeyPath)
	writeRotationCRL(t, crlPath, ca)
	handshakeTLS(t, &tls.Config{Certificates: []tls.Certificate{serverCertificate}}, security.getClientTLSConfig())
	writeRotationCRL(t, crlPath, ca, serverLeaf.SerialNumber)
	_, _, _, clientErr := tryHandshakeTLS(t,
		&tls.Config{Certificates: []tls.Certificate{serverCertificate}}, security.getClientTLSConfig())
	require.ErrorContains(t, clientErr, "revoked")

	require.NoError(t, os.WriteFile(crlPath, []byte("invalid"), 0o600))
	_, _, _, clientErr = tryHandshakeTLS(t,
		&tls.Config{Certificates: []tls.Certificate{serverCertificate}}, security.getClientTLSConfig())
	require.ErrorContains(t, clientErr, "can not parse certificate revocation list")
}

func TestCertificateRevocationListRejectsResumedClientSession(t *testing.T) {
	ca := newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(
		t, serverDir, "server", ca, 901, "rotation.test",
	)
	caPath, crlPath := filepath.Join(serverDir, "ca.crt"), filepath.Join(serverDir, "revoked.crl")
	writeRotationCA(t, caPath, ca)
	writeRotationCRL(t, crlPath, ca)
	security := &SecurityConfig{
		CertFile: serverCertPath, KeyFile: serverKeyPath, CA: caPath, CRL: crlPath,
		ServerName: "rotation.test", ClientAuth: true,
		minVersion: tls.VersionTLS12, maxVersion: tls.VersionTLS12,
	}
	require.NoError(t, security.validate())

	clientDir := t.TempDir()
	clientCertPath, clientKeyPath := writeRotationCertificate(
		t, clientDir, "client", ca, 902, "rotation.test",
	)
	clientCertificate, clientLeaf := leafCertificate(t, clientCertPath, clientKeyPath)
	clientConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCertificate},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS12,
		ClientSessionCache: tls.NewLRUClientSessionCache(1),
		ServerName:         "rotation.test",
	}

	_, first := handshakeTLS(t, security.getServerTLSConfig(), clientConfig)
	require.False(t, first.DidResume)
	_, resumed := handshakeTLS(t, security.getServerTLSConfig(), clientConfig)
	require.True(t, resumed.DidResume, "the negative CRL check must exercise a resumed TLS session")

	writeRotationCRL(t, crlPath, ca, clientLeaf.SerialNumber)
	_, _, serverErr, _ := tryHandshakeTLS(t, security.getServerTLSConfig(), clientConfig)
	require.ErrorContains(t, serverErr, "revoked")
}

func TestCertificateRevocationListRejectsResumedServerSession(t *testing.T) {
	ca := newRotationCA(t)
	serverDir := t.TempDir()
	serverCertPath, serverKeyPath := writeRotationCertificate(
		t, serverDir, "server", ca, 911, "rotation.test",
	)
	serverCertificate, serverLeaf := leafCertificate(t, serverCertPath, serverKeyPath)
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCertificate},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	}

	clientDir := t.TempDir()
	clientCertPath, clientKeyPath := writeRotationCertificate(
		t, clientDir, "client", ca, 912, "rotation.test",
	)
	caPath, crlPath := filepath.Join(clientDir, "ca.crt"), filepath.Join(clientDir, "revoked.crl")
	writeRotationCA(t, caPath, ca)
	writeRotationCRL(t, crlPath, ca)
	security := &SecurityConfig{
		CertFile: clientCertPath, KeyFile: clientKeyPath, CA: caPath, CRL: crlPath,
		ServerName: "rotation.test",
		minVersion: tls.VersionTLS12, maxVersion: tls.VersionTLS12,
	}
	require.NoError(t, security.validate())
	clientConfig := security.getClientTLSConfig()
	clientConfig.ClientSessionCache = tls.NewLRUClientSessionCache(1)

	_, first := handshakeTLS(t, serverConfig, clientConfig)
	require.False(t, first.DidResume)
	_, resumed := handshakeTLS(t, serverConfig, clientConfig)
	require.True(t, resumed.DidResume, "the negative CRL check must exercise a resumed TLS session")

	writeRotationCRL(t, crlPath, ca, serverLeaf.SerialNumber)
	_, _, _, clientErr := tryHandshakeTLS(t, serverConfig, clientConfig)
	require.ErrorContains(t, clientErr, "revoked")
}
