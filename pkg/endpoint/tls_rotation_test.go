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
		KeyUsage:              x509.KeyUsageCertSign,
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
	serverRaw, clientRaw := net.Pipe()
	server := tls.Server(serverRaw, serverConfig)
	client := tls.Client(clientRaw, clientConfig)
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Handshake() }()
	require.NoError(t, client.Handshake())
	require.NoError(t, <-serverErr)
	serverState, clientState := server.ConnectionState(), client.ConnectionState()
	require.NoError(t, clientRaw.Close())
	require.NoError(t, serverRaw.Close())
	return serverState, clientState
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
