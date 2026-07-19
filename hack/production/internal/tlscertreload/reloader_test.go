package tlscertreload

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReloaderRotatesCertificateAndRetainsLastValidPair(t *testing.T) {
	certFile := filepath.Join(t.TempDir(), "tls.crt")
	keyFile := filepath.Join(filepath.Dir(certFile), "tls.key")
	writeCertificate(t, certFile, keyFile, 1)

	reloader, err := New(certFile, keyFile)
	require.NoError(t, err)
	server := &http.Server{
		Handler: http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		}),
		TLSConfig: &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: reloader.GetCertificate,
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Close())
	})
	go func() {
		_ = server.ServeTLS(listener, "", "")
	}()

	require.Equal(t, int64(1), peerSerial(t, listener.Addr().String()))
	writeCertificate(t, certFile, keyFile, 2)
	require.NoError(t, reloader.Reload())
	require.Equal(t, int64(2), peerSerial(t, listener.Addr().String()))

	require.NoError(t, os.WriteFile(keyFile, []byte("not a private key"), 0o600))
	require.Error(t, reloader.Reload())
	require.Equal(t, int64(2), peerSerial(t, listener.Addr().String()))
}

func TestReloaderRunReloadsAndReportsInvalidUpdates(t *testing.T) {
	certFile := filepath.Join(t.TempDir(), "tls.crt")
	keyFile := filepath.Join(filepath.Dir(certFile), "tls.key")
	writeCertificate(t, certFile, keyFile, 1)
	reloader, err := New(certFile, keyFile)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reloadErrors := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- reloader.Run(ctx, time.Millisecond, func(err error) {
			select {
			case reloadErrors <- err:
			default:
			}
		})
	}()

	writeCertificate(t, certFile, keyFile, 3)
	require.Eventually(t, func() bool {
		certificate, getErr := reloader.GetCertificate(nil)
		if getErr != nil {
			return false
		}
		leaf, parseErr := x509.ParseCertificate(certificate.Certificate[0])
		return parseErr == nil && leaf.SerialNumber.Int64() == 3
	}, time.Second, time.Millisecond)

	require.NoError(t, os.WriteFile(certFile, []byte("invalid"), 0o600))
	select {
	case <-reloadErrors:
	case <-time.After(time.Second):
		t.Fatal("invalid certificate update was not reported")
	}
	cancel()
	require.NoError(t, <-done)
}

func TestReloaderRejectsInvalidConfigurationAndInterval(t *testing.T) {
	_, err := New("", "")
	require.Error(t, err)

	certFile := filepath.Join(t.TempDir(), "tls.crt")
	keyFile := filepath.Join(filepath.Dir(certFile), "tls.key")
	writeCertificate(t, certFile, keyFile, 1)
	reloader, err := New(certFile, keyFile)
	require.NoError(t, err)
	require.Error(t, reloader.Run(context.Background(), 0, nil))
	require.NoError(t, reloader.ValidAt(time.Now()))
	require.Error(t, reloader.ValidAt(time.Now().Add(2*time.Hour)))
}

func peerSerial(t *testing.T, address string) int64 {
	t.Helper()
	connection, err := tls.Dial("tcp", address, &tls.Config{
		InsecureSkipVerify: true, // The generated certificate is inspected below.
		MinVersion:         tls.VersionTLS12,
	})
	require.NoError(t, err)
	defer connection.Close()
	require.Len(t, connection.ConnectionState().PeerCertificates, 1)
	return connection.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
}

func writeCertificate(t *testing.T, certFile, keyFile string, serial int64) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "operation-parameter-broker"},
		DNSNames:     []string{"operation-parameter-broker"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600))
}
