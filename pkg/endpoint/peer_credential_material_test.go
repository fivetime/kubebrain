package endpoint

import (
	"bytes"
	"context"
	"crypto/x509"
	"math/big"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerCredentialMaterialReloadsAllFiles(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	cert, key := writeRotationCertificate(t, dir, "local", ca, 71, "peer.test")
	caFile, crlFile := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "crl.der")
	writeRotationCA(t, caFile, ca)
	writeRotationCRL(t, crlFile, ca)
	sc := &SecurityConfig{CertFile: cert, KeyFile: key, CA: caFile, CRL: crlFile, ClientAuth: true, ServerName: "peer.test"}
	source, err := newPeerCredentialSource(sc)
	require.NoError(t, err)
	sc.CA = "mutated caller path"
	sc.ServerName = "mutated caller name"
	first, err := source.LoadClientCredentialMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(71), first.Certificate.Leaf.SerialNumber.Int64())
	require.Equal(t, "peer.test", first.ServerName)
	require.Empty(t, first.Revocations.RevokedCertificateEntries)

	rotatedCA := newRotationCA(t)
	writeRotationCertificate(t, dir, "local-rotated", rotatedCA, 72, "peer.test")
	writeRotationCA(t, caFile, rotatedCA)
	writeRotationCRL(t, crlFile, rotatedCA, big.NewInt(99))
	second, err := source.LoadClientCredentialMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(72), second.Certificate.Leaf.SerialNumber.Int64())
	_, err = second.Certificate.Leaf.Verify(x509.VerifyOptions{Roots: second.Roots, DNSName: "peer.test"})
	require.NoError(t, err)
	_, err = first.Certificate.Leaf.Verify(x509.VerifyOptions{Roots: second.Roots, DNSName: "peer.test"})
	require.Error(t, err, "rotated roots must not retain removed CA")
	require.Len(t, second.Revocations.RevokedCertificateEntries, 1)
	require.Equal(t, int64(99), second.Revocations.RevokedCertificateEntries[0].SerialNumber.Int64())
	second.Certificate.Certificate[0][0] ^= 0xff
	second.Revocations.RevokedCertificateEntries[0].SerialNumber.SetInt64(1000)
	third, err := source.LoadClientCredentialMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(72), third.Certificate.Leaf.SerialNumber.Int64())
	require.Equal(t, int64(99), third.Revocations.RevokedCertificateEntries[0].SerialNumber.Int64())

	for _, path := range []string{key, caFile, crlFile} {
		original, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte("PRIVATE-MATERIAL-MUST-NOT-LEAK"), 0600))
		material, err := source.LoadClientCredentialMaterial(context.Background())
		require.EqualError(t, err, "peer credential material unavailable")
		require.Empty(t, material.Certificate.Certificate, "no fallback to previous good snapshot")
		require.NoError(t, os.WriteFile(path, original, 0600))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = source.LoadClientCredentialMaterial(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPeerCredentialMaterialUsesDistinctClientPair(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	serverCert, serverKey := writeRotationCertificate(t, dir, "server", ca, 81, "peer.test")
	clientCert, clientKey := writeRotationCertificate(t, t.TempDir(), "client", ca, 82, "peer.test")
	caFile := filepath.Join(dir, "ca.pem")
	writeRotationCA(t, caFile, ca)
	sc := &SecurityConfig{CertFile: serverCert, KeyFile: serverKey, ClientCertFile: clientCert, ClientKeyFile: clientKey, CA: caFile, ClientAuth: true}
	source, err := newPeerCredentialSource(sc)
	require.NoError(t, err)
	material, err := source.LoadClientCredentialMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(82), material.Certificate.Leaf.SerialNumber.Int64())
	for _, bad := range []*SecurityConfig{nil, {}, {CertFile: serverCert, KeyFile: serverKey}, {CertFile: serverCert, KeyFile: serverKey, CA: caFile, ClientAuth: true, AllowInsecure: true}, {CertFile: serverCert, KeyFile: serverKey, CA: caFile, ClientAuth: true, ClientCertFile: clientCert}} {
		_, err := newPeerCredentialSource(bad)
		require.Error(t, err)
	}
}

func TestPeerCredentialFileRejectsNonRegularAndOversized(t *testing.T) {
	dir := t.TempDir()
	regular := filepath.Join(dir, "regular")
	require.NoError(t, os.WriteFile(regular, []byte("ok"), 0600))
	link := filepath.Join(dir, "secret-link")
	require.NoError(t, os.Symlink(regular, link))
	contents, err := readPeerCredentialFile(link, 3)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), contents)
	require.NoError(t, os.WriteFile(regular, bytes.Repeat([]byte("x"), 4), 0600))
	_, err = readPeerCredentialFile(link, 3)
	require.Error(t, err)
	fifo := filepath.Join(dir, "fifo")
	require.NoError(t, syscall.Mkfifo(fifo, 0600))
	for _, path := range []string{fifo, dir, "/dev/null"} {
		_, err := readPeerCredentialFile(path, 3)
		require.Error(t, err)
	}
}
