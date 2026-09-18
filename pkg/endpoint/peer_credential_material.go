package endpoint

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

// peerCredentialSource captures operator-owned file paths and TLS policy, not
// live SecurityConfig callback pointers. Files are re-read on every Load. It is
// used by the explicit experimental endpoint mode. Existing normal endpoint TLS
// rotation and revocation callbacks remain unchanged.
type peerCredentialSource struct {
	certFile, keyFile, caFile, crlFile, serverName string
	minVersion, maxVersion                         uint16
	cipherSuites                                   []uint16
}

func newPeerCredentialSource(sc *SecurityConfig) (*peerCredentialSource, error) {
	if sc == nil || sc.mode() != modeOnlySecure || !sc.ClientAuth || sc.CA == "" {
		return nil, errors.New("peer credential source requires TLS-only mutual authentication")
	}
	if (sc.ClientCertFile == "") != (sc.ClientKeyFile == "") || (sc.maxVersion != 0 && sc.maxVersion < max(sc.minVersion, tls.VersionTLS12)) {
		return nil, errors.New("invalid peer credential configuration")
	}
	certFile, keyFile := sc.ClientCertFile, sc.ClientKeyFile
	if certFile == "" {
		certFile, keyFile = sc.CertFile, sc.KeyFile
	}
	return &peerCredentialSource{certFile: certFile, keyFile: keyFile, caFile: sc.CA, crlFile: sc.CRL,
		serverName: sc.ServerName, minVersion: sc.minVersion, maxVersion: sc.maxVersion,
		cipherSuites: append([]uint16(nil), sc.cipherSuites...)}, nil
}

func (s *peerCredentialSource) LoadClientCredentialMaterial(ctx context.Context) (transportidentity.ClientCredentialMaterial, error) {
	var empty transportidentity.ClientCredentialMaterial
	invalid := errors.New("peer credential material unavailable")
	if s == nil || ctx == nil {
		return empty, invalid
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	certPEM, err := readPeerCredentialFile(s.certFile, maxTLSPEMBytes)
	if err != nil {
		return empty, invalid
	}
	keyPEM, err := readPeerCredentialFile(s.keyFile, maxTLSPEMBytes)
	if err != nil {
		return empty, invalid
	}
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return empty, invalid
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return empty, invalid
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return empty, invalid
	}
	certificate.Leaf = leaf
	pem, err := readPeerCredentialFile(s.caFile, maxTLSPEMBytes)
	if err != nil {
		return empty, invalid
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return empty, invalid
	}
	var revocations *x509.RevocationList
	if s.crlFile != "" {
		der, readErr := readPeerCredentialFile(s.crlFile, maxTLSCRLBytes)
		if readErr != nil {
			return empty, invalid
		}
		revocations, err = x509.ParseRevocationList(der)
		if err != nil {
			return empty, invalid
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return transportidentity.ClientCredentialMaterial{Certificate: certificate, Roots: roots,
		Revocations: revocations, ServerName: s.serverName, MinVersion: max(s.minVersion, tls.VersionTLS12),
		MaxVersion: s.maxVersion, CipherSuites: append([]uint16(nil), s.cipherSuites...)}, nil
}

var _ transportidentity.ClientCredentialSource = (*peerCredentialSource)(nil)

// Nonblocking open rejects FIFOs/devices without waiting for a writer. Follow
// mounted-secret symlinks, but accept only an opened regular file, with a byte
// bound checked during reading (not only against a racy pre-open stat).
func readPeerCredentialFile(path string, limit int64) (contents []byte, retErr error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("invalid credential file")
	}
	contents, err = io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > limit {
		return nil, errors.New("credential file too large")
	}
	return contents, nil
}
