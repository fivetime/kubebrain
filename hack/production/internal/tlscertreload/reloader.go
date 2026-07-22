package tlscertreload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

const maxTLSCertificateFileBytes int64 = 1 << 20

type Reloader struct {
	certFile string
	keyFile  string
	current  atomic.Pointer[tls.Certificate]
}

func New(certFile, keyFile string) (*Reloader, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("certificate and key files are required")
	}
	reloader := &Reloader{certFile: certFile, keyFile: keyFile}
	if err := reloader.Reload(); err != nil {
		return nil, err
	}
	return reloader, nil
}

func (r *Reloader) Reload() error {
	certificate, err := loadX509KeyPairBounded(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("load TLS certificate: %w", err)
	}
	if len(certificate.Certificate) == 0 {
		return fmt.Errorf("load TLS certificate: certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return fmt.Errorf("parse TLS leaf certificate: %w", err)
	}
	if err := validAt(leaf, time.Now()); err != nil {
		return err
	}
	certificate.Leaf = leaf
	r.current.Store(&certificate)
	return nil
}

func loadX509KeyPairBounded(certFile, keyFile string) (tls.Certificate, error) {
	certPEMBlock, err := readBoundedCertificateFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEMBlock, err := readBoundedCertificateFile(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}

func readBoundedCertificateFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxTLSCertificateFileBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxTLSCertificateFileBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, maxTLSCertificateFileBytes)
	}
	return data, nil
}

func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	certificate := r.current.Load()
	if certificate == nil {
		return nil, fmt.Errorf("TLS certificate is not loaded")
	}
	return certificate, nil
}

func (r *Reloader) ValidAt(now time.Time) error {
	certificate := r.current.Load()
	if certificate == nil || certificate.Leaf == nil {
		return fmt.Errorf("TLS certificate is not loaded")
	}
	return validAt(certificate.Leaf, now)
}

func validAt(certificate *x509.Certificate, now time.Time) error {
	switch {
	case now.Before(certificate.NotBefore):
		return fmt.Errorf("TLS certificate is not valid before %s", certificate.NotBefore.UTC().Format(time.RFC3339))
	case !now.Before(certificate.NotAfter):
		return fmt.Errorf("TLS certificate expired at %s", certificate.NotAfter.UTC().Format(time.RFC3339))
	default:
		return nil
	}
}

func (r *Reloader) Run(ctx context.Context, interval time.Duration, onError func(error)) error {
	if interval <= 0 {
		return fmt.Errorf("TLS certificate reload interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Reload(); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}
