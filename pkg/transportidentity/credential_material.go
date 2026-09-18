package transportidentity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
)

// ClientCredentialMaterial contains data, never arbitrary TLS callbacks. An
// internal transport can build its own verifier from fresh material without
// disabling chain, hostname, revocation or member-pin checks. This is not itself
// a TLS configuration or permission to accept a peer certificate.
type ClientCredentialMaterial struct {
	Certificate            tls.Certificate
	Roots                  *x509.CertPool
	Revocations            *x509.RevocationList
	ServerName             string
	MinVersion, MaxVersion uint16
	CipherSuites           []uint16
}

// ClientCredentialSource returns a detached, size-bounded snapshot for one load.
// Consumers must re-load at each handshake/revalidation, validate local holder
// ownership, and verify peers themselves. A failed load must never fall back to
// stale material. Implementations must not log key contents or return them in
// errors. These sources do not control certificate authorization decisions.
type ClientCredentialSource interface {
	LoadClientCredentialMaterial(context.Context) (ClientCredentialMaterial, error)
}
