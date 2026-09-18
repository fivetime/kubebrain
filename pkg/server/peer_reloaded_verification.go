package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

var errPeerCredentialVerification = errors.New("peer credential verification failed")

// validateLocalPeerMaterial checks every fresh load, not just initial startup.
// Leaf caches and an earlier successful snapshot are not authority for the new
// private key. It returns a detached parsed leaf for the owned snapshot.
func validateLocalPeerMaterial(material transportidentity.ClientCredentialMaterial, auth *peerRetirementAuthorizer, holder string, now time.Time) (tls.Certificate, error) {
	invalid := func() (tls.Certificate, error) { return tls.Certificate{}, errPeerCredentialVerification }
	if auth == nil || material.Roots == nil || len(material.Certificate.Certificate) == 0 || len(material.Certificate.Certificate) > 16 || (material.MaxVersion != 0 && material.MaxVersion < max(material.MinVersion, tls.VersionTLS12)) {
		return invalid()
	}
	certificate := material.Certificate
	chain := make([][]byte, len(certificate.Certificate))
	total := 0
	for i, der := range certificate.Certificate {
		if len(der) > (1<<20)-total {
			return invalid()
		}
		total += len(der)
		chain[i] = bytes.Clone(der)
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil || now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return invalid()
	}
	key, err := retirementSigningPublicKey(certificate.PrivateKey)
	if err != nil || !bytes.Equal(key, leaf.RawSubjectPublicKeyInfo) {
		return invalid()
	}
	if _, ok := auth.holders[holder][sha256.Sum256(leaf.RawSubjectPublicKeyInfo)]; !ok {
		return invalid()
	}
	return tls.Certificate{Certificate: chain, PrivateKey: certificate.PrivateKey, Leaf: leaf}, nil
}

// verifyReloadedPeer deliberately rebuilds chains using CURRENT material. Old
// ConnectionState.VerifiedChains (including a resumed session's old trust) must
// not bypass removal of a CA or a newly published CRL. Use only transport-owned
// ConnectionState, never a certificate supplied in a request body/header.
func (a *peerRetirementAuthorizer) verifyReloadedPeer(material transportidentity.ClientCredentialMaterial, state tls.ConnectionState, holder, hostname string, now time.Time) (tls.ConnectionState, error) {
	invalid := func() (tls.ConnectionState, error) { return tls.ConnectionState{}, errPeerCredentialVerification }
	if a == nil || material.Roots == nil || hostname == "" || len(state.PeerCertificates) == 0 || state.PeerCertificates[0] == nil || state.Version < max(material.MinVersion, tls.VersionTLS12) || (material.MaxVersion != 0 && state.Version > material.MaxVersion) {
		return invalid()
	}
	if material.ServerName != "" {
		hostname = material.ServerName
	}
	intermediates := x509.NewCertPool()
	for _, cert := range state.PeerCertificates[1:] {
		if cert == nil {
			return invalid()
		}
		intermediates.AddCert(cert)
	}
	chains, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: material.Roots,
		Intermediates: intermediates, DNSName: hostname, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	if err != nil {
		return invalid()
	}
	if material.Revocations != nil {
		allowed := chains[:0:0]
		for _, chain := range chains {
			if verifyPeerRevocations(material.Revocations, chain, now) == nil {
				allowed = append(allowed, chain)
			}
		}
		chains = allowed
		if len(chains) == 0 {
			return invalid()
		}
	}
	state.VerifiedChains = chains
	if err := a.authorizeVerifiedCertificate(&state, a.instance, holder, now); err != nil {
		return invalid()
	}
	return state, nil
}

// The experimental single-CRL policy requires a current, signed, full CRL from
// the leaf's issuer. Delta/partitioned CRLs are unsupported and fail closed.
// This stricter check does not alter the existing endpoint CRL implementation.
func verifyPeerRevocations(list *x509.RevocationList, chain []*x509.Certificate, now time.Time) error {
	if len(chain) < 2 || list.ThisUpdate.After(now) || list.NextUpdate.IsZero() || !now.Before(list.NextUpdate) || !bytes.Equal(list.RawIssuer, chain[1].RawSubject) || list.CheckSignatureFrom(chain[1]) != nil {
		return errPeerCredentialVerification
	}
	for _, extension := range list.Extensions {
		if extension.Critical || extension.Id.String() == "2.5.29.27" || extension.Id.String() == "2.5.29.28" {
			return errPeerCredentialVerification
		}
	}
	for _, entry := range list.RevokedCertificateEntries {
		if entry.SerialNumber == nil {
			return errPeerCredentialVerification
		}
		for _, extension := range entry.Extensions {
			if extension.Critical || extension.Id.String() == "2.5.29.29" {
				return errPeerCredentialVerification
			}
		}
		for _, certificate := range chain {
			if certificate.SerialNumber.Cmp(entry.SerialNumber) == 0 {
				return errPeerCredentialVerification
			}
		}
	}
	return nil
}
