package server

import (
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPeerRetirementSenderRejectsMalformedSigningKeys(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	for _, key := range []any{nil, "not a signer", ed25519.PrivateKey{1}, (*rsa.PrivateKey)(nil)} {
		certificate := certs[0]
		certificate.PrivateKey = key
		sender, err := newPeerRetirementSender("instance", "old", []string{"https://peer.invalid"},
			&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certificate}}, time.Second)
		require.Error(t, err)
		require.Nil(t, sender)
	}
}

func TestPeerRetirementSigningCheckUsesDERNotCachedLeaf(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	certificate := certs[0]
	certificate.Leaf = certs[1].Leaf
	_, err := newPeerRetirementSender("instance", "old", []string{"https://peer.invalid"},
		&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certificate}}, time.Second)
	require.NoError(t, err, "the DER and signer match; a caller-owned Leaf cache is not authoritative")
}

func TestPeerRetirementSenderRejectsMismatchedSigningKeyBeforeStartup(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	certificate := certs[0]
	certificate.PrivateKey = certs[1].PrivateKey
	sender, err := newPeerRetirementSender("instance", "old", []string{"https://peer.invalid"},
		&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certificate}}, time.Second)
	require.Error(t, err, "a pinned public certificate is insufficient when the configured signer belongs to another key")
	require.Nil(t, sender)
}
