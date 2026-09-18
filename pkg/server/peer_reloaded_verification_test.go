package server

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
)

func TestReloadedPeerVerificationOnRealResumedHandshake(t *testing.T) {
	pool, certs, issuer, signer := retirementTestCertificatesAndIssuer(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"next": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certs[1]}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
		MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()

	material := transportidentity.ClientCredentialMaterial{Roots: pool}
	var resumed bool
	verifications := 0
	config := &tls.Config{Certificates: []tls.Certificate{certs[0]}, ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12, ClientSessionCache: tls.NewLRUClientSessionCache(1),
		// This test exercises the complete replacement verifier, including chain
		// and hostname checks. It is not a production TLS configuration.
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			verifications++
			resumed = state.DidResume
			_, err := auth.verifyReloadedPeer(material, state, "next", "127.0.0.1", time.Now())
			return err
		},
	}
	dial := func(wantResume, wantError bool) {
		t.Helper()
		before := verifications
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", strings.TrimPrefix(server.URL, "https://"), config)
		if conn != nil {
			require.NoError(t, conn.Close())
		}
		if wantError {
			require.ErrorIs(t, err, errPeerCredentialVerification)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, before+1, verifications)
		require.Equal(t, wantResume, resumed)
	}
	dial(false, false)
	dial(true, false)
	material.Roots = x509.NewCertPool()
	dial(true, true)
	material.Roots = pool
	// Go evicts a cached session after a failed handshake; establish a new one.
	dial(false, false)
	now := time.Now()
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: certs[1].Leaf.SerialNumber, RevocationTime: now.Add(-time.Minute)}},
	}, issuer, signer)
	require.NoError(t, err)
	material.Revocations, err = x509.ParseRevocationList(der)
	require.NoError(t, err)
	dial(true, true)
}

func TestReloadedPeerVerificationDoesNotTrustOldChains(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local": {retirementTestPin(certs[0])}, "next": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	now := time.Now()
	oldChains, err := certs[1].Leaf.Verify(x509.VerifyOptions{Roots: pool})
	require.NoError(t, err)
	for _, mode := range []string{"valid", "removed CA", "wrong hostname", "wrong holder", "expired", "old TLS", "maximum TLS", "nil roots", "unknown holder", "nil peer", "no peer"} {
		t.Run(mode, func(t *testing.T) {
			material := transportidentity.ClientCredentialMaterial{Roots: pool}
			state := tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{certs[1].Leaf}, VerifiedChains: oldChains, DidResume: true}
			hostname, holder, when := "127.0.0.1", "next", now
			switch mode {
			case "removed CA":
				material.Roots = x509.NewCertPool()
			case "wrong hostname":
				hostname = "unrelated.invalid"
			case "wrong holder":
				holder = "local"
			case "expired":
				when = now.Add(2 * time.Hour)
			case "old TLS":
				state.Version = tls.VersionTLS11
			case "maximum TLS":
				material.MaxVersion = tls.VersionTLS12
			case "nil roots":
				material.Roots = nil
			case "unknown holder":
				holder = "unknown"
			case "nil peer":
				state.PeerCertificates = []*x509.Certificate{nil}
			case "no peer":
				state.PeerCertificates = nil
			}
			verified, err := auth.verifyReloadedPeer(material, state, holder, hostname, when)
			if mode == "valid" {
				require.NoError(t, err)
				require.NotEmpty(t, verified.VerifiedChains)
			} else {
				require.ErrorIs(t, err, errPeerCredentialVerification)
				require.Empty(t, verified.VerifiedChains)
			}
		})
	}
}

func TestReloadedPeerVerificationChecksSignedCurrentCRL(t *testing.T) {
	pool, certs, issuer, signer := retirementTestCertificatesAndIssuer(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"next": {retirementTestPin(certs[1])}})
	require.NoError(t, err)
	now := time.Now()
	for _, mode := range []string{"valid", "revoked", "stale", "future", "wrong issuer", "corrupt signature", "delta", "partitioned", "unknown critical", "critical entry"} {
		t.Run(mode, func(t *testing.T) {
			template := &x509.RevocationList{Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour)}
			root, key := issuer, signer
			switch mode {
			case "critical entry":
				template.RevokedCertificateEntries = []x509.RevocationListEntry{{SerialNumber: big.NewInt(999), RevocationTime: now.Add(-time.Minute), ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{1, 2, 3, 4}, Critical: true, Value: []byte{5, 0}}}}}
			case "revoked":
				template.RevokedCertificateEntries = []x509.RevocationListEntry{{SerialNumber: certs[1].Leaf.SerialNumber, RevocationTime: now.Add(-time.Minute)}}
			case "stale":
				template.NextUpdate = now.Add(-time.Second)
			case "future":
				template.ThisUpdate = now.Add(time.Minute)
			case "wrong issuer":
				_, _, root, key = retirementTestCertificatesAndIssuer(t)
			case "delta":
				template.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 27}, Value: []byte{2, 1, 1}}}
			case "partitioned":
				template.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Value: []byte{0x30, 0}}}
			case "unknown critical":
				template.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{1, 2, 3, 4}, Critical: true, Value: []byte{5, 0}}}
			}
			der, err := x509.CreateRevocationList(rand.Reader, template, root, key)
			require.NoError(t, err)
			if mode == "corrupt signature" {
				der[len(der)-1] ^= 1
			}
			list, err := x509.ParseRevocationList(der)
			require.NoError(t, err)
			material := transportidentity.ClientCredentialMaterial{Roots: pool, Revocations: list}
			state := tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{certs[1].Leaf}}
			_, err = auth.verifyReloadedPeer(material, state, "next", "127.0.0.1", now)
			if mode == "valid" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errPeerCredentialVerification)
			}
		})
	}
}

func TestReloadedLocalMaterialRevalidatesSignerAndPin(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	for _, mode := range []string{"valid", "wrong key", "wrong pin", "stale leaf cache", "missing roots"} {
		t.Run(mode, func(t *testing.T) {
			m := transportidentity.ClientCredentialMaterial{Certificate: certs[0], Roots: pool}
			switch mode {
			case "wrong key":
				m.Certificate.PrivateKey = certs[1].PrivateKey
			case "wrong pin":
				m.Certificate = certs[1]
			case "stale leaf cache":
				m.Certificate.Leaf = certs[1].Leaf
			case "missing roots":
				m.Roots = nil
			}
			certificate, err := validateLocalPeerMaterial(m, auth, "local", time.Now())
			if mode == "valid" || mode == "stale leaf cache" {
				require.NoError(t, err)
				require.Equal(t, certs[0].Leaf.Raw, certificate.Leaf.Raw)
			} else {
				require.ErrorIs(t, err, errPeerCredentialVerification)
			}
		})
	}
}
