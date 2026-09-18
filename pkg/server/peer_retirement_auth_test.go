package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func retirementTestCertificates(t *testing.T) (*x509.CertPool, []tls.Certificate) {
	t.Helper()
	now := time.Now()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, root, root, pub, private)
	require.NoError(t, err)
	root, err = x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(root)
	var certificates []tls.Certificate
	for i := int64(2); i < 6; i++ {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		// Same CN and CA deliberately: neither can identify the holder.
		leaf := &x509.Certificate{SerialNumber: big.NewInt(i), Subject: pkix.Name{CommonName: "shared-cn"},
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * time.Minute),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, root, pub, private)
		require.NoError(t, err)
		leaf, err = x509.ParseCertificate(der)
		require.NoError(t, err)
		certificates = append(certificates, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf})
	}
	return pool, certificates
}

func retirementTestPin(cert tls.Certificate) string {
	key := sha256.Sum256(cert.Leaf.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(key[:])
}

func TestPeerRetirementIdentityConfiguration(t *testing.T) {
	_, certs := retirementTestCertificates(t)
	pin, next := retirementTestPin(certs[0]), retirementTestPin(certs[1])
	for name, pins := range map[string]map[string][]string{
		"empty": nil, "empty holder": {"": {pin}}, "missing pin": {"a": nil},
		"malformed": {"a": {"not-a-pin"}}, "short": {"a": {"00"}},
		"shared key": {"a": {pin}, "b": {pin}},
	} {
		t.Run(name, func(t *testing.T) {
			a, err := newPeerRetirementAuthorizer("instance", pins)
			require.Error(t, err)
			require.Nil(t, a)
		})
	}
	_, err := newPeerRetirementAuthorizer("", map[string][]string{"a": {pin}})
	require.Error(t, err)
	pins := map[string][]string{"a": {pin, next}}
	a, err := newPeerRetirementAuthorizer("instance", pins)
	require.NoError(t, err)
	pins["a"][0] = "changed"
	delete(pins, "a")
	require.Len(t, a.holders["a"], 2, "configuration must be detached and immutable")
}

func TestPeerRetirementRequiresVerifiedExactLeafAndScope(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	leaf := certs[0].Leaf
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	require.NoError(t, err)
	a, err := newPeerRetirementAuthorizer("instance", map[string][]string{"a": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	for _, name := range []string{"valid", "plaintext", "unverified", "wrong leaf", "unknown holder", "other instance", "expired", "not yet valid", "incomplete", "old tls", "disabled"} {
		t.Run(name, func(t *testing.T) {
			state := &tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true,
				PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}
			now, instance, holder, auth := time.Now(), "instance", "a", a
			switch name {
			case "plaintext":
				state = nil
			case "unverified":
				state.VerifiedChains = nil
			case "wrong leaf":
				state.PeerCertificates = []*x509.Certificate{certs[1].Leaf}
			case "unknown holder":
				holder = "b"
			case "other instance":
				instance = "other"
			case "expired":
				now = leaf.NotAfter.Add(time.Second)
			case "not yet valid":
				now = leaf.NotBefore.Add(-time.Second)
			case "incomplete":
				state.HandshakeComplete = false
			case "old tls":
				state.Version = tls.VersionTLS11
			case "disabled":
				auth = nil
			}
			err := auth.authorize(state, instance, holder, now)
			if name == "valid" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errPeerRetirementUnauthorized)
			}
		})
	}
}

func TestPeerRetirementRejectsSameCAClientAndWrongHolderOverTLS(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	a, err := newPeerRetirementAuthorizer("instance", map[string][]string{
		"a": {retirementTestPin(certs[0]), retirementTestPin(certs[1])},
		"b": {retirementTestPin(certs[2])},
	})
	require.NoError(t, err)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.authorize(r.TLS, r.URL.Query().Get("instance"), r.URL.Query().Get("holder"), time.Now()) != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certs[0]},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		name             string
		cert             int
		instance, holder string
		want             int
	}{
		{"holder a", 0, "instance", "a", 204}, {"rotated a", 1, "instance", "a", 204},
		{"holder b", 2, "instance", "b", 204}, {"b impersonates a", 2, "instance", "a", 403},
		{"a impersonates b", 0, "instance", "b", 403}, {"ordinary client same CA and CN", 3, "instance", "a", 403},
		{"other instance", 0, "other", "a", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{certs[tc.cert]}}}
			t.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			response, err := client.Get(srv.URL + "?instance=" + tc.instance + "&holder=" + tc.holder)
			require.NoError(t, err, "all cases must complete actual mutual TLS, including the denied ordinary client")
			require.NoError(t, response.Body.Close())
			require.Equal(t, tc.want, response.StatusCode)
		})
	}
}
