package etcdutil

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeoutFromEnv(t *testing.T) {
	old := os.Getenv("TIMEOUT")
	defer os.Setenv("TIMEOUT", old)

	require.NoError(t, os.Unsetenv("TIMEOUT"))
	timeout, err := TimeoutFromEnv()
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, timeout)

	require.NoError(t, os.Setenv("TIMEOUT", "30s"))
	timeout, err = TimeoutFromEnv()
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, timeout)

	require.NoError(t, os.Setenv("TIMEOUT", "0s"))
	_, err = TimeoutFromEnv()
	require.Error(t, err)

	require.NoError(t, os.Setenv("TIMEOUT", "bad"))
	_, err = TimeoutFromEnv()
	require.Error(t, err)
}

func TestCredentialsFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		user     string
		password string
		wantUser string
		wantPass string
		wantErr  string
	}{
		{name: "disabled"},
		{name: "embedded", user: "root:secret", wantUser: "root", wantPass: "secret"},
		{name: "embedded colons", user: "root:secret:with:colons", wantUser: "root", wantPass: "secret:with:colons"},
		{name: "separate", user: "root", password: "secret", wantUser: "root", wantPass: "secret"},
		{name: "password only", password: "secret", wantErr: "requires ETCDCTL_USER"},
		{name: "empty username", user: ":secret", wantErr: "username must not be empty"},
		{name: "missing password", user: "root", wantErr: "requires a password"},
		{name: "two passwords", user: "root:embedded", password: "separate", wantErr: "must not include a password"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("ETCDCTL_USER", test.user)
			t.Setenv("ETCDCTL_PASSWORD", test.password)
			username, password, err := CredentialsFromEnv()
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantUser, username)
			require.Equal(t, test.wantPass, password)
		})
	}
}

func TestTLSConfigFromEnvRequiresCertAndKeyTogether(t *testing.T) {
	clearTLSEnv(t)

	cfg, err := TLSConfigFromEnv()
	require.NoError(t, err)
	require.Nil(t, cfg)

	require.NoError(t, os.Setenv("ETCDCTL_CERT", "/tmp/client.crt"))
	_, err = TLSConfigFromEnv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires both")
}

func TestTLSConfigFromEnvRejectsOversizedPEMFiles(t *testing.T) {
	cases := []struct {
		name string
		mut  func(t *testing.T, certPath, keyPath, caPath, oversizedPath string)
	}{
		{
			name: "cert",
			mut: func(t *testing.T, _, keyPath, _, oversizedPath string) {
				t.Setenv("ETCDCTL_CERT", oversizedPath)
				t.Setenv("ETCDCTL_KEY", keyPath)
			},
		},
		{
			name: "key",
			mut: func(t *testing.T, certPath, _, _, oversizedPath string) {
				t.Setenv("ETCDCTL_CERT", certPath)
				t.Setenv("ETCDCTL_KEY", oversizedPath)
			},
		},
		{
			name: "ca",
			mut: func(t *testing.T, certPath, keyPath, _, oversizedPath string) {
				t.Setenv("ETCDCTL_CACERT", oversizedPath)
				t.Setenv("ETCDCTL_CERT", certPath)
				t.Setenv("ETCDCTL_KEY", keyPath)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearTLSEnv(t)
			dir := t.TempDir()
			certPath := filepath.Join(dir, "client.crt")
			keyPath := filepath.Join(dir, "client.key")
			caPath := filepath.Join(dir, "ca.crt")
			writeClientCertificate(t, certPath, keyPath, caPath)
			oversizedPath := filepath.Join(dir, "oversized.pem")
			require.NoError(t, os.WriteFile(oversizedPath, make([]byte, maxEnvTLSPEMBytes+1), 0o600))

			tc.mut(t, certPath, keyPath, caPath, oversizedPath)
			_, err := TLSConfigFromEnv()
			require.ErrorContains(t, err, "exceeds")
		})
	}
}

func clearTLSEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"CACERT", "CERT", "KEY", "ETCD_CACERT", "ETCD_CERT", "ETCD_KEY", "ETCDCTL_CACERT", "ETCDCTL_CERT", "ETCDCTL_KEY"} {
		t.Setenv(name, "")
	}
}

func writeClientCertificate(t *testing.T, certPath, keyPath, caPath string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kubebrain-etcdutil-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(caPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
}
