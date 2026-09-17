package compat

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEphemeralControlPlanePKI(t *testing.T) {
	pki := filepath.Join(t.TempDir(), "pki")
	helper := filepath.Join("..", "dev", "create-apiserver-test-pki.sh")
	out, err := runCompatCommandContext(t, context.Background(), "bash", []string{helper, pki, "--controlplane"}, nil)
	require.NoError(t, err, "%s", out)
	serials := map[string]bool{}
	keys := map[string]bool{}
	for _, tc := range []struct {
		name, cn string
		groups   []string
	}{
		{"admin", "kubebrain-test-admin", []string{"system:masters"}},
		{"controller-manager", "system:kube-controller-manager", nil},
		{"scheduler", "system:kube-scheduler", nil},
	} {
		data, err := os.ReadFile(filepath.Join(pki, tc.name+".crt"))
		require.NoError(t, err)
		block, _ := pem.Decode(data)
		require.NotNil(t, block)
		cert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		require.Equal(t, tc.cn, cert.Subject.CommonName)
		require.Equal(t, tc.groups, cert.Subject.Organization)
		require.False(t, cert.IsCA)
		require.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, cert.ExtKeyUsage)
		require.Empty(t, cert.IPAddresses)
		require.Empty(t, cert.DNSNames)
		require.False(t, serials[cert.SerialNumber.String()], "distinct certificate serials")
		serials[cert.SerialNumber.String()] = true
		publicKey := string(cert.RawSubjectPublicKeyInfo)
		require.False(t, keys[publicKey], "components must not share a private key")
		keys[publicKey] = true
		keyFile := filepath.Join(pki, tc.name+".key")
		info, err := os.Stat(keyFile)
		require.NoError(t, err)
		require.Zero(t, info.Mode().Perm()&0077)
		out, err := runCompatCommandContext(t, context.Background(), "openssl", []string{"verify", "-CAfile", filepath.Join(pki, "ca.crt"), "-purpose", "sslclient", filepath.Join(pki, tc.name+".crt")}, nil)
		require.NoError(t, err, "%s", out)
		key, err := runCompatCommandContext(t, context.Background(), "openssl", []string{"pkey", "-in", keyFile, "-pubout", "-outform", "DER"}, nil)
		require.NoError(t, err)
		require.Equal(t, cert.RawSubjectPublicKeyInfo, key)
	}
}

func TestEphemeralPKIRejectsUnknownMode(t *testing.T) {
	for _, args := range [][]string{{"--unknown"}, {"--controlplane", "extra"}} {
		pki := filepath.Join(t.TempDir(), "pki")
		command := append([]string{filepath.Join("..", "dev", "create-apiserver-test-pki.sh"), pki}, args...)
		out, err := runCompatCommandContext(t, context.Background(), "bash", command, nil)
		require.Error(t, err, "%s", out)
		require.NoDirExists(t, pki, "invalid invocation must not create credentials")
	}
}

func TestEphemeralAPIServerPKI(t *testing.T) {
	pki := filepath.Join(t.TempDir(), "pki")
	helper := filepath.Join("..", "dev", "create-apiserver-test-pki.sh")
	run := func(command string, args ...string) []byte {
		t.Helper()
		out, err := runCompatCommandContext(t, context.Background(), command, args, nil)
		require.NoError(t, err, "%s", out)
		return out
	}
	run("bash", helper, pki)
	for _, name := range []string{"admin", "controller-manager", "scheduler"} {
		require.NoFileExists(t, filepath.Join(pki, name+".key"), "default mode must not create extra credentials")
	}
	info, err := os.Stat(pki)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	for _, name := range []string{"ca", "front-proxy-ca", "apiserver", "apiserver-kubelet-client", "front-proxy-client", "sa"} {
		info, err := os.Stat(filepath.Join(pki, name+".key"))
		require.NoError(t, err)
		require.Zero(t, info.Mode().Perm()&0077, "private key is accessible outside owner")
	}
	for _, tc := range []struct{ leaf, ca, purpose string }{
		{"apiserver", "ca", "sslserver"},
		{"apiserver-kubelet-client", "ca", "sslclient"},
		{"front-proxy-client", "front-proxy-ca", "sslclient"},
	} {
		run("openssl", "verify", "-CAfile", filepath.Join(pki, tc.ca+".crt"), "-purpose", tc.purpose, filepath.Join(pki, tc.leaf+".crt"))
		key := run("openssl", "pkey", "-in", filepath.Join(pki, tc.leaf+".key"), "-pubout")
		cert := run("openssl", "x509", "-in", filepath.Join(pki, tc.leaf+".crt"), "-pubkey", "-noout")
		require.Equal(t, key, cert)
	}
	run("openssl", "verify", "-CAfile", filepath.Join(pki, "ca.crt"), "-verify_ip", "127.0.0.1", filepath.Join(pki, "apiserver.crt"))
	_, err = runCompatCommandContext(t, context.Background(), "openssl", []string{"verify", "-CAfile", filepath.Join(pki, "ca.crt"), "-verify_ip", "10.32.32.66", filepath.Join(pki, "apiserver.crt")}, nil)
	require.Error(t, err, "loopback certificate must not impersonate the real cluster")
	_, err = runCompatCommandContext(t, context.Background(), "openssl", []string{"verify", "-CAfile", filepath.Join(pki, "ca.crt"), filepath.Join(pki, "front-proxy-client.crt")}, nil)
	require.Error(t, err, "front proxy must have a separate CA")
	before, err := os.ReadFile(filepath.Join(pki, "ca.crt"))
	require.NoError(t, err)
	_, err = runCompatCommandContext(t, context.Background(), "bash", []string{helper, pki}, nil)
	require.Error(t, err, "must refuse existing PKI")
	after, err := os.ReadFile(filepath.Join(pki, "ca.crt"))
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = runCompatCommandContext(t, context.Background(), "bash", []string{helper, "relative-pki"}, nil)
	require.Error(t, err)
}

func TestEphemeralAPIServerRequiresExplicitBinary(t *testing.T) {
	for _, script := range []string{"apiserver-smoke.sh", "apiserver-watch-soak.sh"} {
		t.Run(script, func(t *testing.T) {
			root := t.TempDir()
			out, err := runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "dev", script)}, []string{
				"APISERVER_PKI_MODE=ephemeral", "APISERVER_BIN=/nonexistent/kube-apiserver",
				"ALLOW_MUTATING_APISERVER_WATCH_SOAK=true",
				"ALLOW_MUTATING_APISERVER_SMOKE=true", "ETCD_PREFIX=/registry-kubebrain-apiserver-pki-test",
				"RUN_ID=pki-test", "WORK_ROOT=" + root, "PORT_LOCK_ROOT=" + filepath.Join(root, "locks"),
			})
			require.Error(t, err)
			require.Contains(t, string(out), "ephemeral PKI requires an explicit executable APISERVER_BIN")
			require.NotContains(t, string(out), "missing required command: docker")
		})
	}
}
