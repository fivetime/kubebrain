package testcluster_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func trustMaterialFixture(t *testing.T, root string, shortLife, wrongPurpose, reuseKey bool) {
	t.Helper()
	now := time.Now()
	var originalKey *ecdsa.PrivateKey
	for i, name := range []string{"original", "member"} {
		dir := filepath.Join(root, name)
		require.NoError(t, os.Mkdir(dir, 0700))
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		ca := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 1)), Subject: pkix.Name{CommonName: name}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
		caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
		require.NoError(t, err)
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		if name == "original" {
			originalKey = leafKey
		} else if reuseKey {
			leafKey = originalKey
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 10)), Subject: pkix.Name{CommonName: name + "-peer"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(12 * time.Hour), DNSNames: []string{"peer.test"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		if name == "member" && shortLife {
			leaf.NotAfter = now.Add(30 * time.Minute)
		}
		if name == "member" && wrongPurpose {
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
		require.NoError(t, err)
		keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
		require.NoError(t, err)
		for file, data := range map[string][]byte{
			"ca.crt":  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
			"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
			"tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, file), data, 0600))
		}
	}
	require.NoError(t, os.Mkdir(filepath.Join(root, "expanded"), 0700))
	for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
		data, err := os.ReadFile(filepath.Join(root, "original", file))
		require.NoError(t, err)
		if file == "ca.crt" {
			extra, err := os.ReadFile(filepath.Join(root, "member", file))
			require.NoError(t, err)
			data = append(data, extra...)
		}
		require.NoError(t, os.WriteFile(filepath.Join(root, "expanded", file), data, 0600))
	}
}

func TestPeerTrustMaterialCheck(t *testing.T) {
	for _, scenario := range []string{"valid", "blank-lines", "wrong-dns", "short-life", "wrong-purpose", "wrong-key", "reused-member-key", "changed-old-leaf", "missing-old-root", "extra-root", "private-key-in-roots", "same-authority"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			trustMaterialFixture(t, root, scenario == "short-life", scenario == "wrong-purpose", scenario == "reused-member-key")
			read := func(dir, file string) []byte {
				data, err := os.ReadFile(filepath.Join(root, dir, file))
				require.NoError(t, err)
				return data
			}
			write := func(dir, file string, data []byte) {
				require.NoError(t, os.WriteFile(filepath.Join(root, dir, file), data, 0600))
			}
			dns := "peer.test"
			switch scenario {
			case "blank-lines":
				write("expanded", "ca.crt", append(append(read("original", "ca.crt"), '\n'), append(read("member", "ca.crt"), '\n')...))
			case "wrong-dns":
				dns = "wrong.test"
			case "wrong-key":
				write("member", "tls.key", read("original", "tls.key"))
			case "changed-old-leaf":
				write("expanded", "tls.crt", read("member", "tls.crt"))
			case "missing-old-root":
				write("expanded", "ca.crt", read("member", "ca.crt"))
			case "extra-root":
				write("expanded", "ca.crt", append(read("expanded", "ca.crt"), read("original", "ca.crt")...))
			case "private-key-in-roots":
				write("expanded", "ca.crt", append(read("expanded", "ca.crt"), read("member", "tls.key")...))
			case "same-authority":
				for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
					write("member", file, read("original", file))
				}
				write("expanded", "ca.crt", append(read("original", "ca.crt"), read("original", "ca.crt")...))
			}
			cmd := exec.Command("bash", "verify-peer-trust-material.sh", filepath.Join(root, "original"), filepath.Join(root, "expanded"), filepath.Join(root, "member"), dns)
			output, err := cmd.CombinedOutput()
			if scenario == "valid" || scenario == "blank-lines" {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "OFFLINE_PEER_TRUST_MATERIAL_OK")
			} else {
				require.Error(t, err, string(output))
			}
			require.NotContains(t, string(output), "BEGIN PRIVATE KEY")
		})
	}
}
