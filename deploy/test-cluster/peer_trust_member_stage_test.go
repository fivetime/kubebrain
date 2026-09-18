package testcluster_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func memberStageFixture(t *testing.T, dir string, reuseKey bool) map[string]any {
	t.Helper()
	ca, caKey := trustMaterialFixture(t, dir, false, false, false)
	read := func(path string) []byte { data, err := os.ReadFile(path); require.NoError(t, err); return data }
	write := func(path string, data []byte) { require.NoError(t, os.WriteFile(path, data, 0600)) }
	bundle := filepath.Join(dir, "bundle")
	require.NoError(t, os.Mkdir(bundle, 0700))
	client := filepath.Join(dir, "client")
	require.NoError(t, os.Mkdir(client, 0700))
	for dst, src := range map[string]string{"ca.crt": "ca.crt", "probe.crt": "tls.crt", "probe.key": "tls.key"} {
		write(filepath.Join(client, dst), read(filepath.Join(dir, "original", src)))
	}
	pins := map[string][]string{}
	holders := []string{}
	var firstKey *ecdsa.PrivateKey
	now := time.Now()
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("kubebrain-local-%d", i)
		member := filepath.Join(bundle, name)
		require.NoError(t, os.Mkdir(member, 0700))
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		if i == 0 {
			firstKey = key
		} else if reuseKey {
			key = firstKey
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 100)), Subject: pkix.Name{CommonName: name}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(12 * time.Hour), DNSNames: []string{"peer.test", name + ".peer.test"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		write(filepath.Join(member, "tls.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey})
		if reuseKey {
			// Distinct PEM bytes must not hide the same underlying public key.
			keyPEM = append(keyPEM, []byte(strings.Repeat("\n", i))...)
		}
		write(filepath.Join(member, "tls.key"), keyPEM)
		write(filepath.Join(member, "ca.crt"), read(filepath.Join(dir, "member", "ca.crt")))
		cert, err := x509.ParseCertificate(der)
		require.NoError(t, err)
		hash := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
		holder := name + ".peer.test:3380"
		holders = append(holders, holder)
		pins[holder] = []string{hex.EncodeToString(hash[:])}
	}
	scope := "retirement-v1:" + strings.Repeat("a", 64)
	memberData := map[string]any{}
	for i, holder := range holders {
		targets := map[string]string{}
		for _, other := range holders {
			if other != holder {
				targets["https://"+other] = other
			}
		}
		policy, err := json.Marshal(map[string]any{"scope": scope, "holder_pins": pins, "endpoint_holders": targets, "read_budget": "1s", "operation_budget": "1s", "send_budget": "1s", "concurrency": 2, "requests_per_second": 4})
		require.NoError(t, err)
		name := fmt.Sprintf("kubebrain-local-%d", i)
		write(filepath.Join(bundle, name, "policy.json"), policy)
		for _, file := range []string{"tls.crt", "tls.key", "policy.json"} {
			memberData[name+"."+file] = base64.StdEncoding.EncodeToString(read(filepath.Join(bundle, name, file)))
		}
		memberData[name+".ca.crt"] = base64.StdEncoding.EncodeToString(read(filepath.Join(dir, "expanded", "ca.crt")))
	}
	receipt := memberTrustInput(t)
	trustMap(receipt, "member_secret")["data"] = memberData
	for target, source := range map[string]string{"original_secret": "original", "expanded_secret": "expanded"} {
		data := map[string]any{}
		for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
			data[file] = base64.StdEncoding.EncodeToString(read(filepath.Join(dir, source, file)))
		}
		trustMap(receipt, target)["data"] = data
	}
	checker, err := filepath.Abs("verify-peer-trust-material.sh")
	require.NoError(t, err)
	hash := sha256.Sum256(read(checker))
	receipt["verification"] = map[string]any{"bundle_dir": bundle, "client_tls_dir": client, "material_verifier": checker, "material_verifier_sha256": hex.EncodeToString(hash[:]), "peer_dns": "peer.test", "client_dns": "client.test", "cluster_id": "42", "retirement_scope": scope}
	return receipt
}

func TestPeerTrustMembersPreflightBindsCryptographicIdentities(t *testing.T) {
	for _, scenario := range []string{"expand", "restore", "reused-spki", "wrong-ca", "wrong-policy-pin", "wrong-policy-scope", "self-target", "wrong-budget", "mismatched-secret", "extra-file"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			receipt := memberStageFixture(t, dir, scenario == "reused-spki")
			data := trustMap(receipt, "member_secret", "data")
			switch scenario {
			case "wrong-ca":
				data["kubebrain-local-0.ca.crt"] = trustMap(receipt, "original_secret", "data")["ca.crt"]
			case "mismatched-secret":
				data["kubebrain-local-0.tls.key"] = data["kubebrain-local-1.tls.key"]
			case "extra-file":
				data["ca.key"] = "private"
			case "wrong-policy-pin", "wrong-policy-scope", "self-target", "wrong-budget":
				file := filepath.Join(dir, "bundle", "kubebrain-local-0", "policy.json")
				raw, err := os.ReadFile(file)
				require.NoError(t, err)
				var policy map[string]any
				require.NoError(t, json.Unmarshal(raw, &policy))
				switch scenario {
				case "wrong-policy-pin":
					trustMap(policy, "holder_pins")["kubebrain-local-0.peer.test:3380"] = []string{strings.Repeat("0", 64)}
				case "wrong-policy-scope":
					policy["scope"] = "retirement-v1:" + strings.Repeat("b", 64)
				case "self-target":
					trustMap(policy, "endpoint_holders")["https://kubebrain-local-0.peer.test:3380"] = "kubebrain-local-0.peer.test:3380"
				case "wrong-budget":
					policy["concurrency"] = 64
				}
				raw, err = json.Marshal(policy)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(file, raw, 0600))
				data["kubebrain-local-0.policy.json"] = base64.StdEncoding.EncodeToString(raw)
			}
			raw, err := json.Marshal(receipt)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "receipt.json"), raw, 0600))
			mode := "expand"
			if scenario == "restore" {
				mode = "restore"
			}
			marker := filepath.Join(dir, "unexpected-api")
			require.NoError(t, os.WriteFile(filepath.Join(dir, "kubectl"), []byte("#!/bin/sh\ntouch \"$UNEXPECTED_API\"\nexit 77\n"), 0700))
			cmd := exec.Command("bash", "verify-peer-trust-stage.sh", mode, "preflight", dir, "/unused/kubeconfig", "test")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "UNEXPECTED_API="+marker)
			output, err := cmd.CombinedOutput()
			if scenario == "expand" || scenario == "restore" {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.NoFileExists(t, marker)
			require.NotContains(t, string(output), "BEGIN PRIVATE KEY")
		})
	}
}
