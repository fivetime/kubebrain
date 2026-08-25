package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyBindsSignedExportToMaterialAndIdentity(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	material := []byte("private-key-material")
	now := time.Unix(2_000_000_000, 0)
	receipt := signedReceipt(t, privateKey, material, "change-1", "instance-a", "kms/prod/jwt/versions/42", now)
	require.NoError(t, verify(receipt, publicKey, material, "change-1", "instance-a", "kms/prod/jwt/versions/42", now))

	for _, tc := range []struct {
		name                       string
		data, material             []byte
		request, instance, version string
		now                        time.Time
	}{
		{name: "material", data: receipt, material: []byte("other"), request: "change-1", instance: "instance-a", version: "kms/prod/jwt/versions/42", now: now},
		{name: "request", data: receipt, material: material, request: "change-2", instance: "instance-a", version: "kms/prod/jwt/versions/42", now: now},
		{name: "version", data: receipt, material: material, request: "change-1", instance: "instance-a", version: "kms/prod/jwt/versions/43", now: now},
		{name: "expired", data: receipt, material: material, request: "change-1", instance: "instance-a", version: "kms/prod/jwt/versions/42", now: now.Add(6 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, verify(tc.data, publicKey, tc.material, tc.request, tc.instance, tc.version, tc.now))
		})
	}

	mutated := append([]byte(nil), receipt...)
	mutated[len(mutated)-2] ^= 1
	require.Error(t, verify(mutated, publicKey, material, "change-1", "instance-a", "kms/prod/jwt/versions/42", now))
}

func TestVerifyRejectsNonCanonicalOrUnknownPayload(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	payload := []byte(`{"request_id":"change-1","format":"kubebrain.jwt-kms-export.v1","instance":"instance-a","version_id":"kms/v/2","material_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","state":"enabled","exported_at_unix":1999999999,"expires_at_unix":2000000300}`)
	receipt := envelopeBytes(t, privateKey, payload)
	require.ErrorContains(t, verify(receipt, publicKey, []byte("material"), "change-1", "instance-a", "kms/v/2", time.Unix(2_000_000_000, 0)), "canonical")

	var values map[string]any
	require.NoError(t, json.Unmarshal(payload, &values))
	values["extra"] = true
	unknown, err := json.Marshal(values)
	require.NoError(t, err)
	require.ErrorContains(t, verify(envelopeBytes(t, privateKey, unknown), publicKey, []byte("material"), "change-1", "instance-a", "kms/v/2", time.Unix(2_000_000_000, 0)), "schema")
}

func TestVerifyFilesEnforcesPEMAndFilesystemBoundary(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	dir := t.TempDir()
	materialPath, receiptPath, publicKeyPath := filepath.Join(dir, "material"), filepath.Join(dir, "receipt.json"), filepath.Join(dir, "provider.pem")
	require.NoError(t, os.WriteFile(materialPath, []byte("private-key-material"), 0o600))
	require.NoError(t, os.WriteFile(receiptPath, signedReceipt(t, privateKey, []byte("private-key-material"), "change-1", "instance-a", "kms/v/42", now), 0o600))
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(publicKeyPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o644))
	require.NoError(t, verifyFiles(receiptPath, publicKeyPath, materialPath, "change-1", "instance-a", "kms/v/42", now))

	symlink := filepath.Join(dir, "receipt-link")
	require.NoError(t, os.Symlink(receiptPath, symlink))
	require.Error(t, verifyFiles(symlink, publicKeyPath, materialPath, "change-1", "instance-a", "kms/v/42", now))
	require.NoError(t, os.Chmod(materialPath, 0o640))
	require.ErrorContains(t, verifyFiles(receiptPath, publicKeyPath, materialPath, "change-1", "instance-a", "kms/v/42", now), "permissions")
}

func signedReceipt(t *testing.T, privateKey ed25519.PrivateKey, material []byte, request, instance, version string, now time.Time) []byte {
	sum := sha256.Sum256(material)
	payload, err := json.Marshal(exportReceipt{Format: "kubebrain.jwt-kms-export.v1", RequestID: request, Instance: instance, VersionID: version, MaterialSHA256: hex.EncodeToString(sum[:]), State: "enabled", ExportedAtUnix: now.Unix() - 1, ExpiresAtUnix: now.Unix() + 300})
	require.NoError(t, err)
	return envelopeBytes(t, privateKey, payload)
}

func envelopeBytes(t *testing.T, privateKey ed25519.PrivateKey, payload []byte) []byte {
	data, err := json.Marshal(envelope{Format: "kubebrain.jwt-kms-export-envelope.v1", Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))})
	require.NoError(t, err)
	return data
}
