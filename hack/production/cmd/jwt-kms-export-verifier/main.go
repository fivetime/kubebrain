// Command jwt-kms-export-verifier verifies a short-lived, provider-signed JWT
// key export receipt and binds it to the exact material before any Kubernetes
// object is created.
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
	"unicode"

	"golang.org/x/sys/unix"
)

const (
	maxReceiptBytes    = 64 << 10
	maxMaterialBytes   = 512 << 10
	maxPublicKeyBytes  = 1 << 20
	maxReceiptLifetime = 15 * time.Minute
	clockSkew          = time.Minute
)

type envelope struct {
	Format    string `json:"format"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

type exportReceipt struct {
	Format         string `json:"format"`
	RequestID      string `json:"request_id"`
	Instance       string `json:"instance"`
	VersionID      string `json:"version_id"`
	MaterialSHA256 string `json:"material_sha256"`
	State          string `json:"state"`
	ExportedAtUnix int64  `json:"exported_at_unix"`
	ExpiresAtUnix  int64  `json:"expires_at_unix"`
}

func main() {
	var receiptPath, publicKeyPath, materialPath, requestID, instance, versionID string
	flag.StringVar(&receiptPath, "receipt", "", "provider-signed export receipt")
	flag.StringVar(&publicKeyPath, "public-key", "", "pinned Ed25519 provider public key PEM")
	flag.StringVar(&materialPath, "material", "", "exported JWT signing material")
	flag.StringVar(&requestID, "request-id", "", "expected external rotation request")
	flag.StringVar(&instance, "instance", "", "expected DBaaS instance")
	flag.StringVar(&versionID, "version-id", "", "expected immutable KMS version")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("positional arguments are not supported"))
	}
	if err := verifyFiles(receiptPath, publicKeyPath, materialPath, requestID, instance, versionID, time.Now()); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "JWT KMS export receipt verification failed:", err)
	os.Exit(1)
}

func verifyFiles(receiptPath, publicKeyPath, materialPath, requestID, instance, versionID string, now time.Time) error {
	if !validIdentity(requestID, 128) || !validIdentity(instance, 128) || !validVersion(versionID) {
		return errors.New("expected request, instance, or version is invalid")
	}
	receiptData, err := readBoundedRegular(receiptPath, maxReceiptBytes, 0o022)
	if err != nil {
		return fmt.Errorf("read receipt: %w", err)
	}
	publicKeyData, err := readBoundedRegular(publicKeyPath, maxPublicKeyBytes, 0o022)
	if err != nil {
		return fmt.Errorf("read public key: %w", err)
	}
	material, err := readBoundedRegular(materialPath, maxMaterialBytes, 0o077)
	if err != nil {
		return fmt.Errorf("read material: %w", err)
	}
	publicKey, err := parsePublicKey(publicKeyData)
	if err != nil {
		return err
	}
	return verify(receiptData, publicKey, material, requestID, instance, versionID, now)
}

func verify(data []byte, publicKey ed25519.PublicKey, material []byte, requestID, instance, versionID string, now time.Time) error {
	var outer envelope
	if err := decodeExact(data, &outer); err != nil || outer.Format != "kubebrain.jwt-kms-export-envelope.v1" {
		return errors.New("receipt envelope is invalid")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(outer.Payload)
	if err != nil || len(payload) == 0 || len(payload) > maxReceiptBytes {
		return errors.New("receipt payload is invalid")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(outer.Signature)
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("receipt signature is invalid")
	}
	var receipt exportReceipt
	if err := decodeExact(payload, &receipt); err != nil {
		return errors.New("receipt payload schema is invalid")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(canonical, payload) {
		return errors.New("receipt payload is not canonical JSON")
	}
	if receipt.Format != "kubebrain.jwt-kms-export.v1" || receipt.RequestID != requestID || receipt.Instance != instance || receipt.VersionID != versionID || receipt.State != "enabled" {
		return errors.New("receipt identity or state does not match")
	}
	if receipt.ExportedAtUnix <= 0 || receipt.ExpiresAtUnix <= receipt.ExportedAtUnix || receipt.ExpiresAtUnix-receipt.ExportedAtUnix > int64(maxReceiptLifetime/time.Second) {
		return errors.New("receipt validity window is invalid")
	}
	nowUnix := now.Unix()
	if nowUnix < receipt.ExportedAtUnix-int64(clockSkew/time.Second) || nowUnix > receipt.ExpiresAtUnix {
		return errors.New("receipt is not currently valid")
	}
	sum := sha256.Sum256(material)
	if receipt.MaterialSHA256 != hex.EncodeToString(sum[:]) {
		return errors.New("receipt material digest does not match")
	}
	return nil
}

func decodeExact(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func parsePublicKey(data []byte) (ed25519.PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 || block.Type != "PUBLIC KEY" {
		return nil, errors.New("provider public key PEM is invalid")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errors.New("provider public key is invalid")
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("provider public key must be Ed25519")
	}
	return key, nil
}

func readBoundedRegular(path string, maximum int64, forbiddenPermissions os.FileMode) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("path must be canonical and absolute")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("file must be a bounded regular non-symlink file")
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("file could not be opened")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("file must be a bounded regular non-symlink file")
	}
	if info.Mode().Perm()&forbiddenPermissions != 0 {
		return nil, errors.New("file permissions violate the receipt trust boundary")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	finalInfo, statErr := file.Stat()
	if err != nil || statErr != nil || int64(len(data)) != info.Size() || finalInfo.Size() != info.Size() || int64(len(data)) > maximum {
		return nil, errors.New("file could not be read consistently")
	}
	return data, nil
}

func validIdentity(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, char := range value {
		if unicode.IsSpace(char) || unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func validVersion(value string) bool {
	if len(value) == 0 || len(value) > 512 {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || (index > 0 && bytes.ContainsRune([]byte("._:/@+=-"), rune(char))) {
			continue
		}
		return false
	}
	return true
}
