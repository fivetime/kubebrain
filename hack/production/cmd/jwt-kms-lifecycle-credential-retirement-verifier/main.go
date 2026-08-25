// Command jwt-kms-lifecycle-credential-retirement-verifier verifies external
// identity-provider proof that an old lifecycle credential has been revoked.
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
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type envelope struct {
	Format    string `json:"format"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}
type retirementReceipt struct {
	ExpiresAtUnix               int64  `json:"expires_at_unix"`
	Format                      string `json:"format"`
	Principal                   string `json:"principal"`
	ReplacementCredentialID     string `json:"replacement_credential_id"`
	ReplacementReadinessSHA256  string `json:"replacement_readiness_receipt_sha256"`
	ReplacementSecretDataSHA256 string `json:"replacement_secret_data_sha256"`
	ReplacementSecretUID        string `json:"replacement_secret_uid"`
	RetiredCredentialID         string `json:"retired_credential_id"`
	RetiredSecretDataSHA256     string `json:"retired_secret_data_sha256"`
	RetiredSecretUID            string `json:"retired_secret_uid"`
	RevokedAtUnix               int64  `json:"revoked_at_unix"`
	State                       string `json:"state"`
}
type retirementArtifact struct {
	ExpiresAtUnix               int64  `json:"expires_at_unix"`
	Format                      string `json:"format"`
	Principal                   string `json:"principal"`
	ReplacementCredentialID     string `json:"replacement_credential_id"`
	ReplacementReadinessSHA256  string `json:"replacement_readiness_receipt_sha256"`
	ReplacementSecretDataSHA256 string `json:"replacement_secret_data_sha256"`
	ReplacementSecretUID        string `json:"replacement_secret_uid"`
	RetiredCredentialID         string `json:"retired_credential_id"`
	RetiredSecretDataSHA256     string `json:"retired_secret_data_sha256"`
	RetiredSecretUID            string `json:"retired_secret_uid"`
	RetirementReceiptBase64     string `json:"retirement_receipt_base64"`
	RetirementReceiptSHA256     string `json:"retirement_receipt_sha256"`
	RevokedAtUnix               int64  `json:"revoked_at_unix"`
}

func main() {
	var receipt, key, retiredID, retiredUID, retiredSHA, replacementID, replacementUID, replacementSHA, readinessSHA, artifactOutput string
	flag.StringVar(&receipt, "receipt", "", "provider-signed credential retirement receipt")
	flag.StringVar(&key, "public-key", "", "pinned provider Ed25519 key")
	flag.StringVar(&retiredID, "retired-credential-id", "", "retired credential identifier")
	flag.StringVar(&retiredUID, "retired-secret-uid", "", "retired immutable Secret UID")
	flag.StringVar(&retiredSHA, "retired-secret-data-sha256", "", "retired Secret data SHA-256")
	flag.StringVar(&replacementID, "replacement-credential-id", "", "active replacement credential identifier")
	flag.StringVar(&replacementUID, "replacement-secret-uid", "", "replacement immutable Secret UID")
	flag.StringVar(&replacementSHA, "replacement-secret-data-sha256", "", "replacement Secret data SHA-256")
	flag.StringVar(&readinessSHA, "replacement-readiness-receipt-sha256", "", "active replacement readiness receipt SHA-256")
	flag.StringVar(&artifactOutput, "artifact-output", "", "exclusive canonical retirement artifact output")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("positional arguments are unsupported"))
	}
	if err := verifyFiles(receipt, key, artifactOutput, expected{retiredID, retiredUID, retiredSHA, replacementID, replacementUID, replacementSHA, readinessSHA}, time.Now()); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "JWT KMS lifecycle credential retirement verification failed:", err)
	os.Exit(1)
}

type expected struct {
	retiredID, retiredUID, retiredSHA, replacementID, replacementUID, replacementSHA, readinessSHA string
}

func verifyFiles(receiptPath, keyPath, artifactOutput string, want expected, now time.Time) error {
	data, err := readSecure(receiptPath, 64<<10)
	if err != nil {
		return errors.New("retirement receipt file is invalid")
	}
	keyData, err := readSecure(keyPath, 1<<20)
	if err != nil {
		return errors.New("retirement trust key file is invalid")
	}
	key, err := parseKey(keyData)
	if err != nil {
		return err
	}
	if err := verify(data, key, want, now); err != nil {
		return err
	}
	if artifactOutput == "" {
		return nil
	}
	return writeArtifact(artifactOutput, data, want)
}

func writeArtifact(output string, data []byte, want expected) error {
	var outer envelope
	if err := decodeExact(data, &outer); err != nil {
		return errors.New("cannot decode verified retirement envelope")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(outer.Payload)
	if err != nil {
		return errors.New("cannot decode verified retirement payload")
	}
	var receipt retirementReceipt
	if err := decodeExact(payload, &receipt); err != nil {
		return errors.New("cannot decode verified retirement receipt")
	}
	sum := sha256.Sum256(data)
	artifact := retirementArtifact{Format: "kubebrain.jwt-kms-lifecycle-credential-retirement-artifact.v1", Principal: receipt.Principal, RevokedAtUnix: receipt.RevokedAtUnix, ExpiresAtUnix: receipt.ExpiresAtUnix, RetiredCredentialID: want.retiredID, RetiredSecretUID: want.retiredUID, RetiredSecretDataSHA256: want.retiredSHA, ReplacementCredentialID: want.replacementID, ReplacementSecretUID: want.replacementUID, ReplacementSecretDataSHA256: want.replacementSHA, ReplacementReadinessSHA256: want.readinessSHA, RetirementReceiptSHA256: hex.EncodeToString(sum[:]), RetirementReceiptBase64: base64.StdEncoding.EncodeToString(data)}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		return err
	}
	return writeNoClobber(output, append(encoded, '\n'))
}

func writeNoClobber(path string, data []byte) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("artifact output path is invalid")
	}
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".jwt-kms-retirement-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(temporary, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func verify(data []byte, key ed25519.PublicKey, want expected, now time.Time) error {
	if !validID(want.retiredID, 40) || !validID(want.replacementID, 40) || want.retiredID == want.replacementID || !validID(want.retiredUID, 128) || !validID(want.replacementUID, 128) || !validSHA(want.retiredSHA) || !validSHA(want.replacementSHA) || !validSHA(want.readinessSHA) {
		return errors.New("expected retirement identity is invalid")
	}
	var outer envelope
	if err := decodeExact(data, &outer); err != nil || outer.Format != "kubebrain.jwt-kms-lifecycle-credential-retirement-envelope.v1" {
		return errors.New("retirement envelope is invalid")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(outer.Payload)
	if err != nil || len(payload) == 0 || len(payload) > 64<<10 {
		return errors.New("retirement payload is invalid")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(outer.Signature)
	if err != nil || !ed25519.Verify(key, payload, signature) {
		return errors.New("retirement signature is invalid")
	}
	var receipt retirementReceipt
	if err := decodeExact(payload, &receipt); err != nil {
		return errors.New("retirement payload schema is invalid")
	}
	canonical, _ := json.Marshal(receipt)
	if !bytes.Equal(canonical, payload) {
		return errors.New("retirement payload is not canonical")
	}
	if receipt.Format != "kubebrain.jwt-kms-lifecycle-credential-retirement.v1" || receipt.State != "revoked" || !validPrincipal(receipt.Principal) || receipt.RetiredCredentialID != want.retiredID || receipt.RetiredSecretUID != want.retiredUID || receipt.RetiredSecretDataSHA256 != want.retiredSHA || receipt.ReplacementCredentialID != want.replacementID || receipt.ReplacementSecretUID != want.replacementUID || receipt.ReplacementSecretDataSHA256 != want.replacementSHA || receipt.ReplacementReadinessSHA256 != want.readinessSHA {
		return errors.New("retirement identity, replacement, or state binding is invalid")
	}
	if receipt.RevokedAtUnix <= 0 || receipt.ExpiresAtUnix <= receipt.RevokedAtUnix || receipt.ExpiresAtUnix-receipt.RevokedAtUnix > 900 || now.Unix() < receipt.RevokedAtUnix-60 {
		return errors.New("retirement validity window is invalid")
	}
	return nil
}

func parseKey(data []byte) (ed25519.PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "PUBLIC KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("retirement trust key PEM is invalid")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	key, ok := parsed.(ed25519.PublicKey)
	if err != nil || !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("retirement trust key must be Ed25519")
	}
	return key, nil
}
func decodeExact(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
func readSecure(path string, maximum int64) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("invalid path")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open failed")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("file boundary is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	final, statErr := file.Stat()
	if err != nil || statErr != nil || int64(len(data)) != info.Size() || final.Size() != info.Size() {
		return nil, errors.New("file changed while reading")
	}
	return data, nil
}
func validID(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for i, c := range []byte(value) {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '-' || c == '.' || c == '_' || c == ':')) {
			return false
		}
	}
	return true
}
func validSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func validPrincipal(value string) bool {
	if len(value) == 0 || len(value) > 512 {
		return false
	}
	for _, char := range value {
		if char <= ' ' || char == 0x7f {
			return false
		}
	}
	return true
}
