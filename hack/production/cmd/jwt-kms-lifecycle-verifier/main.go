// Command jwt-kms-lifecycle-verifier verifies provider-signed promotion and
// revocation receipts against one completed JWT rotation operation receipt.
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

	"golang.org/x/sys/unix"
)

const maxInputBytes = 1 << 20
const maxLifecycleValidity = 15 * time.Minute
const lifecycleClockSkew = time.Minute

type envelope struct {
	Format    string `json:"format"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}
type operationReceipt struct {
	Attempt          int64  `json:"attempt"`
	CompletedAtUnix  int64  `json:"completed_at_unix"`
	Format           string `json:"format"`
	Instance         string `json:"instance"`
	NewKeyVersionID  string `json:"new_key_version_id"`
	OldKeyVersionID  string `json:"old_key_version_id"`
	OperationID      string `json:"operation_id"`
	OperationUID     string `json:"operation_uid"`
	ParametersSHA256 string `json:"parameters_sha256"`
	PhaseAGateSHA    string `json:"phase_a_gate_receipt_sha256"`
	PhaseAPublishSHA string `json:"phase_a_publish_receipt_sha256"`
	PhaseBGateSHA    string `json:"phase_b_gate_receipt_sha256"`
	PhaseBPublishSHA string `json:"phase_b_publish_receipt_sha256"`
	PhaseCGateSHA    string `json:"phase_c_gate_receipt_sha256"`
	PhaseCPublishSHA string `json:"phase_c_publish_receipt_sha256"`
	RequestID        string `json:"request_id"`
}
type lifecycleReceipt struct {
	Action                 string `json:"action"`
	ExpiresAtUnix          int64  `json:"expires_at_unix"`
	Format                 string `json:"format"`
	Instance               string `json:"instance"`
	NewVersionID           string `json:"new_version_id"`
	ObservedAtUnix         int64  `json:"observed_at_unix"`
	OldVersionID           string `json:"old_version_id"`
	OperationID            string `json:"operation_id"`
	OperationReceiptSHA256 string `json:"operation_receipt_sha256"`
	PreviousReceiptSHA256  string `json:"previous_receipt_sha256"`
	RequestID              string `json:"request_id"`
	State                  string `json:"state"`
}

type lifecycleArtifact struct {
	Format                  string `json:"format"`
	RequestID               string `json:"request_id"`
	OperationID             string `json:"operation_id"`
	OperationUID            string `json:"operation_uid"`
	Instance                string `json:"instance"`
	OldVersionID            string `json:"old_version_id"`
	NewVersionID            string `json:"new_version_id"`
	CompletedAtUnix         int64  `json:"completed_at_unix"`
	PromotedAtUnix          int64  `json:"promoted_at_unix"`
	RevokedAtUnix           int64  `json:"revoked_at_unix"`
	OperationReceiptSHA256  string `json:"operation_receipt_sha256"`
	PromotionReceiptSHA256  string `json:"promotion_receipt_sha256"`
	RevocationReceiptSHA256 string `json:"revocation_receipt_sha256"`
	OperationReceiptBase64  string `json:"operation_receipt_base64"`
	PromotionReceiptBase64  string `json:"promotion_receipt_base64"`
	RevocationReceiptBase64 string `json:"revocation_receipt_base64"`
}

func main() {
	var action, receiptPath, publicKeyPath, operationPath, previousPath, artifactOutput string
	flag.StringVar(&action, "action", "", "promote or revoke")
	flag.StringVar(&receiptPath, "receipt", "", "signed lifecycle receipt")
	flag.StringVar(&publicKeyPath, "public-key", "", "pinned provider Ed25519 public key PEM")
	flag.StringVar(&operationPath, "operation-receipt", "", "completed JWT rotation v3 receipt")
	flag.StringVar(&previousPath, "previous-receipt", "", "verified promotion receipt required by revoke")
	flag.StringVar(&artifactOutput, "artifact-output", "", "exclusive canonical lifecycle evidence output; valid only for revoke")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("positional arguments are not supported"))
	}
	if err := verifyFiles(action, receiptPath, publicKeyPath, operationPath, previousPath, artifactOutput, time.Now()); err != nil {
		fatal(err)
	}
}
func fatal(err error) {
	fmt.Fprintln(os.Stderr, "JWT KMS lifecycle receipt verification failed:", err)
	os.Exit(1)
}

func verifyFiles(action, receiptPath, publicKeyPath, operationPath, previousPath, artifactOutput string, now time.Time) error {
	if artifactOutput != "" && action != "revoke" {
		return errors.New("artifact output is valid only for revoke verification")
	}
	receipt, err := readFile(receiptPath, 64<<10, 0o022)
	if err != nil {
		return fmt.Errorf("read lifecycle receipt: %w", err)
	}
	keyData, err := readFile(publicKeyPath, maxInputBytes, 0o022)
	if err != nil {
		return fmt.Errorf("read public key: %w", err)
	}
	operation, err := readFile(operationPath, maxInputBytes, 0o022)
	if err != nil {
		return fmt.Errorf("read operation receipt: %w", err)
	}
	previous := []byte(nil)
	if previousPath != "" {
		previous, err = readFile(previousPath, 64<<10, 0o022)
		if err != nil {
			return fmt.Errorf("read previous receipt: %w", err)
		}
	}
	key, err := parsePublicKey(keyData)
	if err != nil {
		return err
	}
	if err := verify(action, receipt, key, operation, previous, now); err != nil {
		return err
	}
	if artifactOutput == "" {
		return nil
	}
	return writeLifecycleArtifact(artifactOutput, operation, previous, receipt, key)
}

func writeLifecycleArtifact(output string, operationData, promotionData, revocationData []byte, key ed25519.PublicKey) error {
	if !filepath.IsAbs(output) || filepath.Clean(output) != output {
		return errors.New("artifact output path must be canonical and absolute")
	}
	var operation operationReceipt
	var promotion, revocation lifecycleReceipt
	if err := decodeCanonical(operationData, &operation); err != nil {
		return errors.New("operation receipt is not canonical")
	}
	if err := decodeSigned(promotionData, key, &promotion); err != nil {
		return errors.New("promotion receipt cannot be embedded")
	}
	if err := decodeSigned(revocationData, key, &revocation); err != nil {
		return errors.New("revocation receipt cannot be embedded")
	}
	digest := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
	artifact := lifecycleArtifact{
		Format: "kubebrain.jwt-kms-lifecycle-artifact.v1", RequestID: operation.RequestID,
		OperationID: operation.OperationID, OperationUID: operation.OperationUID, Instance: operation.Instance,
		OldVersionID: operation.OldKeyVersionID, NewVersionID: operation.NewKeyVersionID,
		CompletedAtUnix: operation.CompletedAtUnix, PromotedAtUnix: promotion.ObservedAtUnix, RevokedAtUnix: revocation.ObservedAtUnix,
		OperationReceiptSHA256: digest(operationData), PromotionReceiptSHA256: digest(promotionData), RevocationReceiptSHA256: digest(revocationData),
		OperationReceiptBase64:  base64.StdEncoding.EncodeToString(operationData),
		PromotionReceiptBase64:  base64.StdEncoding.EncodeToString(promotionData),
		RevocationReceiptBase64: base64.StdEncoding.EncodeToString(revocationData),
	}
	data, err := json.Marshal(artifact)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	fd, err := unix.Open(output, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create lifecycle artifact without replacement: %w", err)
	}
	file := os.NewFile(uintptr(fd), output)
	if file == nil {
		_ = unix.Close(fd)
		_ = os.Remove(output)
		return errors.New("create lifecycle artifact failed")
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(output)
		}
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func verify(action string, data []byte, publicKey ed25519.PublicKey, operationData, previousData []byte, now time.Time) error {
	if action != "promote" && action != "revoke" {
		return errors.New("action must be promote or revoke")
	}
	var operation operationReceipt
	if err := decodeCanonical(operationData, &operation); err != nil || operation.Format != "kubebrain.jwt-key-rotation.operation.receipt.v3" {
		return errors.New("operation receipt is not canonical v3")
	}
	if operation.Attempt <= 0 || operation.Attempt > 5 || operation.CompletedAtUnix <= 0 || !validRequestID(operation.RequestID) || !validOperationID(operation.OperationID) || !validInstance(operation.Instance) || !validVersion(operation.OldKeyVersionID) || !validVersion(operation.NewKeyVersionID) || operation.OldKeyVersionID == operation.NewKeyVersionID || operation.OperationUID == "" || len(operation.OperationUID) > 253 {
		return errors.New("operation receipt identity is invalid")
	}
	for _, digest := range []string{operation.ParametersSHA256, operation.PhaseAGateSHA, operation.PhaseAPublishSHA, operation.PhaseBGateSHA, operation.PhaseBPublishSHA, operation.PhaseCGateSHA, operation.PhaseCPublishSHA} {
		if !validSHA(digest) {
			return errors.New("operation receipt digest is invalid")
		}
	}
	operationSum := sha256.Sum256(operationData)
	operationSHA := hex.EncodeToString(operationSum[:])
	previousSHA := ""
	var previous lifecycleReceipt
	if action == "promote" {
		if len(previousData) != 0 {
			return errors.New("promotion must not have a previous lifecycle receipt")
		}
	} else {
		if len(previousData) == 0 {
			return errors.New("revocation requires the promotion receipt")
		}
		previousSum := sha256.Sum256(previousData)
		previousSHA = hex.EncodeToString(previousSum[:])
		if err := decodeSigned(previousData, publicKey, &previous); err != nil || previous.Action != "promote" || previous.State != "new-primary" || previous.OperationReceiptSHA256 != operationSHA || previous.RequestID != operation.RequestID || previous.OperationID != operation.OperationID || previous.Instance != operation.Instance || previous.OldVersionID != operation.OldKeyVersionID || previous.NewVersionID != operation.NewKeyVersionID {
			return errors.New("previous promotion receipt is invalid")
		}
		if previous.Format != "kubebrain.jwt-kms-lifecycle.v1" || previous.PreviousReceiptSHA256 != "" || previous.ObservedAtUnix <= operation.CompletedAtUnix || previous.ExpiresAtUnix <= previous.ObservedAtUnix || previous.ExpiresAtUnix-previous.ObservedAtUnix > int64(maxLifecycleValidity/time.Second) {
			return errors.New("previous promotion receipt time ordering is invalid")
		}
	}
	var receipt lifecycleReceipt
	if err := decodeSigned(data, publicKey, &receipt); err != nil {
		return err
	}
	expectedState := "new-primary"
	if action == "revoke" {
		expectedState = "old-revoked"
	}
	if receipt.Format != "kubebrain.jwt-kms-lifecycle.v1" || receipt.Action != action || receipt.State != expectedState || receipt.RequestID != operation.RequestID || receipt.OperationID != operation.OperationID || receipt.Instance != operation.Instance || receipt.OldVersionID != operation.OldKeyVersionID || receipt.NewVersionID != operation.NewKeyVersionID || receipt.OperationReceiptSHA256 != operationSHA || receipt.PreviousReceiptSHA256 != previousSHA {
		return errors.New("lifecycle receipt binding is invalid")
	}
	if receipt.ObservedAtUnix <= operation.CompletedAtUnix || receipt.ExpiresAtUnix <= receipt.ObservedAtUnix || receipt.ExpiresAtUnix-receipt.ObservedAtUnix > int64(maxLifecycleValidity/time.Second) {
		return errors.New("lifecycle receipt time ordering is invalid")
	}
	if action == "revoke" {
		if receipt.ObservedAtUnix <= previous.ObservedAtUnix {
			return errors.New("revocation must be observed after promotion")
		}
	}
	nowUnix := now.Unix()
	if nowUnix < receipt.ObservedAtUnix-int64(lifecycleClockSkew/time.Second) || nowUnix > receipt.ExpiresAtUnix {
		return errors.New("lifecycle receipt is not currently valid")
	}
	return nil
}

func decodeSigned(data []byte, key ed25519.PublicKey, output any) error {
	var outer envelope
	if err := decodeExact(data, &outer); err != nil || outer.Format != "kubebrain.jwt-kms-lifecycle-envelope.v1" {
		return errors.New("lifecycle envelope is invalid")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(outer.Payload)
	if err != nil || len(payload) == 0 || len(payload) > 64<<10 {
		return errors.New("lifecycle payload is invalid")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(outer.Signature)
	if err != nil || !ed25519.Verify(key, payload, signature) {
		return errors.New("lifecycle signature is invalid")
	}
	if err := decodeCanonical(payload, output); err != nil {
		return errors.New("lifecycle payload is not canonical")
	}
	return nil
}
func decodeCanonical(data []byte, output any) error {
	if err := decodeExact(data, output); err != nil {
		return err
	}
	canonical, err := json.Marshal(output)
	if err != nil || !bytes.Equal(canonical, data) {
		return errors.New("JSON is not canonical")
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
		return errors.New("trailing JSON")
	}
	return nil
}
func parsePublicKey(data []byte) (ed25519.PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "PUBLIC KEY" || len(bytes.TrimSpace(rest)) != 0 {
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
func readFile(path string, maximum int64, forbidden os.FileMode) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("path must be canonical and absolute")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("file must be a regular non-symlink")
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open failed")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum || info.Mode().Perm()&forbidden != 0 {
		return nil, errors.New("file boundary is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	final, statErr := file.Stat()
	if err != nil || statErr != nil || int64(len(data)) != info.Size() || final.Size() != info.Size() {
		return nil, errors.New("file changed while reading")
	}
	return data, nil
}
func validSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == string(bytes.ToLower([]byte(value)))
}
func validRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	isAlnum := func(c byte) bool { return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' }
	if !isAlnum(value[0]) || !isAlnum(value[len(value)-1]) {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if isAlnum(c) || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}
func validOperationID(value string) bool {
	const prefix = "jwt-key-rotate-"
	if len(value) != len(prefix)+20 || !bytes.HasPrefix([]byte(value), []byte(prefix)) {
		return false
	}
	for _, c := range []byte(value[len(prefix):]) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validInstance(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (i > 0 && (c == '.' || c == '_' || c == '-')) {
			continue
		}
		return false
	}
	return true
}
func validVersion(value string) bool {
	if len(value) == 0 || len(value) > 512 {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (i > 0 && bytes.ContainsRune([]byte("._:/@+=-"), rune(c))) {
			continue
		}
		return false
	}
	return true
}
