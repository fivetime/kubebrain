package nativepitr

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const brCrypterIVBytes = aes.BlockSize

const (
	CipherMethodPlaintext = "plaintext"
	CipherMethodAES256CTR = "aes256-ctr"
)

var encryptionKeyIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$`)

// EncryptionIdentity is the non-secret identity recorded in durable evidence.
// KeyID names an immutable external key version; it is deliberately not a
// digest of key material and is empty for plaintext artifacts.
type EncryptionIdentity struct {
	Method string
	KeyID  string
}

// ReadAES256KeyFile validates BR v7.5.1 key-file semantics without retaining
// a verifier in durable evidence. The returned bytes are raw key material and
// must remain process-local.
func ReadAES256KeyFile(path string) (key []byte, retErr error) {
	if path == "" || !filepath.IsAbs(path) {
		return nil, errors.New("AES-256 encryption key file must be an absolute path")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open AES-256 encryption key file: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("AES-256 encryption key file must be regular")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 66))
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimSuffix(raw, []byte("\n"))
	if len(raw) != 64 {
		return nil, errors.New("AES-256 encryption key file must contain exactly 64 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(string(raw))
	if err != nil || len(decoded) != 32 || string(raw) != strings.ToLower(string(raw)) {
		return nil, errors.New("AES-256 encryption key file must contain exactly 64 lowercase hexadecimal characters")
	}
	return decoded, nil
}

// StageAES256KeyFile gives BR a private immutable-by-convention snapshot of
// already validated key bytes instead of the externally mutable source path.
func StageAES256KeyFile(key []byte) (string, func() error, error) {
	if len(key) != 32 {
		return "", nil, errors.New("staged AES-256 key must contain 32 bytes")
	}
	f, err := os.CreateTemp("", ".kubebrain-native-pitr-key-*")
	if err != nil {
		return "", nil, err
	}
	path := f.Name()
	cleanup := func() error {
		err := os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := f.Chmod(0o600); err != nil {
		return "", nil, errors.Join(err, f.Close(), cleanup())
	}
	_, writeErr := f.WriteString(hex.EncodeToString(key))
	var syncErr error
	if writeErr == nil {
		syncErr = f.Sync()
	}
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return "", nil, errors.Join(err, cleanup())
	}
	return path, cleanup, nil
}

func (e EncryptionIdentity) Validate() error {
	switch e.Method {
	case CipherMethodPlaintext:
		if e.KeyID != "" {
			return errors.New("plaintext backup must not name an encryption key")
		}
	case CipherMethodAES256CTR:
		if !encryptionKeyIDRE.MatchString(e.KeyID) {
			return errors.New("AES-256 backup requires a safe immutable encryption key version ID")
		}
	default:
		return errors.New("unsupported native PITR backup cipher method")
	}
	return nil
}

// decryptBRContent implements BR v7.5.1's AES-CTR artifact format. Top-level
// backupmeta prefixes its random 16-byte IV; recursive meta-index objects carry
// the IV in their parent backuppb.File entry instead.
func decryptBRContent(content []byte, encryption EncryptionIdentity, key, iv []byte, prefixedIV bool) ([]byte, error) {
	if err := encryption.Validate(); err != nil {
		return nil, err
	}
	if encryption.Method == CipherMethodPlaintext {
		if len(key) != 0 || prefixedIV {
			return nil, errors.New("plaintext BR content must not use an encryption key or prefixed IV")
		}
		return content, nil
	}
	if len(key) != 32 {
		return nil, errors.New("AES-256 BR content requires exactly 32 key bytes")
	}
	if prefixedIV {
		if len(content) <= brCrypterIVBytes {
			return nil, errors.New("encrypted backupmeta is missing its prefixed IV or ciphertext")
		}
		iv, content = content[:brCrypterIVBytes], content[brCrypterIVBytes:]
	}
	if len(iv) != brCrypterIVBytes {
		return nil, errors.New("encrypted BR content requires a 16-byte IV")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	plaintext := make([]byte, len(content))
	cipher.NewCTR(block, iv).XORKeyStream(plaintext, content)
	return plaintext, nil
}

func planRestoreEncryptionMatches(plan Plan, restore FullRestoreExecutionReceipt) bool {
	method := plan.Full.Encryption
	if method == "" && plan.Format == legacyPlanFormat {
		method = CipherMethodPlaintext
	}
	restoreMethod := restore.Encryption
	if restoreMethod == "" && restore.Format == legacyFullRestoreExecutionFormat {
		restoreMethod = CipherMethodPlaintext
	}
	return method == restoreMethod && plan.Full.EncryptionKeyID == restore.EncryptionKeyID
}
