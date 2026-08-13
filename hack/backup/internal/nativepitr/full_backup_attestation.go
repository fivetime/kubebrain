package nativepitr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const FullBackupAttestationFormat = "kubebrain.native-pitr-full-backup-attestation.v1"

// FullBackupAttestation is emitted by the executor that actually ran BR. It
// binds a successful, explicitly plaintext invocation to the exact binary and
// backupmeta consumed by the later full-snapshot receipt. It intentionally
// records no key material or credential-bearing storage URL.
type FullBackupAttestation struct {
	Format              string   `json:"format"`
	BRBinarySHA256      string   `json:"br_binary_sha256"`
	CanonicalArgs       []string `json:"canonical_args"`
	CanonicalArgsSHA256 string   `json:"canonical_args_sha256"`
	PDAddressesSHA256   string   `json:"pd_addresses_sha256"`
	StoragePrefix       string   `json:"storage_prefix"`
	BackupTS            uint64   `json:"backup_ts"`
	CipherMethod        string   `json:"cipher_method"`
	BackupMetaSHA256    string   `json:"backupmeta_sha256"`
	CompletedAtUnix     int64    `json:"completed_at_unix"`
	ExitSuccessful      bool     `json:"exit_successful"`
}

func BuildFullBackupAttestation(brBinarySHA, pdAddressesSHA, storagePrefix, backupMetaSHA string, backupTS uint64, args []string, completedAt int64) (FullBackupAttestation, error) {
	r := FullBackupAttestation{
		Format: FullBackupAttestationFormat, BRBinarySHA256: brBinarySHA,
		CanonicalArgs: append([]string(nil), args...), PDAddressesSHA256: pdAddressesSHA,
		StoragePrefix: storagePrefix, BackupTS: backupTS, CipherMethod: "plaintext",
		BackupMetaSHA256: backupMetaSHA, CompletedAtUnix: completedAt, ExitSuccessful: true,
	}
	r.CanonicalArgsSHA256 = digestStringSlice(r.CanonicalArgs)
	return r, r.Validate()
}

func (r FullBackupAttestation) Validate() error {
	if r.Format != FullBackupAttestationFormat || !r.ExitSuccessful || r.CompletedAtUnix <= 0 || r.BackupTS == 0 || r.CipherMethod != "plaintext" {
		return errors.New("native PITR full-backup attestation is incomplete")
	}
	for _, value := range []string{r.BRBinarySHA256, r.CanonicalArgsSHA256, r.PDAddressesSHA256, r.BackupMetaSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("native PITR full-backup attestation has invalid digest evidence")
		}
	}
	if err := validateS3Prefix(r.StoragePrefix); err != nil {
		return err
	}
	if len(r.CanonicalArgs) == 0 || digestStringSlice(r.CanonicalArgs) != r.CanonicalArgsSHA256 {
		return errors.New("native PITR full-backup canonical-argument digest does not match")
	}
	want := []string{
		"backup", "txn", "--storage=" + r.StoragePrefix,
		"--backupts=" + strconv.FormatUint(r.BackupTS, 10),
		"--crypter.method=plaintext",
	}
	if len(r.CanonicalArgs) != len(want) {
		return errors.New("native PITR full-backup arguments are not the canonical credential-free projection")
	}
	for i := range want {
		if r.CanonicalArgs[i] != want[i] || strings.ContainsAny(r.CanonicalArgs[i], "\x00\r\n") {
			return errors.New("native PITR full-backup arguments are not the canonical credential-free projection")
		}
	}
	return nil
}

// FullBackupAttestationSHA256 returns the digest of the canonical JSON form
// embedded in an artifact receipt. It deliberately does not hash source-file
// whitespace, so a decoded receipt can independently verify the binding.
func FullBackupAttestationSHA256(receipt FullBackupAttestation) (string, error) {
	if err := receipt.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func DecodeFullBackupAttestation(reader io.Reader) (FullBackupAttestation, error) {
	var receipt FullBackupAttestation
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode full-backup attestation: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return receipt, errors.New("full-backup attestation contains trailing JSON")
	}
	return receipt, receipt.Validate()
}

func digestStringSlice(values []string) string {
	encoded, _ := json.Marshal(values)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
