// Command native-pitr-full-artifact-verify verifies an exact local mirror of
// every object referenced by a BR transactional backupmeta file tree.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
)

const maxReceiptBytes = 4 << 20

func main() {
	fullSnapshot := flag.String("full-snapshot", "", "exact native-pitr-full-snapshot.v3 receipt")
	backupAttestation := flag.String("full-backup-attestation", "", "exact native-pitr-full-backup-attestation receipt")
	artifactRoot := flag.String("artifact-root", "", "exact local mirror root containing backupmeta and all referenced objects")
	remoteInventory := flag.String("remote-inventory", "", "canonical native-pitr-object-inventory.v1 receipt")
	encryptionKeyID := flag.String("encryption-key-id", "", "immutable non-secret key version ID required by encrypted attestation")
	encryptionKeyFile := flag.String("encryption-key-file", "", "exact AES-256 key file required to inspect encrypted metadata")
	flag.Parse()
	if err := run(*fullSnapshot, *backupAttestation, *remoteInventory, *artifactRoot, *encryptionKeyID, *encryptionKeyFile, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR full artifact verify:", err)
		os.Exit(1)
	}
}

func run(fullSnapshotPath, backupAttestationPath, remoteInventoryPath, artifactRoot, encryptionKeyID, encryptionKeyFile string, out io.Writer) error {
	if fullSnapshotPath == "" || backupAttestationPath == "" || remoteInventoryPath == "" || artifactRoot == "" {
		return errors.New("full-snapshot, full-backup-attestation, remote-inventory, and artifact-root are required")
	}
	b, err := readReceipt(fullSnapshotPath)
	if err != nil {
		return err
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(b))
	if err != nil {
		return err
	}
	attestationBytes, err := readReceipt(backupAttestationPath)
	if err != nil {
		return err
	}
	attestation, err := nativepitr.DecodeFullBackupAttestation(bytes.NewReader(attestationBytes))
	if err != nil {
		return err
	}
	var encryptionKey []byte
	if attestation.CipherMethod == nativepitr.CipherMethodAES256CTR {
		if encryptionKeyID != attestation.EncryptionKeyID {
			return errors.New("encryption-key-id must equal the attestation-bound immutable key version ID")
		}
		encryptionKey, err = nativepitr.ReadAES256KeyFile(encryptionKeyFile)
		if err != nil {
			return err
		}
	} else if encryptionKeyID != "" || encryptionKeyFile != "" {
		return errors.New("plaintext artifacts must not receive encryption key inputs")
	}
	inventory, inventoryBytes, err := pitrinventory.ReadCanonical(remoteInventoryPath)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(b)
	attestationDigest, err := nativepitr.FullBackupAttestationSHA256(attestation)
	if err != nil {
		return err
	}
	inventoryDigest := sha256.Sum256(inventoryBytes)
	receipt, err := nativepitr.VerifyFullArtifactsWithEncryption(full, hex.EncodeToString(digest[:]), attestation, attestationDigest, inventory, hex.EncodeToString(inventoryDigest[:]), artifactRoot, encryptionKey)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = out.Write(encoded)
	return err
}

func readReceipt(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxReceiptBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxReceiptBytes {
		return nil, fmt.Errorf("receipt %s exceeds %d bytes", path, maxReceiptBytes)
	}
	return b, nil
}
