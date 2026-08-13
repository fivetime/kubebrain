// Command native-pitr-full-snapshot-receipt derives authoritative full backup
// evidence from a BR transactional backupmeta protobuf.
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
)

const (
	maxReceiptBytes    = 4 << 20
	maxBackupMetaBytes = 64 << 20
)

func main() {
	taskCreate := flag.String("task-create", "", "exact native-pitr-task-create.v4 receipt")
	backupMeta := flag.String("backupmeta", "", "downloaded BR txn backupmeta protobuf")
	storagePrefix := flag.String("storage-prefix", "", "immutable s3:// bucket/prefix containing backupmeta and SSTs")
	cipherMethod := flag.String("crypter-method", nativepitr.CipherMethodPlaintext, "plaintext or aes256-ctr")
	encryptionKeyID := flag.String("encryption-key-id", "", "immutable non-secret key version ID for encrypted backupmeta")
	encryptionKeyFile := flag.String("encryption-key-file", "", "exact AES-256 key file for encrypted backupmeta")
	flag.Parse()
	if err := run(*taskCreate, *backupMeta, *storagePrefix, *cipherMethod, *encryptionKeyID, *encryptionKeyFile, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR full snapshot receipt:", err)
		os.Exit(1)
	}
}

func run(taskCreatePath, backupMetaPath, storagePrefix, cipherMethod, encryptionKeyID, encryptionKeyFile string, out io.Writer) error {
	if taskCreatePath == "" || backupMetaPath == "" || storagePrefix == "" {
		return errors.New("task-create, backupmeta, and storage-prefix are required")
	}
	taskBytes, err := readBounded(taskCreatePath, maxReceiptBytes)
	if err != nil {
		return err
	}
	task, err := nativepitr.DecodeTaskCreate(bytes.NewReader(taskBytes))
	if err != nil {
		return err
	}
	metaBytes, err := readBounded(backupMetaPath, maxBackupMetaBytes)
	if err != nil {
		return err
	}
	taskDigest := sha256.Sum256(taskBytes)
	encryption := nativepitr.EncryptionIdentity{Method: cipherMethod, KeyID: encryptionKeyID}
	if err := encryption.Validate(); err != nil {
		return err
	}
	var encryptionKey []byte
	if encryption.Method == nativepitr.CipherMethodAES256CTR {
		encryptionKey, err = nativepitr.ReadAES256KeyFile(encryptionKeyFile)
		if err != nil {
			return err
		}
	} else if encryptionKeyFile != "" {
		return errors.New("plaintext backupmeta must not receive an encryption key file")
	}
	receipt, err := nativepitr.BuildFullSnapshotWithEncryption(task, hex.EncodeToString(taskDigest[:]), storagePrefix, metaBytes, encryption, encryptionKey)
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

func readBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, limit)
	}
	return b, nil
}
