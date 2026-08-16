// Command native-pitr-target-retirement-receipt verifies an old target's
// retirement observation against its immutable lineage and publishes it.
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
	"path/filepath"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

func main() {
	input := flag.String("input", "", "candidate target retirement receipt")
	provisioning := flag.String("old-target-provisioning", "", "old target provisioning receipt")
	target := flag.String("old-target-snapshot-empty", "", "old target empty-snapshot receipt")
	admission := flag.String("old-restore-admission", "", "old restore admission receipt")
	output := flag.String("output", "", "new durable receipt path")
	verifyOnly := flag.Bool("verify-only", false, "verify the input and lineage without publishing")
	flag.Parse()
	if err := run(*input, *provisioning, *target, *admission, *output, *verifyOnly); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target retirement receipt:", err)
		os.Exit(1)
	}
}

func run(input, provisioningPath, targetPath, admissionPath, output string, verifyOnly bool) error {
	if input == "" || provisioningPath == "" || targetPath == "" || admissionPath == "" || (!verifyOnly && output == "") || (verifyOnly && output != "") {
		return errors.New("input, old target evidence, and output are required")
	}
	candidateData, err := readBounded(input)
	if err != nil {
		return err
	}
	provisioningData, err := readBounded(provisioningPath)
	if err != nil {
		return err
	}
	targetData, err := readBounded(targetPath)
	if err != nil {
		return err
	}
	admissionData, err := readBounded(admissionPath)
	if err != nil {
		return err
	}
	receipt, err := nativepitr.DecodeTargetRetirementReceipt(bytes.NewReader(candidateData))
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningData))
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetData))
	if err != nil {
		return err
	}
	admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionData))
	if err != nil {
		return err
	}
	if err := nativepitr.VerifyTargetRetirementBinding(receipt, provisioning, digest(provisioningData), target, admission, digest(admissionData)); err != nil {
		return err
	}
	if verifyOnly {
		return nil
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return writeExclusive(output, append(canonical, '\n'))
}

func readBounded(path string) (data []byte, retErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	data, err = io.ReadAll(io.LimitReader(file, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > 8<<20 {
		return nil, errors.New("receipt is empty or exceeds 8 MiB")
	}
	return data, nil
}

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func writeExclusive(output string, data []byte) (retErr error) {
	clean := filepath.Clean(output)
	dir := filepath.Dir(clean)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(clean)+".tmp-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer func() {
		if removeErr := os.Remove(name); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, removeErr)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return errors.Join(err, temp.Close())
	}
	_, writeErr := temp.Write(data)
	var syncErr error
	if writeErr == nil {
		syncErr = temp.Sync()
	}
	if err := errors.Join(writeErr, syncErr, temp.Close()); err != nil {
		return err
	}
	if err = os.Link(name, clean); err != nil {
		return err
	}
	if err = os.Remove(name); err != nil {
		return fmt.Errorf("remove published retirement receipt temporary link: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr = dirFile.Sync()
	closeErr := dirFile.Close()
	return errors.Join(syncErr, closeErr)
}
