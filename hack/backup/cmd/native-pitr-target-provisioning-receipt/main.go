// Command native-pitr-target-provisioning-receipt strictly validates and
// durably publishes a read-only Kubernetes/CSI target observation.
package main

import (
	"bytes"
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
	input := flag.String("input", "", "candidate target provisioning receipt")
	output := flag.String("output", "", "new durable receipt path")
	flag.Parse()
	if err := run(*input, *output); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target provisioning receipt:", err)
		os.Exit(1)
	}
}

func run(input, output string) error {
	if input == "" || output == "" {
		return errors.New("input and output are required")
	}
	data, err := readBounded(input)
	if err != nil {
		return err
	}
	receipt, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(data))
	if err != nil {
		return err
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	canonical = append(canonical, '\n')
	cleanOutput := filepath.Clean(output)
	return writeExclusive(cleanOutput, canonical)
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
		return nil, errors.New("target provisioning candidate is empty or exceeds 8 MiB")
	}
	return data, nil
}

func writeExclusive(output string, data []byte) (retErr error) {
	cleanOutput := filepath.Clean(output)
	dir := filepath.Dir(cleanOutput)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(cleanOutput)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer func() {
		if removeErr := os.Remove(tempName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
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
	if err := os.Link(tempName, cleanOutput); err != nil {
		return err
	}
	if err := os.Remove(tempName); err != nil {
		return fmt.Errorf("remove published provisioning receipt temporary link: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr = dirFile.Sync()
	closeErr := dirFile.Close()
	return errors.Join(syncErr, closeErr)
}
