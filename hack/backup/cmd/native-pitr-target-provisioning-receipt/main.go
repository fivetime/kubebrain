// Command native-pitr-target-provisioning-receipt strictly validates and
// durably publishes a read-only Kubernetes/CSI target observation.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	data, err := os.ReadFile(input)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > 8<<20 {
		return errors.New("target provisioning candidate is empty or exceeds 8 MiB")
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
	dir := filepath.Dir(cleanOutput)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(cleanOutput)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err = temp.Write(canonical); err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
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
	syncErr := dirFile.Sync()
	closeErr := dirFile.Close()
	return errors.Join(syncErr, closeErr)
}
