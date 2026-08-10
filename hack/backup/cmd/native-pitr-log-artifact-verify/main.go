// Command native-pitr-log-artifact-verify verifies an exact local mirror of
// BR stream metadata and every referenced log data segment.
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
	taskCreate := flag.String("task-create", "", "exact native-pitr-task-create.v4 receipt")
	taskReady := flag.String("task-ready", "", "exact native-pitr-task-ready.v4 receipt")
	artifactRoot := flag.String("artifact-root", "", "exact local mirror of v1/backupmeta metadata and referenced log objects")
	remoteInventory := flag.String("remote-inventory", "", "canonical native-pitr-object-inventory.v1 receipt")
	flag.Parse()
	if err := run(*taskCreate, *taskReady, *remoteInventory, *artifactRoot, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR log artifact verify:", err)
		os.Exit(1)
	}
}

func run(taskCreatePath, taskReadyPath, remoteInventoryPath, artifactRoot string, out io.Writer) error {
	if taskCreatePath == "" || taskReadyPath == "" || remoteInventoryPath == "" || artifactRoot == "" {
		return errors.New("task-create, task-ready, remote-inventory, and artifact-root are required")
	}
	taskBytes, err := readReceipt(taskCreatePath)
	if err != nil {
		return err
	}
	task, err := nativepitr.DecodeTaskCreate(bytes.NewReader(taskBytes))
	if err != nil {
		return err
	}
	readyBytes, err := readReceipt(taskReadyPath)
	if err != nil {
		return err
	}
	ready, err := nativepitr.DecodeTaskReady(bytes.NewReader(readyBytes))
	if err != nil {
		return err
	}
	inventory, inventoryBytes, err := pitrinventory.ReadCanonical(remoteInventoryPath)
	if err != nil {
		return err
	}
	taskDigest, readyDigest := sha256.Sum256(taskBytes), sha256.Sum256(readyBytes)
	inventoryDigest := sha256.Sum256(inventoryBytes)
	receipt, err := nativepitr.VerifyLogArtifacts(task, hex.EncodeToString(taskDigest[:]), ready, hex.EncodeToString(readyDigest[:]), inventory, hex.EncodeToString(inventoryDigest[:]), artifactRoot)
	if err != nil {
		return err
	}
	b, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = out.Write(b)
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
