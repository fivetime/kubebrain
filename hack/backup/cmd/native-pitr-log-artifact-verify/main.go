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
)

const maxReceiptBytes = 4 << 20

func main() {
	taskCreate := flag.String("task-create", "", "exact native-pitr-task-create.v4 receipt")
	taskReady := flag.String("task-ready", "", "exact native-pitr-task-ready.v4 receipt")
	artifactRoot := flag.String("artifact-root", "", "exact local mirror of v1/backupmeta metadata and referenced log objects")
	flag.Parse()
	if err := run(*taskCreate, *taskReady, *artifactRoot, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR log artifact verify:", err)
		os.Exit(1)
	}
}

func run(taskCreatePath, taskReadyPath, artifactRoot string, out io.Writer) error {
	if taskCreatePath == "" || taskReadyPath == "" || artifactRoot == "" {
		return errors.New("task-create, task-ready, and artifact-root are required")
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
	taskDigest, readyDigest := sha256.Sum256(taskBytes), sha256.Sum256(readyBytes)
	receipt, err := nativepitr.VerifyLogArtifacts(task, hex.EncodeToString(taskDigest[:]), ready, hex.EncodeToString(readyDigest[:]), artifactRoot)
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
