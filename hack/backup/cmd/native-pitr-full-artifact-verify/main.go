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
)

const maxReceiptBytes = 4 << 20

func main() {
	fullSnapshot := flag.String("full-snapshot", "", "exact native-pitr-full-snapshot.v3 receipt")
	artifactRoot := flag.String("artifact-root", "", "exact local mirror root containing backupmeta and all referenced objects")
	flag.Parse()
	if err := run(*fullSnapshot, *artifactRoot, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR full artifact verify:", err)
		os.Exit(1)
	}
}

func run(fullSnapshotPath, artifactRoot string, out io.Writer) error {
	if fullSnapshotPath == "" || artifactRoot == "" {
		return errors.New("full-snapshot and artifact-root are required")
	}
	b, err := readReceipt(fullSnapshotPath)
	if err != nil {
		return err
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(b))
	if err != nil {
		return err
	}
	digest := sha256.Sum256(b)
	receipt, err := nativepitr.VerifyFullArtifacts(full, hex.EncodeToString(digest[:]), artifactRoot)
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
