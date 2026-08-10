// Command native-pitr-restore-plan binds full snapshot, log checkpoint and
// empty target evidence into a canonical, read-only restore plan.
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
	var taskCreate, fullSnapshot, taskReady string
	var in nativepitr.ReceiptPlanInputs
	flag.StringVar(&taskCreate, "task-create", "", "exact native-pitr-task-create.v2 receipt")
	flag.StringVar(&fullSnapshot, "full-snapshot", "", "exact native-pitr-full-snapshot.v1 receipt")
	flag.StringVar(&taskReady, "task-ready", "", "exact native-pitr-task-ready.v2 receipt")
	flag.Uint64Var(&in.TargetClusterID, "target-cluster-id", 0, "fresh isolated target PD cluster ID")
	flag.StringVar(&in.EmptyWitnessSHA256, "target-empty-witness-sha256", "", "SHA-256 of target emptiness evidence")
	flag.Uint64Var(&in.RestoreTS, "restore-ts", 0, "requested point-in-time TSO")
	flag.Parse()
	if err := run(taskCreate, fullSnapshot, taskReady, in, os.Stdout); err != nil {
		fail(err)
	}
}

func run(taskCreatePath, fullSnapshotPath, taskReadyPath string, in nativepitr.ReceiptPlanInputs, out io.Writer) error {
	if taskCreatePath == "" || fullSnapshotPath == "" || taskReadyPath == "" {
		return errors.New("task-create, full-snapshot, and task-ready are required")
	}
	taskBytes, err := readReceipt(taskCreatePath)
	if err != nil {
		return err
	}
	task, err := nativepitr.DecodeTaskCreate(bytes.NewReader(taskBytes))
	if err != nil {
		return err
	}
	fullBytes, err := readReceipt(fullSnapshotPath)
	if err != nil {
		return err
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(fullBytes))
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
	digest := sha256.Sum256(taskBytes)
	in.TaskCreateSHA256 = hex.EncodeToString(digest[:])
	plan, err := nativepitr.BuildFromReceipts(task, full, ready, in)
	if err != nil {
		return err
	}
	b, err := json.Marshal(plan)
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

func fail(err error) {
	fmt.Fprintln(os.Stderr, "native PITR restore plan:", err)
	os.Exit(1)
}
