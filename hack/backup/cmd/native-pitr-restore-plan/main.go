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

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

const maxReceiptBytes = 4 << 20

func main() {
	var taskCreate, fullSnapshot, fullArtifacts, taskReady, logArtifacts, sourceExclusive, targetEmpty, witness string
	var in nativepitr.ReceiptPlanInputs
	flag.StringVar(&taskCreate, "task-create", "", "exact native-pitr-task-create.v4 receipt")
	flag.StringVar(&fullSnapshot, "full-snapshot", "", "exact native-pitr-full-snapshot.v3 receipt")
	flag.StringVar(&fullArtifacts, "full-artifacts", "", "exact native-pitr-full-artifacts.v2 receipt")
	flag.StringVar(&taskReady, "task-ready", "", "exact native-pitr-task-ready.v4 receipt")
	flag.StringVar(&logArtifacts, "log-artifacts", "", "exact native-pitr-log-artifacts.v2 receipt")
	flag.StringVar(&sourceExclusive, "source-range-exclusive", "", "exact native-pitr-source-range-exclusive.v2 receipt")
	flag.StringVar(&targetEmpty, "target-snapshot-empty", "", "exact native-pitr-target-snapshot-empty.v1 receipt")
	flag.StringVar(&witness, "source-witness", "", "exact full-keyspace kubebrain.logical.v2 source witness")
	flag.Uint64Var(&in.RestoreTS, "restore-ts", 0, "requested point-in-time TSO")
	flag.Parse()
	if err := run(taskCreate, fullSnapshot, fullArtifacts, taskReady, logArtifacts, sourceExclusive, targetEmpty, witness, in, os.Stdout); err != nil {
		fail(err)
	}
}

func run(taskCreatePath, fullSnapshotPath, fullArtifactsPath, taskReadyPath, logArtifactsPath, sourceExclusivePath, targetEmptyPath, witnessPath string, in nativepitr.ReceiptPlanInputs, out io.Writer) error {
	if taskCreatePath == "" || fullSnapshotPath == "" || fullArtifactsPath == "" || taskReadyPath == "" || logArtifactsPath == "" || sourceExclusivePath == "" || targetEmptyPath == "" || witnessPath == "" {
		return errors.New("task-create, full-snapshot, full-artifacts, task-ready, log-artifacts, source-range-exclusive, target-snapshot-empty, and source-witness are required")
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
	artifactBytes, err := readReceipt(fullArtifactsPath)
	if err != nil {
		return err
	}
	artifacts, err := nativepitr.DecodeArtifactReceipt(bytes.NewReader(artifactBytes))
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
	logArtifactBytes, err := readReceipt(logArtifactsPath)
	if err != nil {
		return err
	}
	logs, err := nativepitr.DecodeLogArtifactReceipt(bytes.NewReader(logArtifactBytes))
	if err != nil {
		return err
	}
	sourceExclusiveBytes, err := readReceipt(sourceExclusivePath)
	if err != nil {
		return err
	}
	sourceExclusive, err := nativepitr.DecodeSourceRangeExclusive(bytes.NewReader(sourceExclusiveBytes))
	if err != nil {
		return err
	}
	targetBytes, err := readReceipt(targetEmptyPath)
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	witnessDigestBefore, err := digestFile(witnessPath)
	if err != nil {
		return err
	}
	verifiedWitness, err := backupfile.OpenVerified(witnessPath)
	if err != nil {
		return err
	}
	witnessStatus := verifiedWitness.Status()
	if err := verifiedWitness.Close(); err != nil {
		return err
	}
	taskDigest := sha256.Sum256(taskBytes)
	fullDigest := sha256.Sum256(fullBytes)
	artifactDigest := sha256.Sum256(artifactBytes)
	readyDigest := sha256.Sum256(readyBytes)
	logArtifactDigest := sha256.Sum256(logArtifactBytes)
	sourceExclusiveDigest := sha256.Sum256(sourceExclusiveBytes)
	targetDigest := sha256.Sum256(targetBytes)
	witnessDigest, err := digestFile(witnessPath)
	if err != nil {
		return err
	}
	if witnessDigest != witnessDigestBefore {
		return errors.New("source witness changed while restore plan was being built")
	}
	in.TaskCreateSHA256 = hex.EncodeToString(taskDigest[:])
	in.FullSnapshotSHA256 = hex.EncodeToString(fullDigest[:])
	in.ArtifactReceiptSHA256 = hex.EncodeToString(artifactDigest[:])
	in.TaskReadySHA256 = hex.EncodeToString(readyDigest[:])
	in.LogArtifactSHA256 = hex.EncodeToString(logArtifactDigest[:])
	in.SourceExclusiveSHA256 = hex.EncodeToString(sourceExclusiveDigest[:])
	in.TargetReceiptSHA256 = hex.EncodeToString(targetDigest[:])
	in.WitnessFileSHA256 = witnessDigest
	in.Witness = witnessStatus
	plan, err := nativepitr.BuildFromReceipts(task, full, artifacts, ready, logs, sourceExclusive, target, in)
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

func digestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
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
