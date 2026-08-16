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
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/cmd/internal/failedrestore"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

func main() {
	var audit, parameters, planPath, targetPath, provisioningPath, admissionPath, output string
	flag.StringVar(&audit, "failed-operation-audit", "", "canonical failed restore audit")
	flag.StringVar(&parameters, "failed-operation-parameters", "", "exact failed restore parameters")
	flag.StringVar(&planPath, "old-plan", "", "old approved plan")
	flag.StringVar(&targetPath, "old-target-snapshot-empty", "", "old target receipt")
	flag.StringVar(&provisioningPath, "old-target-provisioning", "", "old provisioning receipt")
	flag.StringVar(&admissionPath, "old-restore-admission", "", "old admission receipt")
	flag.StringVar(&output, "output", "", "authorization output")
	flag.Parse()
	if err := run(audit, parameters, planPath, targetPath, provisioningPath, admissionPath, output, time.Now().Unix()); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target retirement authorize:", err)
		os.Exit(1)
	}
}

func run(audit, parameters, planPath, targetPath, provisioningPath, admissionPath, output string, now int64) error {
	for _, v := range []string{audit, parameters, planPath, targetPath, provisioningPath, admissionPath, output} {
		if v == "" {
			return errors.New("all retirement authorization evidence and output are required")
		}
	}
	evidence, err := failedrestore.Load(audit, parameters, planPath)
	if err != nil {
		return err
	}
	if evidence.TargetPath != targetPath || evidence.AdmissionPath != admissionPath || evidence.TargetProvisionPath != provisioningPath {
		return errors.New("retirement evidence paths are not the exact failed restore parameters")
	}
	targetBytes, err := readBounded(targetPath)
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	provisioningBytes, err := readBounded(provisioningPath)
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningBytes))
	if err != nil {
		return err
	}
	admissionBytes, err := readBounded(admissionPath)
	if err != nil {
		return err
	}
	admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	if err != nil {
		return err
	}
	a, err := nativepitr.BuildTargetRetirementAuthorization(evidence.Plan, target, provisioning, admission, nativepitr.TargetRetirementAuthorization{FailedOperationAuditSHA256: evidence.AuditSHA256, FailedOperationParametersSHA: evidence.ParametersSHA256, OldPlanSHA256: evidence.PlanSHA256, OldTargetSHA256: failedrestore.Digest(targetBytes), OldProvisioningSHA256: failedrestore.Digest(provisioningBytes), OldAdmissionSHA256: failedrestore.Digest(admissionBytes), AuthorizedAtUnix: now})
	if err != nil {
		return err
	}
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeExclusive(output, data)
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
		return nil, errors.New("retirement authorization evidence is empty or exceeds 8 MiB")
	}
	return data, nil
}

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
	if err := os.Link(name, clean); err != nil {
		return err
	}
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("remove published retirement authorization temporary link: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr = dirFile.Sync()
	return errors.Join(syncErr, dirFile.Close())
}
