package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
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
	targetBytes, err := os.ReadFile(targetPath)
	if err != nil {
		return err
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	provisioningBytes, err := os.ReadFile(provisioningPath)
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningBytes))
	if err != nil {
		return err
	}
	admissionBytes, err := os.ReadFile(admissionPath)
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
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}
