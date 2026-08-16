// Command native-pitr-log-replay applies a plan-bound BR stream-log window to
// a restored KubeBrain tenant. It does not claim a target writer fence or PITR.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
)

const maxReceiptBytes = 4 << 20

type options struct {
	plan, fullRestore, logArtifacts, logRoot, fenceReceipt, admissionHandoff string
	pdAddrs, ca, cert, key, approve                                          string
	timeout                                                                  time.Duration
}

func main() {
	if err := configurePingCAPLogging(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var o options
	flag.StringVar(&o.plan, "plan", "", "exact native-pitr-restore-plan.v12 receipt")
	flag.StringVar(&o.fullRestore, "full-restore", "", "exact native-pitr-full-restore.v2 receipt")
	flag.StringVar(&o.logArtifacts, "log-artifacts", "", "exact native-pitr-log-artifacts.v2 receipt")
	flag.StringVar(&o.logRoot, "log-root", "", "local exact-version log artifact mirror")
	flag.StringVar(&o.fenceReceipt, "restoration-fence", "", "exact plan-bound restoration fence receipt")
	flag.StringVar(&o.admissionHandoff, "admission-handoff", "", "exact admission-to-restoration handoff receipt")
	flag.StringVar(&o.pdAddrs, "target-pd-addrs", "", "comma-separated target PD addresses")
	flag.StringVar(&o.ca, "target-ca", "", "target PD CA file")
	flag.StringVar(&o.cert, "target-cert", "", "target PD client certificate")
	flag.StringVar(&o.key, "target-key", "", "target PD client private key")
	flag.StringVar(&o.approve, "approve-plan-sha256", "", "explicit approval equal to exact plan SHA-256")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Hour, "log replay deadline")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, o, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR log replay:", err)
		os.Exit(1)
	}
}

func configurePingCAPLogging() error {
	sink := zapcore.AddSync(os.Stderr)
	logger, props, err := pingcaplog.InitLoggerWithWriteSyncer(&pingcaplog.Config{
		Level:  "error",
		Format: "text",
	}, sink, sink)
	if err != nil {
		return fmt.Errorf("configure PingCAP logging: %w", err)
	}
	pingcaplog.ReplaceGlobals(logger, props)
	return nil
}

func execute(parent context.Context, o options, out io.Writer, now func() time.Time) error {
	if o.plan == "" || o.fullRestore == "" || o.logArtifacts == "" || o.logRoot == "" || o.fenceReceipt == "" || o.admissionHandoff == "" || o.pdAddrs == "" || o.approve == "" || o.timeout <= 0 {
		return errors.New("plan, full-restore, log-artifacts, log-root, restoration-fence, target-pd-addrs, approval, and positive timeout are required")
	}
	if (o.ca == "") != (o.cert == "") || (o.ca == "") != (o.key == "") {
		return errors.New("target-ca, target-cert, and target-key must be supplied together")
	}
	planBytes, err := readStable(o.plan)
	if err != nil {
		return err
	}
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	if err != nil {
		return err
	}
	planSHA := digest(planBytes)
	if o.approve != planSHA {
		return errors.New("approve-plan-sha256 must equal the exact plan file SHA-256")
	}
	if plan.RestoreTS <= plan.Full.BackupTS {
		return errors.New("log replay requires restore_ts after the full backup TSO")
	}
	restoreBytes, err := readStable(o.fullRestore)
	if err != nil {
		return err
	}
	restore, err := nativepitr.DecodeFullRestoreExecution(bytes.NewReader(restoreBytes))
	if err != nil {
		return err
	}
	if restore.PlanSHA256 != planSHA || restore.PreWriteTarget.ClusterID != plan.Target.ClusterID {
		return errors.New("full restore receipt does not match plan")
	}
	logBytes, err := readStable(o.logArtifacts)
	if err != nil {
		return err
	}
	logSHA := digest(logBytes)
	if logSHA != plan.Log.ArtifactReceiptSHA {
		return errors.New("log artifact receipt does not match plan")
	}
	logs, err := nativepitr.DecodeLogArtifactReceipt(bytes.NewReader(logBytes))
	if err != nil {
		return err
	}
	fenceBytes, err := readStable(o.fenceReceipt)
	if err != nil {
		return err
	}
	fence, err := nativepitr.DecodeRestorationFenceReceipt(bytes.NewReader(fenceBytes))
	if err != nil {
		return err
	}
	if fence.PlanSHA256 != planSHA || fence.TargetClusterID != plan.Target.ClusterID || fence.Keyspace != plan.Source.Keyspace {
		return errors.New("restoration fence receipt does not match plan")
	}
	fenceToken, err := fence.Token()
	if err != nil {
		return err
	}
	handoffBytes, err := readStable(o.admissionHandoff)
	if err != nil {
		return err
	}
	handoff, err := nativepitr.DecodeAdmissionHandoff(bytes.NewReader(handoffBytes))
	if err != nil {
		return err
	}
	if handoff.PlanSHA256 != planSHA || handoff.TargetClusterID != plan.Target.ClusterID || handoff.Keyspace != plan.Source.Keyspace || handoff.RestorationFenceReceiptSHA256 != digest(fenceBytes) || handoff.FullRestoreReceiptSHA256 != digest(restoreBytes) {
		return errors.New("admission handoff receipt does not match replay inputs")
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	addrs, err := parseAddrs(o.pdAddrs)
	if err != nil {
		return err
	}
	pdc, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return err
	}
	targetID := pdc.GetClusterID(ctx)
	pdc.Close()
	if targetID == 0 || targetID != plan.Target.ClusterID {
		return errors.New("live target PD cluster ID does not match plan")
	}
	manifest, mutations, err := nativepitr.MaterializeReplay(logs, logSHA, o.logRoot, plan.Full.BackupTS, plan.RestoreTS)
	if err != nil {
		return err
	}
	store, err := storagetikv.NewKvStorageWithContext(ctx, addrs, 1, storagetikv.Security{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return err
	}
	defer store.Close()
	if err := restorationfence.Verify(ctx, store, fence.CoordinationPrefix, fenceToken); err != nil {
		return err
	}
	started := now().UTC().Unix()
	result, err := nativepitr.ApplyReplay(ctx, store, planSHA, manifest, mutations)
	if err != nil {
		return err
	}
	if err := restorationfence.Verify(ctx, store, fence.CoordinationPrefix, fenceToken); err != nil {
		return err
	}
	if err := verifyStable(o.plan, planBytes); err != nil {
		return err
	}
	if err := verifyStable(o.fullRestore, restoreBytes); err != nil {
		return err
	}
	if err := verifyStable(o.logArtifacts, logBytes); err != nil {
		return err
	}
	if err := verifyStable(o.fenceReceipt, fenceBytes); err != nil {
		return err
	}
	if err := verifyStable(o.admissionHandoff, handoffBytes); err != nil {
		return err
	}
	fenceSHA := digest(fenceBytes)
	handoffSHA := digest(handoffBytes)
	receipt, err := nativepitr.BuildLogReplayExecution(plan, restore, manifest, fence, fenceSHA, handoff, handoffSHA, nativepitr.LogReplayExecutionReceipt{PlanSHA256: planSHA, FullRestoreReceiptSHA256: digest(restoreBytes), LogArtifactReceiptSHA256: logSHA, RestorationFenceReceiptSHA256: fenceSHA, AdmissionHandoffReceiptSHA256: handoffSHA, AppliedMutations: result.AppliedMutations, AppliedTransactions: result.AppliedTransactions, LastCommitTS: manifest.LastCommitTS, Resumed: result.Resumed, StartedAtUnix: started, CompletedAtUnix: now().UTC().Unix()})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return enc.Encode(receipt)
}

func parseAddrs(raw string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" || strings.Contains(item, "://") || seen[item] {
			return nil, errors.New("target-pd-addrs must contain unique host:port values without schemes")
		}
		seen[item] = true
		out = append(out, item)
	}
	return out, nil
}
func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func readStable(path string) ([]byte, error) {
	a, e := readBounded(path)
	if e != nil {
		return nil, e
	}
	b, e := readBounded(path)
	if e != nil {
		return nil, e
	}
	if !bytes.Equal(a, b) {
		return nil, errors.New("receipt changed while reading")
	}
	return a, nil
}
func readBounded(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, maxReceiptBytes+1))
	if e != nil {
		return nil, e
	}
	if len(b) > maxReceiptBytes {
		return nil, errors.New("receipt exceeds size limit")
	}
	return b, nil
}
func verifyStable(path string, want []byte) error {
	got, e := readStable(path)
	if e != nil {
		return e
	}
	if !bytes.Equal(got, want) {
		return errors.New("receipt changed during replay")
	}
	return nil
}
