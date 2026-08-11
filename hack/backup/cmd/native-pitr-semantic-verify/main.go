// Command native-pitr-semantic-verify proves etcd behavior after a bounded
// native full restore or plan-bound log replay and fence handoff.
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
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/backup/internal/semanticverify"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
)

const maxInputBytes = 4 << 20

type options struct {
	plan, full, restore, replay, admissionHandoff, handoff, witness string
	probePrefix, pdAddrs, ca, cert, key                             string
	timeout                                                         time.Duration
}

func main() {
	if err := configurePingCAPLogging(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var o options
	flag.StringVar(&o.plan, "plan", "", "exact native PITR restore plan")
	flag.StringVar(&o.full, "full-snapshot", "", "exact full snapshot receipt")
	flag.StringVar(&o.restore, "full-restore", "", "exact full restore execution receipt")
	flag.StringVar(&o.replay, "log-replay", "", "exact log replay receipt (required when plan includes logs)")
	flag.StringVar(&o.admissionHandoff, "admission-handoff", "", "exact admission-to-restoration handoff receipt (required when plan includes logs)")
	flag.StringVar(&o.handoff, "fence-handoff", "", "exact fence handoff receipt (required when plan includes logs)")
	flag.StringVar(&o.witness, "witness", "", "full-keyspace kubebrain.logical.v2 witness captured at the plan restore point")
	flag.StringVar(&o.probePrefix, "probe-prefix", "/kubebrain-native-restore-probe", "non-production prefix for the write/watch probe")
	flag.StringVar(&o.pdAddrs, "target-pd-addrs", "", "comma-separated target PD addresses")
	flag.StringVar(&o.ca, "target-ca", "", "target PD CA file")
	flag.StringVar(&o.cert, "target-cert", "", "target PD client certificate")
	flag.StringVar(&o.key, "target-key", "", "target PD client private key")
	flag.DurationVar(&o.timeout, "timeout", 5*time.Minute, "semantic verification deadline")
	flag.Parse()
	if err := execute(context.Background(), o, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR semantic verify:", err)
		os.Exit(1)
	}
}

func configurePingCAPLogging() error {
	sink := zapcore.AddSync(os.Stderr)
	logger, props, err := pingcaplog.InitLoggerWithWriteSyncer(&pingcaplog.Config{Level: "error", Format: "text"}, sink, sink)
	if err != nil {
		return fmt.Errorf("configure PingCAP logging: %w", err)
	}
	pingcaplog.ReplaceGlobals(logger, props)
	return nil
}

func execute(parent context.Context, o options, out io.Writer, now func() time.Time) error {
	if o.plan == "" || o.full == "" || o.restore == "" || o.witness == "" || o.pdAddrs == "" || o.timeout <= 0 {
		return errors.New("plan, full-snapshot, full-restore, witness, target-pd-addrs, and positive timeout are required")
	}
	if os.Getenv("ENDPOINT") == "" {
		return errors.New("ENDPOINT is required")
	}
	if (o.ca == "") != (o.cert == "") || (o.ca == "") != (o.key == "") {
		return errors.New("target-ca, target-cert, and target-key must be supplied together")
	}
	if err := semanticverify.ValidateProbePrefix(o.probePrefix); err != nil {
		return err
	}
	planBytes, err := readStable(o.plan)
	if err != nil {
		return err
	}
	fullBytes, err := readStable(o.full)
	if err != nil {
		return err
	}
	restoreBytes, err := readStable(o.restore)
	if err != nil {
		return err
	}
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	if err != nil {
		return err
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(fullBytes))
	if err != nil {
		return err
	}
	restore, err := nativepitr.DecodeFullRestoreExecution(bytes.NewReader(restoreBytes))
	if err != nil {
		return err
	}
	withLogs := plan.RestoreTS > plan.Full.BackupTS
	if withLogs != (o.replay != "" && o.admissionHandoff != "" && o.handoff != "") {
		return errors.New("log-replay, admission-handoff, and fence-handoff must all be supplied exactly when the plan requires log replay")
	}
	var replayBytes, admissionHandoffBytes, handoffBytes []byte
	var replay nativepitr.LogReplayExecutionReceipt
	var admissionHandoff nativepitr.AdmissionHandoffReceipt
	var handoff nativepitr.RestorationFenceHandoffReceipt
	if withLogs {
		replayBytes, err = readStable(o.replay)
		if err != nil {
			return err
		}
		replay, err = nativepitr.DecodeLogReplayExecution(bytes.NewReader(replayBytes))
		if err != nil {
			return err
		}
		admissionHandoffBytes, err = readStable(o.admissionHandoff)
		if err != nil {
			return err
		}
		admissionHandoff, err = nativepitr.DecodeAdmissionHandoff(bytes.NewReader(admissionHandoffBytes))
		if err != nil {
			return err
		}
		handoffBytes, err = readStable(o.handoff)
		if err != nil {
			return err
		}
		handoff, err = nativepitr.DecodeRestorationFenceHandoff(bytes.NewReader(handoffBytes))
		if err != nil {
			return err
		}
	}
	witnessSHA, err := fileDigest(o.witness)
	if err != nil {
		return err
	}
	verified, err := backupfile.OpenVerified(o.witness)
	if err != nil {
		return fmt.Errorf("open semantic witness: %w", err)
	}
	defer verified.Close()
	openedWitnessSHA, err := fileDigest(o.witness)
	if err != nil {
		return err
	}
	if openedWitnessSHA != witnessSHA {
		return errors.New("semantic witness changed while opening")
	}
	if verified.Status().Prefix != "/" {
		return errors.New("native full semantic witness must cover the complete etcd keyspace prefix /")
	}
	addrs, err := parseAddrs(o.pdAddrs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	pdc, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return fmt.Errorf("connect target PD: %w", err)
	}
	targetClusterID := pdc.GetClusterID(ctx)
	pdc.Close()
	if targetClusterID == 0 || targetClusterID != plan.Target.ClusterID || restore.PreWriteTarget.ClusterID != targetClusterID {
		return errors.New("live target PD cluster ID does not match plan and restore receipt")
	}
	cli, err := etcdutil.NewClientFromEnv()
	if err != nil {
		return err
	}
	defer cli.Close()
	observation, err := semanticverify.Verify(ctx, cli, verified, o.probePrefix)
	if err != nil {
		return err
	}
	if err := semanticverify.VerifyTargetProbeHistory(ctx, addrs, storagetikv.Security{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key}, plan.Source.Keyspace, observation); err != nil {
		return err
	}
	if err := verifyStable(o.plan, planBytes); err != nil {
		return err
	}
	if err := verifyStable(o.full, fullBytes); err != nil {
		return err
	}
	if err := verifyStable(o.restore, restoreBytes); err != nil {
		return err
	}
	if withLogs {
		if err := verifyStable(o.replay, replayBytes); err != nil {
			return err
		}
		if err := verifyStable(o.admissionHandoff, admissionHandoffBytes); err != nil {
			return err
		}
		if err := verifyStable(o.handoff, handoffBytes); err != nil {
			return err
		}
	}
	postWitnessSHA, err := fileDigest(o.witness)
	if err != nil {
		return err
	}
	if postWitnessSHA != witnessSHA {
		return errors.New("semantic witness changed during verification")
	}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	base := nativepitr.FullSemanticVerificationInput{PlanSHA256: digest(planBytes), FullSnapshotSHA256: digest(fullBytes), FullRestoreSHA256: digest(restoreBytes), WitnessFileSHA256: witnessSHA, HistoricalHeaderRevision: observation.HistoricalHeaderRevision, CurrentHeaderRevision: observation.CurrentHeaderRevision, ProbePutRevision: observation.ProbePutRevision, ProbeDeleteRevision: observation.ProbeDeleteRevision, HistoricalExact: observation.HistoricalExact, CurrentExact: observation.CurrentExact, LeaseIdentityExact: observation.LeaseIdentityExact, WatchProbeSucceeded: observation.WatchProbeSucceeded, TargetProbeHistoryExact: true, VerifiedAtUnix: now().UTC().Unix()}
	if withLogs {
		receipt, err := nativepitr.BuildPITRSemanticVerification(plan, full, restore, replay, admissionHandoff, handoff, verified.Status(), nativepitr.PITRSemanticVerificationInput{FullSemanticVerificationInput: base, LogReplaySHA256: digest(replayBytes), FenceHandoffSHA256: digest(handoffBytes), AdmissionHandoffSHA256: digest(admissionHandoffBytes)})
		if err != nil {
			return err
		}
		return enc.Encode(receipt)
	}
	receipt, err := nativepitr.BuildFullSemanticVerification(plan, full, restore, verified.Status(), base)
	if err != nil {
		return err
	}
	return enc.Encode(receipt)
}

func parseAddrs(raw string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" || strings.Contains(item, "://") || seen[item] {
			return nil, errors.New("target-pd-addrs must contain unique non-empty host:port addresses without schemes")
		}
		seen[item] = true
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil, errors.New("target-pd-addrs is empty")
	}
	return out, nil
}
func readStable(path string) ([]byte, error) {
	first, err := readBounded(path)
	if err != nil {
		return nil, err
	}
	second, err := readBounded(path)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(first, second) {
		return nil, fmt.Errorf("input %q changed while reading", path)
	}
	return first, nil
}
func readBounded(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxInputBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxInputBytes {
		return nil, fmt.Errorf("input %q exceeds %d bytes", path, maxInputBytes)
	}
	return b, nil
}
func verifyStable(path string, expected []byte) error {
	got, err := readStable(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, expected) {
		return fmt.Errorf("input %q changed during verification", path)
	}
	return nil
}
func fileDigest(path string) (string, error) {
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
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
