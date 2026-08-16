// Command native-pitr-admission-fence controls the restore gate stored in the
// target PD metadata domain, outside BR's TiKV SST import range.
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
	"github.com/kubewharf/kubebrain/pkg/backend/admissionfence"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/tlsutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zapcore"
)

const maxReceiptBytes = 4 << 20

type options struct {
	action, plan, receipt, fullRestore, restorationFence, operationID, pdAddrs string
	ca, cert, key, approve                                                     string
	timeout                                                                    time.Duration
}

func main() {
	if err := configureLogging(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var o options
	flag.StringVar(&o.action, "action", "acquire", "acquire, verify, or release")
	flag.StringVar(&o.plan, "plan", "", "exact native PITR restore plan")
	flag.StringVar(&o.receipt, "admission-receipt", "", "exact admission receipt (required for verify)")
	flag.StringVar(&o.fullRestore, "full-restore", "", "exact full restore v2 receipt (required for release)")
	flag.StringVar(&o.restorationFence, "restoration-fence", "", "exact post-full restoration fence receipt (required for release)")
	flag.StringVar(&o.operationID, "operation-id", "", "immutable restore operation ID (required for acquire)")
	flag.StringVar(&o.pdAddrs, "target-pd-addrs", "", "comma-separated target PD addresses")
	flag.StringVar(&o.ca, "target-ca", "", "target PD CA file")
	flag.StringVar(&o.cert, "target-cert", "", "target PD client certificate")
	flag.StringVar(&o.key, "target-key", "", "target PD client private key")
	flag.StringVar(&o.approve, "approve-plan-sha256", "", "explicit approval equal to exact plan SHA-256")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Minute, "admission operation deadline")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, o, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR admission fence:", err)
		os.Exit(1)
	}
}

func configureLogging() error {
	sink := zapcore.AddSync(os.Stderr)
	logger, props, err := pingcaplog.InitLoggerWithWriteSyncer(&pingcaplog.Config{Level: "error", Format: "text"}, sink, sink)
	if err != nil {
		return err
	}
	pingcaplog.ReplaceGlobals(logger, props)
	return nil
}

func execute(parent context.Context, o options, out io.Writer, now func() time.Time) error {
	if o.plan == "" || o.pdAddrs == "" || o.approve == "" || o.timeout <= 0 || (o.action != "acquire" && o.action != "verify" && o.action != "release") {
		return errors.New("valid action, plan, target PD, approval, and positive timeout are required")
	}
	if (o.ca == "") != (o.cert == "") || (o.ca == "") != (o.key == "") {
		return errors.New("target-ca, target-cert, and target-key must be supplied together")
	}
	planBytes, err := readBounded(o.plan)
	if err != nil {
		return err
	}
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	if err != nil {
		return err
	}
	planSHA := digest(planBytes)
	if o.approve != planSHA {
		return errors.New("approval must equal exact plan SHA-256")
	}
	addrs, err := parseAddrs(o.pdAddrs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	pdc, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return err
	}
	clusterID := pdc.GetClusterID(ctx)
	pdc.Close()
	if clusterID == 0 || clusterID != plan.Target.ClusterID {
		return errors.New("live target PD cluster ID does not match plan")
	}
	tlsConfig, err := (tlsutil.TLSConfig{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key}).ToTLSConfig()
	if err != nil {
		return err
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: addrs, DialTimeout: 5 * time.Second, TLS: tlsConfig})
	if err != nil {
		return err
	}
	defer cli.Close()
	switch o.action {
	case "acquire":
		if o.operationID == "" || o.receipt != "" || o.fullRestore != "" || o.restorationFence != "" {
			return errors.New("acquire requires operation-id and forbids receipt inputs")
		}
		provisional, token, err := nativepitr.BuildRestoreAdmissionReceipt(plan, planSHA, o.operationID, now().Unix(), false)
		if err != nil {
			return err
		}
		resumed, err := admissionfence.Acquire(ctx, cli, plan.Source.Keyspace, token)
		if err != nil {
			return err
		}
		receipt, _, err := nativepitr.BuildRestoreAdmissionReceipt(plan, planSHA, o.operationID, provisional.AcquiredAtUnix, resumed)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(receipt)
	case "verify":
		if o.receipt == "" || o.operationID != "" || o.fullRestore != "" || o.restorationFence != "" {
			return errors.New("verify requires admission-receipt and forbids other action inputs")
		}
		b, err := readBounded(o.receipt)
		if err != nil {
			return err
		}
		receipt, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(b))
		if err != nil {
			return err
		}
		if receipt.PlanSHA256 != planSHA || receipt.TargetClusterID != plan.Target.ClusterID || receipt.Keyspace != plan.Source.Keyspace {
			return errors.New("admission receipt does not match plan")
		}
		token, err := receipt.Token()
		if err != nil {
			return err
		}
		if err := admissionfence.Verify(ctx, cli, receipt.Keyspace, token); err != nil {
			return err
		}
		_, err = out.Write(b)
		return err
	case "release":
		if o.receipt == "" || o.fullRestore == "" || o.restorationFence == "" || o.operationID != "" {
			return errors.New("release requires admission-receipt, full-restore, and restoration-fence")
		}
		admissionBytes, err := readBounded(o.receipt)
		if err != nil {
			return err
		}
		admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
		if err != nil {
			return err
		}
		fullBytes, err := readBounded(o.fullRestore)
		if err != nil {
			return err
		}
		full, err := nativepitr.DecodeFullRestoreExecution(bytes.NewReader(fullBytes))
		if err != nil {
			return err
		}
		fenceBytes, err := readBounded(o.restorationFence)
		if err != nil {
			return err
		}
		fence, err := nativepitr.DecodeRestorationFenceReceipt(bytes.NewReader(fenceBytes))
		if err != nil {
			return err
		}
		admissionToken, err := admission.Token()
		if err != nil {
			return err
		}
		fenceToken, err := fence.Token()
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
		if err := admissionfence.Verify(ctx, cli, admission.Keyspace, admissionToken); err != nil {
			return err
		}
		for path, expected := range map[string][]byte{o.plan: planBytes, o.receipt: admissionBytes, o.fullRestore: fullBytes, o.restorationFence: fenceBytes} {
			got, err := readBounded(path)
			if err != nil || !bytes.Equal(got, expected) {
				return fmt.Errorf("input %q changed before admission handoff", path)
			}
		}
		releasedAt := now().Unix()
		handoff, err := nativepitr.BuildAdmissionHandoff(plan, planSHA, admission, digest(admissionBytes), full, digest(fullBytes), fence, digest(fenceBytes), releasedAt)
		if err != nil {
			return err
		}
		if err := admissionfence.Release(ctx, cli, admission.Keyspace, admissionToken); err != nil {
			return err
		}
		if err := restorationfence.Verify(ctx, store, fence.CoordinationPrefix, fenceToken); err != nil {
			return fmt.Errorf("restoration fence lost during admission handoff: %w", err)
		}
		return json.NewEncoder(out).Encode(handoff)
	}
	panic("validated action")
}

func readBounded(path string) ([]byte, error) {
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
		return nil, errors.New("receipt exceeds size limit")
	}
	return b, nil
}
func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func parseAddrs(raw string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" || strings.Contains(item, "://") || seen[item] {
			return nil, errors.New("invalid target PD addresses")
		}
		seen[item] = true
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil, errors.New("target PD addresses are empty")
	}
	return out, nil
}
