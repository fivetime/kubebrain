// Command native-pitr-restoration-fence atomically acquires or verifies the
// persistent KubeBrain writer gate for one approved native PITR plan.
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

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
)

const maxReceiptBytes = 4 << 20

type options struct {
	action, plan, receipt, operationID, pdAddrs string
	ca, cert, key, approve                      string
	timeout                                     time.Duration
}

func main() {
	if err := configurePingCAPLogging(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var o options
	flag.StringVar(&o.action, "action", "acquire", "acquire or verify")
	flag.StringVar(&o.plan, "plan", "", "exact native-pitr-restore-plan.v10 receipt")
	flag.StringVar(&o.receipt, "fence-receipt", "", "exact fence receipt (required for verify)")
	flag.StringVar(&o.operationID, "operation-id", "", "immutable restore operation ID (required for acquire)")
	flag.StringVar(&o.pdAddrs, "target-pd-addrs", "", "comma-separated target PD addresses")
	flag.StringVar(&o.ca, "target-ca", "", "target PD CA file")
	flag.StringVar(&o.cert, "target-cert", "", "target PD client certificate")
	flag.StringVar(&o.key, "target-key", "", "target PD client private key")
	flag.StringVar(&o.approve, "approve-plan-sha256", "", "explicit approval equal to exact plan SHA-256")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Minute, "fence operation deadline")
	flag.Parse()
	if err := execute(context.Background(), o, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR restoration fence:", err)
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
	if o.plan == "" || o.pdAddrs == "" || o.approve == "" || o.timeout <= 0 || (o.action != "acquire" && o.action != "verify") {
		return errors.New("valid action, plan, target-pd-addrs, approval, and positive timeout are required")
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
		return errors.New("approve-plan-sha256 must equal the exact plan file SHA-256")
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
	store, err := storagetikv.NewKvStorage(addrs, 1, storagetikv.Security{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return err
	}
	defer store.Close()
	return operate(ctx, o, plan, planSHA, store, out, now)
}

func operate(ctx context.Context, o options, plan nativepitr.Plan, planSHA string, store storage.KvStorage, out io.Writer, now func() time.Time) error {
	switch o.action {
	case "acquire":
		if o.operationID == "" || o.receipt != "" {
			return errors.New("acquire requires operation-id and forbids fence-receipt")
		}
		provisional, token, err := nativepitr.BuildRestorationFenceReceipt(plan, planSHA, o.operationID, now().Unix(), false)
		if err != nil {
			return err
		}
		resumed, err := restorationfence.Acquire(ctx, store, provisional.CoordinationPrefix, token)
		if err != nil {
			return err
		}
		receipt, _, err := nativepitr.BuildRestorationFenceReceipt(plan, planSHA, o.operationID, provisional.VerifiedAtUnix, resumed)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(receipt)
	case "verify":
		if o.receipt == "" || o.operationID != "" {
			return errors.New("verify requires fence-receipt and forbids operation-id")
		}
		b, err := readBounded(o.receipt)
		if err != nil {
			return err
		}
		receipt, err := nativepitr.DecodeRestorationFenceReceipt(bytes.NewReader(b))
		if err != nil {
			return err
		}
		if receipt.PlanSHA256 != planSHA || receipt.TargetClusterID != plan.Target.ClusterID || receipt.Keyspace != plan.Source.Keyspace {
			return errors.New("fence receipt does not match approved plan")
		}
		token, err := receipt.Token()
		if err != nil {
			return err
		}
		if err := restorationfence.Verify(ctx, store, receipt.CoordinationPrefix, token); err != nil {
			return err
		}
		_, err = out.Write(b)
		return err
	default:
		panic("validated action")
	}
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
			return nil, errors.New("target-pd-addrs must contain unique host:port values without schemes")
		}
		seen[item] = true
		out = append(out, item)
	}
	return out, nil
}
