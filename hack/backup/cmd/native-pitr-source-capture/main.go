// Command native-pitr-source-capture fences a source KubeBrain keyspace and
// binds its logical witness to an authoritative TiKV revision before restore
// planning.
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
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
)

const maxReceiptBytes = 4 << 20

type options struct {
	action, task, witness, fenceReceipt, fullSnapshot, operationID string
	pdAddrs, ca, cert, key                                         string
	timeout                                                        time.Duration
}

func main() {
	if err := configureLogging(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var o options
	flag.StringVar(&o.action, "action", "acquire", "acquire or finalize")
	flag.StringVar(&o.task, "task-create", "", "exact native-pitr-task-create.v4 receipt")
	flag.StringVar(&o.witness, "source-witness", "", "exact full-keyspace kubebrain.logical.v2 witness (acquire)")
	flag.StringVar(&o.fenceReceipt, "capture-fence", "", "exact source-capture-fence receipt (finalize)")
	flag.StringVar(&o.fullSnapshot, "full-snapshot", "", "exact native-pitr-full-snapshot.v3 receipt (finalize)")
	flag.StringVar(&o.operationID, "operation-id", "", "immutable source capture operation ID (acquire)")
	flag.StringVar(&o.pdAddrs, "source-pd-addrs", "", "comma-separated source PD addresses")
	flag.StringVar(&o.ca, "source-ca", "", "source PD CA file")
	flag.StringVar(&o.cert, "source-cert", "", "source PD client certificate")
	flag.StringVar(&o.key, "source-key", "", "source PD client private key")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Minute, "capture operation deadline")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, o, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR source capture:", err)
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

func execute(parent context.Context, o options, out io.Writer, now func() time.Time) (retErr error) {
	if (o.action != "acquire" && o.action != "finalize") || o.task == "" || o.pdAddrs == "" || o.timeout <= 0 || (o.ca == "") != (o.cert == "") || (o.ca == "") != (o.key == "") {
		return errors.New("valid action, task-create, source PD, TLS tuple, and positive timeout are required")
	}
	if o.action == "acquire" && (o.witness == "" || o.operationID == "" || o.fenceReceipt != "" || o.fullSnapshot != "") {
		return errors.New("acquire requires source-witness and operation-id only")
	}
	if o.action == "finalize" && (o.fenceReceipt == "" || o.fullSnapshot == "" || o.witness != "" || o.operationID != "") {
		return errors.New("finalize requires capture-fence and full-snapshot only")
	}
	taskBytes, err := readBounded(o.task)
	if err != nil {
		return err
	}
	task, err := nativepitr.DecodeTaskCreate(bytes.NewReader(taskBytes))
	if err != nil {
		return err
	}
	taskSHA := digest(taskBytes)
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
	if clusterID == 0 || clusterID != task.ClusterID {
		return errors.New("live source PD cluster ID does not match task receipt")
	}
	store, err := storagetikv.NewKvStorageWithContext(ctx, addrs, 1, storagetikv.Security{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, store.Close()) }()
	prefix := nativepitr.CoordinationPrefix(task.Keyspace)
	if o.action == "acquire" {
		witnessSHA, err := digestFile(o.witness)
		if err != nil {
			return err
		}
		verified, err := backupfile.OpenVerified(o.witness)
		if err != nil {
			return err
		}
		status := verified.Status()
		if err := verified.Close(); err != nil {
			return err
		}
		token, err := restorationfence.NewToken(o.operationID, taskSHA, task.ClusterID, task.Keyspace)
		if err != nil {
			return err
		}
		resumed, err := restorationfence.Acquire(ctx, store, prefix, token)
		if err != nil {
			return err
		}
		observed, err := nativepitr.InspectFencedSourceRevision(ctx, store, task.Keyspace)
		if err != nil {
			return err
		}
		if observed > math.MaxInt64 {
			return errors.New("observed source revision exceeds etcd wire range")
		}
		fenceTS, err := store.GetTimestampOracle(ctx)
		if err != nil {
			return err
		}
		if err := restorationfence.Verify(ctx, store, prefix, token); err != nil {
			return err
		}
		if err := stableFile(o.task, taskBytes); err != nil {
			return err
		}
		postWitnessSHA, err := digestFile(o.witness)
		if err != nil || postWitnessSHA != witnessSHA {
			return errors.New("source witness changed during fence acquisition")
		}
		receipt, _, err := nativepitr.BuildSourceCaptureFenceReceipt(task, taskSHA, witnessSHA, o.operationID, status, int64(observed), fenceTS, now().UTC().Unix(), resumed)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(receipt)
	}
	fenceBytes, err := readBounded(o.fenceReceipt)
	if err != nil {
		return err
	}
	fence, err := nativepitr.DecodeSourceCaptureFenceReceipt(bytes.NewReader(fenceBytes))
	if err != nil {
		return err
	}
	fullBytes, err := readBounded(o.fullSnapshot)
	if err != nil {
		return err
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(fullBytes))
	if err != nil {
		return err
	}
	token, err := fence.Token()
	if err != nil {
		return err
	}
	if err := restorationfence.Verify(ctx, store, prefix, token); err != nil {
		return err
	}
	captureTS := fence.FenceSnapshotTS
	if full.BackupTS > captureTS {
		captureTS = full.BackupTS
	}
	finalizedAt := now().UTC().Unix()
	receipt, err := nativepitr.BuildSourceCaptureReceipt(task, taskSHA, fence, digest(fenceBytes), full, digest(fullBytes), captureTS, finalizedAt)
	if err != nil {
		return err
	}
	for path, expected := range map[string][]byte{o.task: taskBytes, o.fenceReceipt: fenceBytes, o.fullSnapshot: fullBytes} {
		if err := stableFile(path, expected); err != nil {
			return err
		}
	}
	if err := restorationfence.Release(ctx, store, prefix, token); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(receipt)
}

func readBounded(path string) (b []byte, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	b, err = io.ReadAll(io.LimitReader(f, maxReceiptBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxReceiptBytes {
		return nil, errors.New("receipt exceeds size limit")
	}
	return b, nil
}

func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func digestFile(path string) (digestValue string, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			digestValue = ""
			retErr = errors.Join(retErr, closeErr)
		}
	}()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func stableFile(path string, expected []byte) error {
	b, err := readBounded(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(b, expected) {
		return fmt.Errorf("input %q changed during source capture", path)
	}
	return nil
}
func parseAddrs(raw string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" || strings.Contains(value, "://") || seen[value] {
			return nil, errors.New("invalid source PD addresses")
		}
		seen[value] = true
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, errors.New("source PD addresses are empty")
	}
	return out, nil
}
