// Command native-pitr-target-empty performs a read-only, full transactional
// snapshot scan against a restore target and emits canonical evidence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	pingcaplog "github.com/pingcap/log"
	"go.uber.org/zap/zapcore"
)

type options struct {
	pdAddrs string
	ca      string
	cert    string
	key     string
	timeout time.Duration
}

func main() {
	pingcaplog.SetLevel(zapcore.ErrorLevel)
	var o options
	flag.StringVar(&o.pdAddrs, "pd-addrs", "", "comma-separated target PD client addresses")
	flag.StringVar(&o.ca, "ca", "", "target PD/TiKV CA file")
	flag.StringVar(&o.cert, "cert", "", "target PD/TiKV client certificate file")
	flag.StringVar(&o.key, "key", "", "target PD/TiKV client private key file")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "overall read-only inspection timeout")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR target snapshot-empty check:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, out interface{ Write([]byte) (int, error) }) error {
	addrs, err := validateOptions(o)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	receipt, err := nativepitr.InspectLiveTargetSnapshotEmpty(ctx, addrs, o.ca, o.cert, o.key, time.Now().Unix())
	if err != nil {
		return err
	}
	b, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = out.Write(b)
	return err
}

func inspect(ctx context.Context, probe nativepitr.TargetProbe, addrs []string, checkedAt int64, out interface{ Write([]byte) (int, error) }) error {
	receipt, err := nativepitr.InspectTargetSnapshotEmpty(ctx, probe, addrs, checkedAt)
	if err != nil {
		return err
	}
	b, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = out.Write(b)
	return err
}

func validateOptions(o options) ([]string, error) {
	if o.timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	if (o.cert == "") != (o.key == "") || ((o.cert != "" || o.key != "") && o.ca == "") {
		return nil, errors.New("cert and key must be set together and require ca")
	}
	seen := map[string]bool{}
	var addrs []string
	for _, raw := range strings.Split(o.pdAddrs, ",") {
		addr := strings.TrimSpace(raw)
		if addr == "" || strings.Contains(addr, "://") || seen[addr] {
			return nil, fmt.Errorf("invalid or duplicate target PD address %q; use host:port", raw)
		}
		seen[addr] = true
		addrs = append(addrs, addr)
	}
	if len(addrs) == 0 {
		return nil, errors.New("pd-addrs is required")
	}
	sort.Strings(addrs)
	return addrs, nil
}
