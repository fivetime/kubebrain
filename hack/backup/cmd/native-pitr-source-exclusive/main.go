// Command native-pitr-source-exclusive proves at the exact full-backup TSO
// that no visible transactional keys exist outside the KubeBrain range.
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
	"sort"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	pingcaplog "github.com/pingcap/log"
	"go.uber.org/zap/zapcore"
)

const maxReceiptBytes = 4 << 20

type options struct {
	full, pdAddrs, ca, cert, key string
	timeout                      time.Duration
}

func main() {
	pingcaplog.SetLevel(zapcore.ErrorLevel)
	var o options
	flag.StringVar(&o.full, "full-snapshot", "", "exact native-pitr-full-snapshot.v3 receipt")
	flag.StringVar(&o.pdAddrs, "pd-addrs", "", "comma-separated source PD addresses")
	flag.StringVar(&o.ca, "ca", "", "source CA file")
	flag.StringVar(&o.cert, "cert", "", "source client certificate")
	flag.StringVar(&o.key, "key", "", "source client private key")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Minute, "historical outside-range scan deadline")
	flag.Parse()
	if err := execute(context.Background(), o, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR source range-exclusive check:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, out io.Writer, now func() time.Time) error {
	if o.full == "" || o.timeout <= 0 {
		return errors.New("full-snapshot and a positive timeout are required")
	}
	if (o.cert == "") != (o.key == "") || ((o.cert != "" || o.key != "") && o.ca == "") {
		return errors.New("cert and key must be set together and require ca")
	}
	addrs, err := parseAddrs(o.pdAddrs)
	if err != nil {
		return err
	}
	b, err := readReceipt(o.full)
	if err != nil {
		return err
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(b))
	if err != nil {
		return err
	}
	h := sha256.Sum256(b)
	fullSHA := hex.EncodeToString(h[:])
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	receipt, err := nativepitr.InspectLiveSourceRangeExclusive(ctx, full, fullSHA, addrs, o.ca, o.cert, o.key, now().Unix())
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = out.Write(encoded)
	return err
}

func parseAddrs(raw string) ([]string, error) {
	seen := map[string]bool{}
	var addrs []string
	for _, part := range strings.Split(raw, ",") {
		addr := strings.TrimSpace(part)
		if addr == "" || strings.Contains(addr, "://") || seen[addr] {
			return nil, errors.New("invalid or duplicate source PD address")
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
		return nil, fmt.Errorf("receipt exceeds %d bytes", maxReceiptBytes)
	}
	return b, nil
}
