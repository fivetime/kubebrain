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
	"sort"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/pingcap/kvproto/pkg/metapb"
	pingcaplog "github.com/pingcap/log"
	tikvcfg "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
)

type options struct {
	pdAddrs string
	ca      string
	cert    string
	key     string
	timeout time.Duration
}

type targetProbe interface {
	PDClusterID(context.Context) uint64
	TxnClusterID() uint64
	GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error)
	SnapshotTSAndFirstKey(context.Context) (uint64, bool, error)
	Close()
}

type liveProbe struct {
	pd  pd.Client
	txn *txnkv.Client
}

func (p *liveProbe) PDClusterID(ctx context.Context) uint64 { return p.pd.GetClusterID(ctx) }
func (p *liveProbe) TxnClusterID() uint64                   { return p.txn.GetClusterID() }
func (p *liveProbe) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return p.pd.GetAllStores(ctx, opts...)
}
func (p *liveProbe) SnapshotTSAndFirstKey(ctx context.Context) (uint64, bool, error) {
	ts, err := p.txn.GetTimestamp(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("obtain target TSO: %w", err)
	}
	snapshot := p.txn.GetSnapshot(ts)
	snapshot.SetKeyOnly(true)
	it, err := snapshot.Iter(nil, nil)
	if err != nil {
		return 0, false, fmt.Errorf("open full transactional snapshot scan: %w", err)
	}
	defer it.Close()
	return ts, it.Valid(), nil
}
func (p *liveProbe) Close() {
	p.txn.Close()
	p.pd.Close()
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
	if err := execute(context.Background(), o, os.Stdout); err != nil {
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
	security := pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key}
	pdc, err := pd.NewClientWithContext(ctx, addrs, security)
	if err != nil {
		return fmt.Errorf("connect target PD: %w", err)
	}
	// txnkv v2 reads its TLS settings from the process-global config. This is a
	// one-shot command and sets the complete value before creating the client.
	tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) {
		c.Security = tikvcfg.NewSecurity(o.ca, o.cert, o.key, nil)
	})
	txn, err := txnkv.NewClient(addrs)
	if err != nil {
		pdc.Close()
		return fmt.Errorf("connect target TiKV: %w", err)
	}
	return inspect(ctx, &liveProbe{pd: pdc, txn: txn}, addrs, time.Now().Unix(), out)
}

func inspect(ctx context.Context, probe targetProbe, addrs []string, checkedAt int64, out interface{ Write([]byte) (int, error) }) error {
	defer probe.Close()
	pdID, txnID := probe.PDClusterID(ctx), probe.TxnClusterID()
	if pdID == 0 || txnID == 0 || pdID != txnID {
		return fmt.Errorf("target PD/TiKV cluster ID mismatch: pd=%d txn=%d", pdID, txnID)
	}
	stores, err := probe.GetAllStores(ctx)
	if err != nil {
		return fmt.Errorf("list target TiKV stores: %w", err)
	}
	receipt := nativepitr.TargetSnapshotEmptyReceipt{
		Format: nativepitr.TargetSnapshotEmptyFormat, ClusterID: pdID, PDAddrs: addrs,
		ScanScope: nativepitr.WholeTransactionalKeyspace, CheckedAtUnix: checkedAt, ReadOnly: true,
	}
	for _, store := range stores {
		if store.GetState() == metapb.StoreState_Up {
			if store.GetId() == 0 || store.GetAddress() == "" {
				return errors.New("target PD returned an invalid Up TiKV store")
			}
			receipt.Stores = append(receipt.Stores, nativepitr.TargetStore{ID: store.GetId(), Address: store.GetAddress()})
		}
	}
	if len(receipt.Stores) == 0 {
		return errors.New("target PD returned no Up TiKV stores")
	}
	sort.Slice(receipt.Stores, func(i, j int) bool { return receipt.Stores[i].ID < receipt.Stores[j].ID })
	ts, found, err := probe.SnapshotTSAndFirstKey(ctx)
	if err != nil {
		return err
	}
	if found {
		return errors.New("target transactional keyspace is not empty at the observed snapshot")
	}
	receipt.SnapshotTS = ts
	if err := receipt.Validate(); err != nil {
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
