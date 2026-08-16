// Command native-pitr-preflight proves that a PD/TiKV cluster exposes the
// primitives needed by KubeBrain's future native transactional PITR
// orchestrator. It is deliberately read-only: task registration changes PD
// metadata and GC safepoints and belongs in a durable, approved operation.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	logbackuppb "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const metaPrefix = "/tidb/br-stream"
const maxTLSPEMBytes = 1 << 20

var taskNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)

type options struct {
	pdAddrs  string
	keyspace string
	task     string
	ca       string
	cert     string
	key      string
	timeout  time.Duration
}

type storeReceipt struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
	Service string `json:"log_backup_service"`
}

type receipt struct {
	Format        string         `json:"format"`
	ClusterID     uint64         `json:"cluster_id"`
	PDAddrs       []string       `json:"pd_addrs"`
	Keyspace      string         `json:"keyspace"`
	TaskName      string         `json:"task_name"`
	StartKeyHex   string         `json:"start_key_hex"`
	EndKeyHex     string         `json:"end_key_hex"`
	TaskInfoKey   string         `json:"task_info_key"`
	TaskRangesKey string         `json:"task_ranges_prefix"`
	OwnershipKeys []string       `json:"ownership_paths_checked"`
	TaskAvailable bool           `json:"task_name_available"`
	TaskCount     int64          `json:"existing_task_count"`
	Stores        []storeReceipt `json:"stores"`
	ReadOnly      bool           `json:"read_only"`
}

type pdAPI interface {
	GetClusterID(context.Context) uint64
	GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error)
	Get(context.Context, []byte, ...pd.OpOption) (interfaceGetResponse, error)
	Close()
}

// interfaceGetResponse is kept out of the production path; the concrete PD
// response is adapted below so unit tests need no generated protobuf fixtures.
type interfaceGetResponse interface{ GetCount() int64 }

type pdAdapter struct{ pd.Client }

func (p pdAdapter) Get(ctx context.Context, key []byte, opts ...pd.OpOption) (interfaceGetResponse, error) {
	return p.Client.Get(ctx, key, opts...)
}

func main() {
	// PD client info logs default to stdout. Keep stdout a single canonical
	// receipt so callers can hash and parse it without log filtering.
	pingcaplog.SetLevel(zapcore.ErrorLevel)
	var o options
	flag.StringVar(&o.pdAddrs, "pd-addrs", "", "comma-separated PD client addresses")
	flag.StringVar(&o.keyspace, "keyspace", "", "KubeBrain tenant keyspace; empty selects the legacy tenant")
	flag.StringVar(&o.task, "task-name", "", "proposed native backup-stream task name")
	flag.StringVar(&o.ca, "ca", "", "PD/TiKV CA file")
	flag.StringVar(&o.cert, "cert", "", "PD/TiKV client certificate file")
	flag.StringVar(&o.key, "key", "", "PD/TiKV client private key file")
	flag.DurationVar(&o.timeout, "timeout", 15*time.Second, "overall preflight timeout")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR preflight:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, out interface{ Write([]byte) (int, error) }) error {
	addrs, err := validateOptions(o)
	if err != nil {
		return err
	}
	ks, err := coder.NewKeyspace(o.keyspace)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	security := pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key}
	client, err := pd.NewClientWithContext(ctx, addrs, security)
	if err != nil {
		return fmt.Errorf("connect PD: %w", err)
	}
	return inspect(ctx, pdAdapter{client}, o, addrs, ks, out)
}

func inspect(ctx context.Context, client pdAPI, o options, addrs []string, ks *coder.Keyspace, out interface{ Write([]byte) (int, error) }) error {
	defer client.Close()
	infoKey := []byte(metaPrefix + "/info/" + o.task)
	rangesPrefix := []byte(metaPrefix + "/ranges/" + o.task + "/")
	ownership := []struct {
		key    string
		prefix bool
	}{
		{string(infoKey), false},
		{string(rangesPrefix), true},
		{metaPrefix + "/checkpoint/" + o.task + "/", true},
		{metaPrefix + "/storage-checkpoint/" + o.task + "/", true},
		{metaPrefix + "/pause/" + o.task, false},
		{metaPrefix + "/last-error/" + o.task + "/", true},
	}
	checked := make([]string, 0, len(ownership))
	allTasks, err := client.Get(ctx, []byte(metaPrefix+"/info/"), pd.WithPrefix())
	if err != nil {
		return fmt.Errorf("read backup-stream task list: %w", err)
	}
	if allTasks.GetCount() != 0 {
		return fmt.Errorf("backup-stream already has %d task(s); TiKV v7.5.1 supports one", allTasks.GetCount())
	}
	for _, item := range ownership {
		opts := []pd.OpOption(nil)
		if item.prefix {
			opts = append(opts, pd.WithPrefix())
		}
		got, err := client.Get(ctx, []byte(item.key), opts...)
		if err != nil {
			return fmt.Errorf("read task ownership path %q: %w", item.key, err)
		}
		if got.GetCount() != 0 {
			return fmt.Errorf("backup-stream task %q already has metadata at %q; refusing ambiguous ownership", o.task, item.key)
		}
		checked = append(checked, item.key)
	}
	stores, err := client.GetAllStores(ctx)
	if err != nil {
		return fmt.Errorf("list TiKV stores: %w", err)
	}
	creds, err := transportCredentials(o)
	if err != nil {
		return err
	}
	clusterID := client.GetClusterID(ctx)
	if clusterID == 0 {
		return errors.New("PD returned zero cluster ID")
	}
	result := receipt{Format: "kubebrain.native-pitr-preflight.v1", ClusterID: clusterID, PDAddrs: addrs, Keyspace: ks.Name(), TaskName: o.task, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), TaskInfoKey: string(infoKey), TaskRangesKey: string(rangesPrefix), OwnershipKeys: checked, TaskAvailable: true, TaskCount: 0, ReadOnly: true}
	for _, store := range stores {
		if store.GetState() != metapb.StoreState_Up {
			continue
		}
		if store.GetAddress() == "" {
			return fmt.Errorf("Up TiKV store %d has no address", store.GetId())
		}
		if err := probeLogBackupFn(ctx, store.GetAddress(), creds); err != nil {
			return fmt.Errorf("TiKV store %d (%s): %w", store.GetId(), store.GetAddress(), err)
		}
		result.Stores = append(result.Stores, storeReceipt{ID: store.GetId(), Address: store.GetAddress(), Service: "available"})
	}
	if len(result.Stores) == 0 {
		return errors.New("PD returned no Up TiKV stores")
	}
	sort.Slice(result.Stores, func(i, j int) bool { return result.Stores[i].ID < result.Stores[j].ID })
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = out.Write(b)
	return err
}

func validateOptions(o options) ([]string, error) {
	if !taskNameRE.MatchString(o.task) {
		return nil, errors.New("task-name must be a lowercase DNS label with at most 64 characters")
	}
	if o.timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	if (o.cert == "") != (o.key == "") {
		return nil, errors.New("cert and key must be set together")
	}
	if (o.cert != "" || o.key != "") && o.ca == "" {
		return nil, errors.New("client certificate authentication requires ca")
	}
	var addrs []string
	seen := map[string]bool{}
	for _, raw := range strings.Split(o.pdAddrs, ",") {
		a := strings.TrimSpace(raw)
		if a == "" || strings.Contains(a, "://") {
			return nil, fmt.Errorf("invalid PD address %q; use host:port", raw)
		}
		if seen[a] {
			return nil, fmt.Errorf("duplicate PD address %q", a)
		}
		seen[a] = true
		addrs = append(addrs, a)
	}
	if len(addrs) == 0 {
		return nil, errors.New("pd-addrs is required")
	}
	sort.Strings(addrs)
	return addrs, nil
}

func transportCredentials(o options) (credentials.TransportCredentials, error) {
	if o.ca == "" {
		return insecure.NewCredentials(), nil
	}
	pem, err := readBoundedPEM(o.ca)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("CA contains no certificates")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if o.cert != "" {
		pair, err := tls.LoadX509KeyPair(o.cert, o.key)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return credentials.NewTLS(cfg), nil
}

func probeLogBackup(ctx context.Context, address string, creds credentials.TransportCredentials) (retErr error) {
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(creds), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("connect log-backup service: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, conn.Close()) }()
	_, err = logbackuppb.NewLogBackupClient(conn).GetLastFlushTSOfRegion(ctx, &logbackuppb.GetLastFlushTSOfRegionRequest{})
	if err != nil {
		return fmt.Errorf("probe log-backup service: %w", err)
	}
	return nil
}

func readBoundedPEM(path string) (contents []byte, retErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	contents, err = io.ReadAll(io.LimitReader(file, maxTLSPEMBytes+1))
	if err != nil {
		return nil, err
	}
	if len(contents) > maxTLSPEMBytes {
		return nil, fmt.Errorf("TLS PEM exceeds %d bytes", maxTLSPEMBytes)
	}
	return contents, nil
}

var probeLogBackupFn = probeLogBackup
