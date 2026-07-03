package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	var endpoint string
	var pdAddrsRaw string
	var key string
	var value string
	var mode string
	var requireNonEmpty bool
	flag.StringVar(&endpoint, "endpoint", "kubebrain.kubebrain-dev.svc:3379", "KubeBrain etcd endpoint")
	flag.StringVar(&pdAddrsRaw, "pd-addrs", "kb-pd.tidb-cluster.svc:2379", "comma separated TiKV PD addresses")
	flag.StringVar(&key, "key", "", "etcd key to verify")
	flag.StringVar(&value, "value", "", "expected value; when empty in read mode only existence is checked")
	flag.StringVar(&mode, "mode", "write", "write, read, or deleted")
	flag.BoolVar(&requireNonEmpty, "require-non-empty", false, "require etcd and TiKV object values to be non-empty")
	flag.Parse()

	if key == "" {
		fatalf("missing --key")
	}
	if mode == "write" && value == "" {
		fatalf("missing --value for write mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fatalf("create etcd client: %v", err)
	}
	defer cli.Close()

	if mode == "write" {
		if _, err := cli.Put(ctx, key, value); err != nil {
			fatalf("etcd put: %v", err)
		}
	} else if mode != "read" && mode != "deleted" {
		fatalf("invalid --mode %q", mode)
	}

	getResp, err := cli.Get(ctx, key)
	if err != nil {
		fatalf("etcd get: %v", err)
	}
	if mode == "deleted" {
		if len(getResp.Kvs) != 0 {
			fatalf("expected deleted etcd key, got %d kvs", len(getResp.Kvs))
		}
		verifyDeletedTiKV(ctx, pdAddrsRaw, key)
		return
	}
	if len(getResp.Kvs) != 1 {
		fatalf("expected one etcd kv, got %d", len(getResp.Kvs))
	}
	etcdKV := getResp.Kvs[0]
	if value != "" && string(etcdKV.Value) != value {
		fatalf("unexpected etcd value %q, want %q", string(etcdKV.Value), value)
	}
	if requireNonEmpty && len(etcdKV.Value) == 0 {
		fatalf("expected non-empty etcd value")
	}
	if etcdKV.ModRevision <= 0 {
		fatalf("invalid etcd mod revision %d", etcdKV.ModRevision)
	}

	pdAddrs := splitCSV(pdAddrsRaw)
	kv, err := storagetikv.NewKvStorage(pdAddrs, 0, storagetikv.Security{})
	if err != nil {
		fatalf("create tikv client: %v", err)
	}
	defer kv.Close()

	c := coder.NewNormalCoder()
	rawKey := []byte(key)
	revisionIndexKey := c.EncodeRevisionKey(rawKey)
	revisionIndexValue, err := kv.Get(ctx, revisionIndexKey)
	if err != nil {
		fatalf("tikv get revision index: %v", err)
	}
	revision, tombstone, err := coder.ParseRevision(revisionIndexValue)
	if err != nil {
		fatalf("parse revision index: %v", err)
	}
	if tombstone {
		fatalf("revision index is tombstone for live key")
	}
	if revision != uint64(etcdKV.ModRevision) {
		fatalf("tikv revision index=%d, etcd mod revision=%d", revision, etcdKV.ModRevision)
	}

	objectKey := c.EncodeObjectKey(rawKey, revision)
	objectValue, err := kv.Get(ctx, objectKey)
	if err != nil {
		fatalf("tikv get object key: %v", err)
	}
	if value != "" && !bytes.Equal(objectValue, []byte(value)) {
		fatalf("unexpected tikv object value %q, want %q", string(objectValue), value)
	}
	if requireNonEmpty && len(objectValue) == 0 {
		fatalf("expected non-empty tikv object value")
	}

	if value == "" {
		fmt.Printf("verified mode=%s key=%s revision=%d value-bytes=%d\n", mode, key, revision, len(objectValue))
	} else {
		fmt.Printf("verified mode=%s key=%s revision=%d value=%s\n", mode, key, revision, value)
	}
}

func verifyDeletedTiKV(ctx context.Context, pdAddrsRaw, key string) {
	pdAddrs := splitCSV(pdAddrsRaw)
	kv, err := storagetikv.NewKvStorage(pdAddrs, 0, storagetikv.Security{})
	if err != nil {
		fatalf("create tikv client: %v", err)
	}
	defer kv.Close()

	c := coder.NewNormalCoder()
	rawKey := []byte(key)
	revisionIndexValue, err := kv.Get(ctx, c.EncodeRevisionKey(rawKey))
	if err != nil {
		fatalf("tikv get deleted revision index: %v", err)
	}
	revision, tombstone, err := coder.ParseRevision(revisionIndexValue)
	if err != nil {
		fatalf("parse deleted revision index: %v", err)
	}
	if !tombstone {
		fatalf("expected deleted revision index tombstone, got live revision=%d", revision)
	}
	objectValue, err := kv.Get(ctx, c.EncodeObjectKey(rawKey, revision))
	if err != nil {
		fatalf("tikv get deleted object key: %v", err)
	}
	if !bytes.Equal(objectValue, []byte("tombstone")) {
		fatalf("expected deleted object tombstone, got %q", string(objectValue))
	}
	fmt.Printf("verified mode=deleted key=%s revision=%d\n", key, revision)
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
