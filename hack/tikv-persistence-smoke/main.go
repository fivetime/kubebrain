package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
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

	if mode == "heal" {
		// Offline repair for an existing orphan (object present, revision-index
		// missing): recreate the index pointing at the latest object revision so the
		// key becomes writable/deletable again. New orphans cannot form (#31); this
		// is only for legacy keys left by pre-fix builds.
		healOrphan(ctx, pdAddrsRaw, key)
		return
	}

	if mode == "dump" {
		// Forensic dump: compare the revision-index (rev=0) slot against the actual
		// object keys, to diagnose an orphan/poison key (read-visible but write CAS
		// fails forever because the index and the latest object diverge).
		gr, gerr := cli.Get(ctx, key)
		if gerr != nil {
			fatalf("etcd get: %v", gerr)
		}
		if len(gr.Kvs) == 1 {
			fmt.Printf("etcd: create=%d mod=%d ver=%d val-bytes=%d\n",
				gr.Kvs[0].CreateRevision, gr.Kvs[0].ModRevision, gr.Kvs[0].Version, len(gr.Kvs[0].Value))
		} else {
			fmt.Printf("etcd: %d kvs (key not visible via etcd)\n", len(gr.Kvs))
		}
		dumpTiKV(ctx, pdAddrsRaw, key)
		return
	}

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

	c := coder.DefaultKeyspace().NewCoder()
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

func healOrphan(ctx context.Context, pdAddrsRaw, key string) {
	pdAddrs := splitCSV(pdAddrsRaw)
	kv, err := storagetikv.NewKvStorage(pdAddrs, 0, storagetikv.Security{})
	if err != nil {
		fatalf("create tikv client: %v", err)
	}
	defer kv.Close()

	c := coder.DefaultKeyspace().NewCoder()
	rawKey := []byte(key)
	revKey := c.EncodeRevisionKey(rawKey)

	if _, err := kv.Get(ctx, revKey); err == nil {
		fmt.Printf("no-op: index already present for %s (not an orphan)\n", key)
		return
	}

	// Find the latest object revision by reverse-scanning object keys.
	it, err := kv.Iter(ctx, c.EncodeObjectKey(rawKey, math.MaxUint64), c.EncodeObjectKey(rawKey, 0), 0, 1)
	if err != nil {
		fatalf("iter object keys: %v", err)
	}
	defer it.Close()
	if err := it.Next(ctx); err != nil {
		fatalf("no object key to heal for %s: %v", key, err)
	}
	_, latestRev, derr := c.Decode(it.Key())
	if derr != nil || latestRev == 0 {
		fatalf("decoded no live object revision for %s (rev=%d err=%v)", key, latestRev, derr)
	}

	revBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revBytes, latestRev)
	batch := kv.BeginBatchWrite()
	batch.PutIfNotExist(revKey, revBytes, 0)
	if err := batch.Commit(ctx); err != nil {
		fatalf("recreate index for %s: %v", key, err)
	}
	fmt.Printf("healed orphan key=%s recreated index -> revision=%d\n", key, latestRev)
}

func dumpTiKV(ctx context.Context, pdAddrsRaw, key string) {
	pdAddrs := splitCSV(pdAddrsRaw)
	kv, err := storagetikv.NewKvStorage(pdAddrs, 0, storagetikv.Security{})
	if err != nil {
		fatalf("create tikv client: %v", err)
	}
	defer kv.Close()

	c := coder.DefaultKeyspace().NewCoder()
	rawKey := []byte(key)

	// 1) The revision-index (rev=0) slot: what the write-path CAS reads.
	idxVal, err := kv.Get(ctx, c.EncodeRevisionKey(rawKey))
	if err != nil {
		fmt.Printf("index(rev=0): GET err: %v\n", err)
	} else {
		rev, tomb, perr := coder.ParseRevision(idxVal)
		fmt.Printf("index(rev=0): revision=%d tombstone=%v rawBytes=%d parseErr=%v\n", rev, tomb, len(idxVal), perr)
	}

	// 2) Reverse-scan every object key (rev>=1): what the read-path returns.
	start := c.EncodeObjectKey(rawKey, math.MaxUint64)
	end := c.EncodeObjectKey(rawKey, 0)
	it, err := kv.Iter(ctx, start, end, 0, 64)
	if err != nil {
		fatalf("iter object keys: %v", err)
	}
	defer it.Close()
	fmt.Printf("object keys (highest first):\n")
	var latestObjRev uint64
	n := 0
	for {
		if err := it.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("  iter err: %v\n", err)
			break
		}
		_, rev, derr := c.Decode(it.Key())
		if derr != nil {
			fmt.Printf("  [decode err %v]\n", derr)
			continue
		}
		if rev == 0 {
			// reached the revision-index slot; object scan done
			break
		}
		if n == 0 {
			latestObjRev = rev
		}
		val := it.Val()
		tomb := bytes.Equal(val, []byte("tombstone"))
		fmt.Printf("  obj rev=%d val-bytes=%d tombstone=%v\n", rev, len(val), tomb)
		n++
	}
	if n == 0 {
		fmt.Printf("  (no object keys!)\n")
	}

	// 3) Verdict: index vs latest object.
	idxRev, idxTomb, _ := coder.ParseRevision(idxVal)
	fmt.Printf("VERDICT: index_rev=%d (tomb=%v) latest_obj_rev=%d objects=%d -> %s\n",
		idxRev, idxTomb, latestObjRev, n, divergence(idxRev, latestObjRev))
}

func divergence(idxRev, latestObjRev uint64) string {
	switch {
	case latestObjRev == 0:
		return "ORPHAN: index points somewhere but NO object key exists"
	case idxRev == latestObjRev:
		return "CONSISTENT (index == latest object)"
	case idxRev > latestObjRev:
		return "POISON: index_rev > latest_obj_rev (index points at a missing/compacted object; read scans lower object, write CAS expects that lower rev but index holds higher -> CAS fails forever)"
	default:
		return "POISON: index_rev < latest_obj_rev (an object newer than the index exists; read returns newer object rev, write CAS expects it but index still holds older -> CAS fails forever)"
	}
}

func verifyDeletedTiKV(ctx context.Context, pdAddrsRaw, key string) {
	pdAddrs := splitCSV(pdAddrsRaw)
	kv, err := storagetikv.NewKvStorage(pdAddrs, 0, storagetikv.Security{})
	if err != nil {
		fatalf("create tikv client: %v", err)
	}
	defer kv.Close()

	c := coder.DefaultKeyspace().NewCoder()
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
