package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestScanProbe compares a prefix List against a point Get for every key under
// SCAN_PREFIX, to detect a deleted key that resurfaces only in List (the scanner
// tombstone anomaly). Run: SCAN=1 SCAN_PREFIX=/registry-... go test -run TestScanProbe -v
func TestScanProbe(t *testing.T) {
	if os.Getenv("SCAN") == "" {
		t.Skip("set SCAN=1")
	}
	ep := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if ep == "" {
		ep = "172.18.0.2:30079"
	}
	prefix := os.Getenv("SCAN_PREFIX")
	if prefix == "" {
		prefix = "/registry-kubebrain-apiserver-smoke"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lr, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatalf("prefix list: %v", err)
	}
	fmt.Printf("prefix %q -> %d keys via List (header rev=%d)\n", prefix, len(lr.Kvs), lr.Header.Revision)
	mismatches := 0
	for _, kv := range lr.Kvs {
		gr, err := cli.Get(ctx, string(kv.Key))
		if err != nil {
			t.Fatalf("point get %s: %v", kv.Key, err)
		}
		if len(gr.Kvs) == 0 {
			mismatches++
			fmt.Printf("  ANOMALY: List returns %q (modRev=%d) but point Get returns 0 kvs (deleted)\n", kv.Key, kv.ModRevision)
		}
	}
	fmt.Printf("mismatches (List-live but Get-deleted): %d\n", mismatches)
	if mismatches > 0 {
		t.Fatalf("REPRODUCED: %d deleted key(s) resurface in List", mismatches)
	}
}
