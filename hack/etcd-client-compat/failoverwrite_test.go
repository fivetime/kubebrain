package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestFailoverWriteProbe continuously Puts a key and logs OK/FAIL with a
// millisecond timestamp, so an external leader-kill can be correlated to the
// write-serving gap (the true leaderless window that impacts consumers).
// Run: FOWRITE=1 FOWRITE_SECS=90 go test -run TestFailoverWriteProbe -v
func TestFailoverWriteProbe(t *testing.T) {
	if os.Getenv("FOWRITE") == "" {
		t.Skip("set FOWRITE=1")
	}
	ep := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if ep == "" {
		ep = "172.18.0.2:30079"
	}
	secs := 90
	fmt.Sscanf(os.Getenv("FOWRITE_SECS"), "%d", &secs)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	key := "/failover-write-probe/k"
	for time.Now().Before(deadline) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := cli.Put(ctx, key, fmt.Sprintf("%d", start.UnixMilli()))
		cancel()
		st := "OK"
		if err != nil {
			st = "FAIL"
		}
		fmt.Printf("t=%d %s %dms\n", start.UnixMilli(), st, time.Since(start).Milliseconds())
		time.Sleep(200 * time.Millisecond)
	}
}
