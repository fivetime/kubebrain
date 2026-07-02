#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
TTL_SECONDS="${TTL_SECONDS:-2}"
LEASES="${LEASES:-1}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-15}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go

workdir="$(mktemp -d)"
cleanup() {
  rm -rf "$workdir"
}
trap cleanup EXIT

cd "$workdir"
go mod init kubebrain-lease-expiry-smoke >/dev/null
go get go.etcd.io/etcd/client/v3@v3.5.2 >/dev/null

cat > main.go <<'GOEOF'
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func positiveInt(name string) int64 {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		log.Fatalf("invalid %s: %q", name, os.Getenv(name))
	}
	return value
}

func main() {
	endpoint := os.Getenv("ENDPOINT")
	ttlSeconds := positiveInt("TTL_SECONDS")
	leases := positiveInt("LEASES")
	timeoutSeconds := positiveInt("TIMEOUT_SECONDS")

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	prefix := fmt.Sprintf("/registry/lease-expiry-smoke/%d/", time.Now().UnixNano())
	startResp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		log.Fatal(err)
	}
	startRev := startResp.Header.Revision + 1

	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	watchCh := cli.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(startRev))

	expected := make(map[string]struct{}, leases)
	for i := int64(0); i < leases; i++ {
		key := fmt.Sprintf("%skey-%05d", prefix, i)
		leaseResp, err := cli.Grant(ctx, ttlSeconds)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := cli.Put(ctx, key, "leased", clientv3.WithLease(leaseResp.ID)); err != nil {
			log.Fatal(err)
		}

		ttlResp, err := cli.TimeToLive(ctx, leaseResp.ID, clientv3.WithAttachedKeys())
		if err != nil {
			log.Fatal(err)
		}
		if len(ttlResp.Keys) != 1 || string(ttlResp.Keys[0]) != key {
			log.Fatalf("expected attached key %q, got %q", key, ttlResp.Keys)
		}
		expected[key] = struct{}{}
	}

	seenPut := make(map[string]struct{}, leases)
	seenDelete := make(map[string]struct{}, leases)
	for int64(len(seenDelete)) < leases {
		select {
		case <-ctx.Done():
			log.Fatalf("timed out waiting for lease delete events; puts=%d/%d deletes=%d/%d: %v",
				len(seenPut), leases, len(seenDelete), leases, ctx.Err())
		case resp, ok := <-watchCh:
			if !ok {
				log.Fatalf("watch channel closed before all delete events; puts=%d/%d deletes=%d/%d",
					len(seenPut), leases, len(seenDelete), leases)
			}
			if err := resp.Err(); err != nil {
				log.Fatal(err)
			}
			for _, event := range resp.Events {
				key := string(event.Kv.Key)
				if _, ok := expected[key]; !ok {
					continue
				}
				switch event.Type {
				case clientv3.EventTypePut:
					seenPut[key] = struct{}{}
				case clientv3.EventTypeDelete:
					seenDelete[key] = struct{}{}
				}
			}
		}
	}

	finalResp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		log.Fatal(err)
	}
	if finalResp.Count != 0 {
		log.Fatalf("expected all keys to be deleted after lease expiry, got count=%d", finalResp.Count)
	}
	fmt.Printf("Lease expiry smoke completed: ttl=%d leases=%d saw_put=%d saw_delete=%d prefix=%s\n",
		ttlSeconds, leases, len(seenPut), len(seenDelete), prefix)
}
GOEOF

ENDPOINT="$ENDPOINT" TTL_SECONDS="$TTL_SECONDS" LEASES="$LEASES" TIMEOUT_SECONDS="$TIMEOUT_SECONDS" go run main.go
