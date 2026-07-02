#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
WORKERS="${WORKERS:-8}"
OPS_PER_WORKER="${OPS_PER_WORKER:-25}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"

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
go mod init kubebrain-load-smoke >/dev/null
go get go.etcd.io/etcd/client/v3@v3.5.2 >/dev/null

cat > main.go <<'GOEOF'
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func positiveInt(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		log.Fatalf("invalid %s: %q", name, os.Getenv(name))
	}
	return value
}

type recorder struct {
	mu        sync.Mutex
	latencyUS []int64
}

func (r *recorder) record(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latencyUS = append(r.latencyUS, d.Microseconds())
}

func (r *recorder) percentiles() (p50, p95, p99 int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := append([]int64(nil), r.latencyUS...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return 0, 0, 0
	}
	at := func(p float64) int64 {
		idx := int(float64(len(values)-1) * p)
		return values[idx]
	}
	return at(0.50), at(0.95), at(0.99)
}

func main() {
	endpoint := os.Getenv("ENDPOINT")
	workers := positiveInt("WORKERS")
	opsPerWorker := positiveInt("OPS_PER_WORKER")
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

	prefix := fmt.Sprintf("/registry/load-smoke/%d/", time.Now().UnixNano())
	var success atomic.Int64
	var failures atomic.Int64
	rec := &recorder{}
	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("%sworker-%03d/key-%05d", prefix, worker, i)
				start := time.Now()
				createResp, err := cli.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
					Then(clientv3.OpPut(key, "v1")).
					Commit()
				if err != nil || !createResp.Succeeded {
					failures.Add(1)
					errCh <- fmt.Errorf("create %s failed succeeded=%v err=%v", key, createResp != nil && createResp.Succeeded, err)
					return
				}

				getResp, err := cli.Get(ctx, key)
				if err != nil || getResp.Count != 1 {
					failures.Add(1)
					var count int64
					if getResp != nil {
						count = getResp.Count
					}
					errCh <- fmt.Errorf("get %s failed count=%d err=%v", key, count, err)
					return
				}

				updateResp, err := cli.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", getResp.Kvs[0].ModRevision)).
					Then(clientv3.OpPut(key, "v2")).
					Commit()
				if err != nil || !updateResp.Succeeded {
					failures.Add(1)
					errCh <- fmt.Errorf("update %s failed succeeded=%v err=%v", key, updateResp != nil && updateResp.Succeeded, err)
					return
				}

				deleteResp, err := cli.Delete(ctx, key)
				if err != nil || deleteResp.Deleted != 1 {
					failures.Add(1)
					var deleted int64
					if deleteResp != nil {
						deleted = deleteResp.Deleted
					}
					errCh <- fmt.Errorf("delete %s failed deleted=%d err=%v", key, deleted, err)
					return
				}
				rec.record(time.Since(start))
				success.Add(1)
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := cli.Delete(context.Background(), prefix, clientv3.WithPrefix()); err != nil {
		log.Fatal(err)
	}

	p50, p95, p99 := rec.percentiles()
	total := success.Load()
	fmt.Printf("Load smoke completed: workers=%d ops_per_worker=%d successful_ops=%d failures=%d p50_us=%d p95_us=%d p99_us=%d prefix=%s\n",
		workers, opsPerWorker, total, failures.Load(), p50, p95, p99, prefix)
}
GOEOF

ENDPOINT="$ENDPOINT" WORKERS="$WORKERS" OPS_PER_WORKER="$OPS_PER_WORKER" TIMEOUT_SECONDS="$TIMEOUT_SECONDS" go run main.go
