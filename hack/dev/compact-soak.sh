#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
ITERATIONS="${ITERATIONS:-20}"
READERS="${READERS:-4}"
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
go mod init kubebrain-compact-soak >/dev/null
go get go.etcd.io/etcd/client/v3@v3.5.2 >/dev/null

cat > main.go <<'GOEOF'
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const compactRevKey = "compact_rev_key"

func positiveInt(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		log.Fatalf("invalid %s: %q", name, os.Getenv(name))
	}
	return value
}

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	default:
		return false
	}
}

func retry(ctx context.Context, label string, fn func() error) error {
	var lastErr error
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if !retryable(err) {
			return fmt.Errorf("%s: %w", label, err)
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", label, lastErr)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func expectCompactedRange(ctx context.Context, cli *clientv3.Client, key string, rev int64) error {
	_, err := cli.Get(ctx, key, clientv3.WithRev(rev))
	if err == nil {
		return fmt.Errorf("range at compacted revision %d unexpectedly succeeded", rev)
	}
	return nil
}

func expectCompactedWatch(ctx context.Context, cli *clientv3.Client, key string, rev int64) error {
	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	watchCh := cli.Watch(watchCtx, key, clientv3.WithRev(rev))
	for resp := range watchCh {
		if resp.Err() == nil {
			continue
		}
		if !resp.Canceled {
			return fmt.Errorf("watch at compacted revision %d returned error without canceled flag: %v", rev, resp.Err())
		}
		if resp.CompactRevision < rev {
			return fmt.Errorf("watch compact revision %d is older than requested revision %d", resp.CompactRevision, rev)
		}
		return nil
	}
	return fmt.Errorf("watch at compacted revision %d closed without compacted response", rev)
}

func kubernetesCompact(ctx context.Context, cli *clientv3.Client, expectVersion, rev int64) (currentVersion, currentRev, compactRev int64, err error) {
	resp, err := cli.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(compactRevKey), "=", expectVersion),
	).Then(
		clientv3.OpPut(compactRevKey, strconv.FormatInt(rev, 10)),
	).Else(
		clientv3.OpGet(compactRevKey),
	).Commit()
	if err != nil {
		return expectVersion, rev, 0, err
	}

	currentRev = resp.Header.Revision
	if !resp.Succeeded {
		currentVersion = resp.Responses[0].GetResponseRange().Kvs[0].Version
		compactRev, err = strconv.ParseInt(string(resp.Responses[0].GetResponseRange().Kvs[0].Value), 10, 64)
		if err != nil {
			return currentVersion, currentRev, 0, nil
		}
		return currentVersion, currentRev, compactRev, nil
	}
	currentVersion = expectVersion + 1

	if rev == 0 {
		return currentVersion, currentRev, 0, nil
	}
	if _, err = cli.Compact(ctx, rev); err != nil {
		return currentVersion, currentRev, 0, err
	}
	return currentVersion, currentRev, rev, nil
}

func main() {
	endpoint := os.Getenv("ENDPOINT")
	iterations := positiveInt("ITERATIONS")
	readers := positiveInt("READERS")
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

	prefix := fmt.Sprintf("/registry/compact-soak/%d/", time.Now().UnixNano())
	readerCtx, stopReaders := context.WithCancel(ctx)
	var readerOps atomic.Int64
	var readerFailures atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-readerCtx.Done():
					return
				default:
				}
				if err := retry(readerCtx, "latest reader range", func() error {
					_, err := cli.Get(readerCtx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
					return err
				}); err != nil {
					if readerCtx.Err() != nil {
						return
					}
					readerFailures.Add(1)
					log.Printf("reader %d latest range failed: %v", readerID, err)
					return
				}
				readerOps.Add(1)
				time.Sleep(25 * time.Millisecond)
			}
		}(i)
	}

	var compactVersion int64
	var compactTarget int64
	var previousOldKey string
	for i := 0; i < iterations; i++ {
		oldKey := fmt.Sprintf("%sold-%04d", prefix, i)
		newKey := fmt.Sprintf("%snew-%04d", prefix, i)
		var oldResp *clientv3.PutResponse
		if err := retry(ctx, "old put", func() error {
			var err error
			oldResp, err = cli.Put(ctx, oldKey, "old")
			return err
		}); err != nil {
			log.Fatal(err)
		}
		if err := retry(ctx, "new put", func() error {
			_, err := cli.Put(ctx, newKey, "new")
			return err
		}); err != nil {
			log.Fatal(err)
		}
		var currentRev int64
		var compactedRev int64
		if err := retry(ctx, "kubernetes compact", func() error {
			var err error
			compactVersion, currentRev, compactedRev, err = kubernetesCompact(ctx, cli, compactVersion, compactTarget)
			return err
		}); err != nil {
			log.Fatal(err)
		}
		if compactTarget > 0 {
			if compactedRev != compactTarget {
				log.Fatalf("compacted revision %d, want %d", compactedRev, compactTarget)
			}
			if currentRev < compactTarget {
				log.Fatalf("current revision %d is older than compact target %d", currentRev, compactTarget)
			}
			if err := expectCompactedRange(ctx, cli, previousOldKey, compactedRev-1); err != nil {
				log.Fatal(err)
			}
			if err := expectCompactedWatch(ctx, cli, previousOldKey, compactedRev-1); err != nil {
				log.Fatal(err)
			}
		}
		compactTarget = oldResp.Header.Revision
		previousOldKey = oldKey
		if i%5 == 0 {
			fmt.Printf("compact iteration %d/%d ok old_rev=%d compact_target=%d compacted_rev=%d current_rev=%d\n", i+1, iterations, oldResp.Header.Revision, compactTarget, compactedRev, currentRev)
		}
	}

	if compactTarget > 0 {
		var currentRev int64
		var compactedRev int64
		if err := retry(ctx, "final kubernetes compact", func() error {
			var err error
			compactVersion, currentRev, compactedRev, err = kubernetesCompact(ctx, cli, compactVersion, compactTarget)
			return err
		}); err != nil {
			log.Fatal(err)
		}
		if compactedRev != compactTarget {
			log.Fatalf("final compacted revision %d, want %d", compactedRev, compactTarget)
		}
		if currentRev < compactTarget {
			log.Fatalf("final current revision %d is older than compact target %d", currentRev, compactTarget)
		}
		if err := expectCompactedRange(ctx, cli, previousOldKey, compactedRev-1); err != nil {
			log.Fatal(err)
		}
		if err := expectCompactedWatch(ctx, cli, previousOldKey, compactedRev-1); err != nil {
			log.Fatal(err)
		}
	}

	stopReaders()
	wg.Wait()
	if readerFailures.Load() != 0 {
		log.Fatalf("latest readers failed: %d", readerFailures.Load())
	}

	if _, err := cli.Delete(context.Background(), prefix, clientv3.WithPrefix()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Compact soak completed: iterations=%d readers=%d reader_ops=%d prefix=%s\n",
		iterations, readers, readerOps.Load(), prefix)
}
GOEOF

ENDPOINT="$ENDPOINT" ITERATIONS="$ITERATIONS" READERS="$READERS" TIMEOUT_SECONDS="$TIMEOUT_SECONDS" go run main.go
