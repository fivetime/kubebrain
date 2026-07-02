#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
WATCHERS="${WATCHERS:-25}"
EVENTS="${EVENTS:-50}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"

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
go mod init kubebrain-watch-soak >/dev/null
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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func mustPositiveInt(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		log.Fatalf("invalid %s: %q", name, os.Getenv(name))
	}
	return value
}

func main() {
	endpoint := os.Getenv("ENDPOINT")
	watchers := mustPositiveInt("WATCHERS")
	events := mustPositiveInt("EVENTS")
	timeoutSeconds := mustPositiveInt("TIMEOUT_SECONDS")

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	prefix := fmt.Sprintf("/registry/watch-soak/%d/", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		log.Fatal(err)
	}
	startRevision := resp.Header.Revision + 1

	errCh := make(chan error, watchers)
	var wg sync.WaitGroup
	for i := 0; i < watchers; i++ {
		watcherID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			watchCh := cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(startRevision), clientv3.WithProgressNotify())
			for count < events {
				select {
				case <-ctx.Done():
					errCh <- fmt.Errorf("watcher %d timed out after %d/%d events: %w", watcherID, count, events, ctx.Err())
					return
				case watchResp, ok := <-watchCh:
					if !ok {
						errCh <- fmt.Errorf("watcher %d channel closed after %d/%d events", watcherID, count, events)
						return
					}
					if err := watchResp.Err(); err != nil {
						errCh <- fmt.Errorf("watcher %d error after %d/%d events: %w", watcherID, count, events, err)
						return
					}
					count += len(watchResp.Events)
				}
			}
			fmt.Printf("watcher %d received %d events\n", watcherID, count)
		}()
	}

	// Give all watcher streams a short window to register before writes start.
	time.Sleep(2 * time.Second)

	for i := 0; i < events; i++ {
		key := fmt.Sprintf("%sevent-%04d", prefix, i)
		value := fmt.Sprintf("value-%04d", i)
		if _, err := cli.Put(ctx, key, value); err != nil {
			log.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := cli.Delete(context.Background(), prefix, clientv3.WithPrefix()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Watch soak completed: watchers=%d events=%d prefix=%s\n", watchers, events, prefix)
}
GOEOF

ENDPOINT="$ENDPOINT" WATCHERS="$WATCHERS" EVENTS="$EVENTS" TIMEOUT_SECONDS="$TIMEOUT_SECONDS" go run main.go
