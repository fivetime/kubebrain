// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	endpoints := splitRequired("ENDPOINTS")
	victimEndpoint := required("VICTIM_ENDPOINT")
	stateDir := required("STATE_DIR")
	if len(endpoints) != 3 {
		log.Fatalf("ENDPOINTS must contain exactly three endpoints, got %d", len(endpoints))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-balancer-smoke/%d/", time.Now().UnixNano())
	watchPrefix := prefix + "watch/"

	cleanup, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup.Close()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, deleteErr := cleanup.Delete(cleanupCtx, prefix, clientv3.WithPrefix()); deleteErr != nil {
			log.Printf("cleanup failed: %v", deleteErr)
		}
	}()

	unary := newPinnedClient(victimEndpoint)
	defer unary.Close()
	if _, err := unary.Get(ctx, prefix+"pin"); err != nil {
		log.Fatalf("pin unary connection: %v", err)
	}
	syncAndRequireEndpoints(ctx, unary, endpoints)

	watchClient := newAutoSyncPinnedClient(victimEndpoint)
	defer watchClient.Close()
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh := watchClient.Watch(watchCtx, watchPrefix, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	select {
	case response := <-watchCh:
		if err := response.Err(); err != nil || !response.Created {
			log.Fatalf("create pinned watch: created=%v err=%v", response.Created, err)
		}
	case <-ctx.Done():
		log.Fatalf("create pinned watch: %v", ctx.Err())
	}
	waitAndRequireAutoSyncedEndpoints(ctx, watchClient, endpoints)

	writeMarker(stateDir, "ready")
	waitMarker(ctx, stateDir, "deleted")

	const writes = 20
	transientFailures := createEventually(ctx, unary, watchPrefix+"00", "value-00")
	for index := 1; index < writes; index++ {
		callCtx, callCancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := unary.Put(callCtx, watchPrefix+fmt.Sprintf("%02d", index), fmt.Sprintf("value-%02d", index))
		callCancel()
		if err != nil {
			log.Fatalf("put after victim deletion at %d: %v", index, err)
		}
	}

	lastRevision := int64(0)
	for index := 0; index < writes; {
		select {
		case response, ok := <-watchCh:
			if !ok {
				log.Fatal("watch closed during endpoint failover")
			}
			if err := response.Err(); err != nil {
				log.Fatalf("watch failed during endpoint failover: %v", err)
			}
			for _, event := range response.Events {
				expectedKey := watchPrefix + fmt.Sprintf("%02d", index)
				expectedValue := fmt.Sprintf("value-%02d", index)
				if string(event.Kv.Key) != expectedKey || string(event.Kv.Value) != expectedValue ||
					event.Kv.ModRevision <= lastRevision {
					log.Fatalf("watch event %d mismatch: key=%q value=%q revision=%d previous=%d",
						index, event.Kv.Key, event.Kv.Value, event.Kv.ModRevision, lastRevision)
				}
				lastRevision = event.Kv.ModRevision
				index++
			}
		case <-ctx.Done():
			log.Fatalf("waiting for failover watch events: %v", ctx.Err())
		}
	}
	waitMarker(ctx, stateDir, "replaced")

	getResponse, err := unary.Get(ctx, watchPrefix+"19")
	if err != nil || len(getResponse.Kvs) != 1 || string(getResponse.Kvs[0].Value) != "value-19" {
		log.Fatalf("linearizable get after failover: count=%d err=%v", len(getResponse.Kvs), err)
	}
	staleSerializableReads := serializableGetEventually(ctx, unary, watchPrefix+"19", "value-19")
	txnKey := prefix + "txn"
	txn, err := unary.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(txnKey), "=", 0)).
		Then(clientv3.OpPut(txnKey, "created")).Commit()
	if err != nil || !txn.Succeeded {
		log.Fatalf("txn after failover: succeeded=%v err=%v", txn != nil && txn.Succeeded, err)
	}
	putKey := prefix + "put"
	if _, err := unary.Put(ctx, putKey, "created"); err != nil {
		log.Fatalf("put after failover recovery: %v", err)
	}
	deleted, err := unary.Delete(ctx, putKey)
	deletedCount := int64(0)
	if deleted != nil {
		deletedCount = deleted.Deleted
	}
	if err != nil || deletedCount != 1 {
		log.Fatalf("delete after failover: deleted=%d err=%v", deletedCount, err)
	}
	fmt.Printf("balancer smoke completed: synced_endpoints=3 autosynced_endpoints=3 watch_events=%d last_revision=%d transient_failures=%d stale_serializable_reads=%d replacement_ready=true\n",
		writes, lastRevision, transientFailures, staleSerializableReads)
}

func syncAndRequireEndpoints(ctx context.Context, client *clientv3.Client, expected []string) {
	if err := client.Sync(ctx); err != nil {
		log.Fatalf("sync advertised endpoints: %v", err)
	}
	want := append([]string(nil), expected...)
	got := append([]string(nil), client.Endpoints()...)
	sort.Strings(want)
	sort.Strings(got)
	if len(got) != len(want) {
		log.Fatalf("synced endpoint count mismatch: got=%q want=%q", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			log.Fatalf("synced endpoints mismatch: got=%q want=%q", got, want)
		}
		if index > 0 && got[index] == got[index-1] {
			log.Fatalf("synced endpoints are not unique: %q", got)
		}
	}
}

func waitAndRequireAutoSyncedEndpoints(ctx context.Context, client *clientv3.Client, expected []string) {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	want := append([]string(nil), expected...)
	sort.Strings(want)
	for {
		got := append([]string(nil), client.Endpoints()...)
		sort.Strings(got)
		if equalUniqueEndpoints(got, want) {
			return
		}
		select {
		case <-waitCtx.Done():
			log.Fatalf("auto-synced endpoints did not converge: got=%q want=%q: %v", got, want, waitCtx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func equalUniqueEndpoints(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] || (index > 0 && got[index] == got[index-1]) {
			return false
		}
	}
	return true
}

func createEventually(ctx context.Context, client *clientv3.Client, key, value string) int {
	transientFailures := 0
	for {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := client.Txn(callCtx).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value)).Commit()
		callCancel()
		if err == nil && response.Succeeded {
			return transientFailures
		}
		if err == nil {
			getResponse, getErr := client.Get(ctx, key)
			if getErr == nil && len(getResponse.Kvs) == 1 && string(getResponse.Kvs[0].Value) == value {
				return transientFailures
			}
			log.Fatalf("reconcile first failover write: count=%d err=%v", len(getResponse.Kvs), getErr)
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			log.Fatalf("first failover write returned non-transient error: %v", err)
		}
		transientFailures++
		select {
		case <-ctx.Done():
			log.Fatalf("first failover write did not recover after %d transient failures: %v", transientFailures, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func serializableGetEventually(ctx context.Context, client *clientv3.Client, key, value string) int {
	staleReads := 0
	for {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := client.Get(callCtx, key, clientv3.WithSerializable())
		callCancel()
		if err == nil && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == value {
			return staleReads
		}
		if err != nil && status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			log.Fatalf("serializable get after failover returned non-transient error: %v", err)
		}
		staleReads++
		select {
		case <-ctx.Done():
			log.Fatalf("serializable get did not converge after %d stale/transient reads: %v", staleReads, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func newPinnedClient(endpoint string) *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func newAutoSyncPinnedClient(endpoint string) *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, AutoSyncInterval: 100 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func splitRequired(name string) []string {
	parts := strings.Split(required(name), ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func writeMarker(stateDir, name string) {
	if err := os.WriteFile(filepath.Join(stateDir, name), []byte("ok\n"), 0o600); err != nil {
		log.Fatal(err)
	}
}

func waitMarker(ctx context.Context, stateDir, name string) {
	marker := filepath.Join(stateDir, name)
	for {
		if _, err := os.Stat(marker); err == nil {
			return
		} else if !os.IsNotExist(err) {
			log.Fatal(err)
		}
		select {
		case <-ctx.Done():
			log.Fatalf("waiting for marker %s: %v", name, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
