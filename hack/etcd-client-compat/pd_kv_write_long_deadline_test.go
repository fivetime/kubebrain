package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pdKVWriteCase struct {
	kind  string
	key   string
	value string
}

type pdKVWriteResult struct {
	operation pdKVWriteCase
	err       error
}

// TestKVWritesWithLongDeadlineSurvivePDTotalLoss extends upstream's black-hole
// Put/Delete/Txn coverage across complete PD loss and durable leader startup.
// Overlapping mutations may return etcd's documented transient errors because
// clientv3 deliberately does not blindly replay non-idempotent writes. After
// recovery, the application first reads state to resolve ambiguity, retries
// only missing effects, and requires exactly one watch event per logical write.
func TestKVWritesWithLongDeadlineSurvivePDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_KV_WRITE_LONG_DEADLINE_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_KV_WRITE_LONG_DEADLINE_COMMAND to run destructive KV write overlap")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for KV write overlap")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	prefix := fmt.Sprintf("/compat/pd-kv-write-long/%d/", time.Now().UnixNano())
	during := pdKVWriteWave(prefix, "during")
	boundary := pdKVWriteWave(prefix, "boundary")
	for _, operation := range append(append([]pdKVWriteCase(nil), during...), boundary...) {
		if operation.kind != "delete" {
			continue
		}
		_, putErr := client.Put(ctx, operation.key, operation.value)
		require.NoError(t, putErr)
	}
	seed, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(seed.Header.Revision+1), clientv3.WithPrevKV())

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	commandWaited := false
	defer func() {
		if commandWaited {
			return
		}
		result := <-commandDone
		require.NoErrorf(t, result.err, "long-deadline KV write cleanup: %s", strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must become unreachable")
	duringDone := startPDKVWriteWave(ctx, client, during)
	assertNoPDKVWriteCompletion(t, duringDone, 2*time.Second)

	time.Sleep(23 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "boundary KV writes must start during PD total loss")
	boundaryDone := startPDKVWriteWave(ctx, client, boundary)
	assertNoPDKVWriteCompletion(t, boundaryDone, 2*time.Second)

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "long-deadline KV write command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("long-deadline KV write command: %s", strings.TrimSpace(string(result.output)))
	duringResults := collectPDKVWriteWave(t, ctx, duringDone, len(during))
	boundaryResults := collectPDKVWriteWave(t, ctx, boundaryDone, len(boundary))
	t.Logf("overlapped KV writes succeeded without reconciliation: during=%d boundary=%d",
		countSuccessfulPDKVWrites(duringResults), countSuccessfulPDKVWrites(boundaryResults))
	reconcilePDKVWrites(t, ctx, client, append(append([]pdKVWriteCase(nil), during...), boundary...))
	assertPDKVWriteState(t, ctx, client, append(append([]pdKVWriteCase(nil), during...), boundary...))
	assertPDKVWriteWatch(t, ctx, watch, append(append([]pdKVWriteCase(nil), during...), boundary...))
}

func pdKVWriteWave(prefix, name string) []pdKVWriteCase {
	return []pdKVWriteCase{
		{kind: "put", key: prefix + name + "/put", value: name + "-put"},
		{kind: "delete", key: prefix + name + "/delete", value: name + "-delete-seed"},
		{kind: "txn", key: prefix + name + "/txn", value: name + "-txn"},
	}
}

func startPDKVWriteWave(ctx context.Context, client *clientv3.Client, operations []pdKVWriteCase) <-chan pdKVWriteResult {
	results := make(chan pdKVWriteResult, len(operations))
	for _, operation := range operations {
		go func(operation pdKVWriteCase) {
			requestCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
			defer cancel()
			var err error
			switch operation.kind {
			case "put":
				_, err = client.Put(requestCtx, operation.key, operation.value)
			case "delete":
				var response *clientv3.DeleteResponse
				response, err = client.Delete(requestCtx, operation.key, clientv3.WithPrevKV())
				if err == nil && (response.Deleted != 1 || len(response.PrevKvs) != 1 || string(response.PrevKvs[0].Value) != operation.value) {
					err = fmt.Errorf("unexpected delete response: deleted=%d prev_kvs=%v", response.Deleted, response.PrevKvs)
				}
			case "txn":
				var response *clientv3.TxnResponse
				response, err = client.Txn(requestCtx).
					If(clientv3.Compare(clientv3.Version(operation.key), "=", 0)).
					Then(clientv3.OpPut(operation.key, operation.value)).
					Else(clientv3.OpGet(operation.key)).Commit()
				if err == nil && !response.Succeeded {
					err = fmt.Errorf("txn compare unexpectedly failed")
				}
			default:
				err = fmt.Errorf("unknown operation kind %q", operation.kind)
			}
			results <- pdKVWriteResult{operation: operation, err: err}
		}(operation)
	}
	return results
}

func assertNoPDKVWriteCompletion(t *testing.T, results <-chan pdKVWriteResult, duration time.Duration) {
	t.Helper()
	select {
	case result := <-results:
		require.Failf(t, "KV write completed during PD total loss", "kind=%s key=%s err=%v", result.operation.kind, result.operation.key, result.err)
	case <-time.After(duration):
	}
}

func collectPDKVWriteWave(t *testing.T, ctx context.Context, results <-chan pdKVWriteResult, count int) []pdKVWriteResult {
	t.Helper()
	collected := make([]pdKVWriteResult, 0, count)
	for range count {
		select {
		case result := <-results:
			if result.err != nil {
				requireTransientPDKVWriteError(t, result.err)
			}
			collected = append(collected, result)
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
	return collected
}

func requireTransientPDKVWriteError(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return
	default:
		require.Failf(t, "unexpected KV write overlap error", "code=%s err=%v", status.Code(err), err)
	}
}

func countSuccessfulPDKVWrites(results []pdKVWriteResult) int {
	count := 0
	for _, result := range results {
		if result.err == nil {
			count++
		}
	}
	return count
}

func reconcilePDKVWrites(t *testing.T, ctx context.Context, client *clientv3.Client, operations []pdKVWriteCase) {
	t.Helper()
	for _, operation := range operations {
		require.Eventually(t, func() bool {
			probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			response, err := client.Get(probeCtx, operation.key)
			if err != nil {
				requireTransientPDKVWriteError(t, err)
				return false
			}
			if operation.kind == "delete" {
				if len(response.Kvs) == 0 {
					return true
				}
			} else if len(response.Kvs) == 1 && string(response.Kvs[0].Value) == operation.value {
				return true
			}
			result := <-startPDKVWriteWave(ctx, client, []pdKVWriteCase{operation})
			if result.err != nil {
				requireTransientPDKVWriteError(t, result.err)
				return false
			}
			return true
		}, 90*time.Second, 500*time.Millisecond, "reconcile %s %s", operation.kind, operation.key)
	}
}

func assertPDKVWriteState(t *testing.T, ctx context.Context, client *clientv3.Client, operations []pdKVWriteCase) {
	t.Helper()
	for _, operation := range operations {
		response, err := client.Get(ctx, operation.key)
		require.NoError(t, err)
		if operation.kind == "delete" {
			require.Empty(t, response.Kvs)
			continue
		}
		require.Len(t, response.Kvs, 1)
		require.Equal(t, operation.value, string(response.Kvs[0].Value))
	}
}

func assertPDKVWriteWatch(t *testing.T, ctx context.Context, watch clientv3.WatchChan, operations []pdKVWriteCase) {
	t.Helper()
	want := make(map[string]pdKVWriteCase, len(operations))
	for _, operation := range operations {
		want[operation.key] = operation
	}
	seen := make(map[string]struct{}, len(want))
	for len(seen) < len(want) {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "KV write watch closed before all events")
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				operation, exists := want[string(event.Kv.Key)]
				require.Truef(t, exists, "unexpected event key %q", event.Kv.Key)
				_, duplicate := seen[operation.key]
				require.Falsef(t, duplicate, "duplicate event for %s", operation.key)
				if operation.kind == "delete" {
					require.Equal(t, mvccpb.DELETE, event.Type)
					require.NotNil(t, event.PrevKv)
					require.Equal(t, operation.value, string(event.PrevKv.Value))
				} else {
					require.Equal(t, mvccpb.PUT, event.Type)
					require.Equal(t, operation.value, string(event.Kv.Value))
				}
				seen[operation.key] = struct{}{}
			}
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
}
