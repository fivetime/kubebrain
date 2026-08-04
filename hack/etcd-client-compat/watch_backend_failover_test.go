package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestWatchDeliversCommittedWritesAcrossBackendFailover keeps one explicit-
// revision Watch open while the supplied command disrupts the external PD/TiKV
// backend. A final linearizable Range is the committed-state oracle: every key
// it contains must have been delivered exactly once, with the same value and
// revision, and Watch event revisions must never regress or repeat.
func TestWatchDeliversCommittedWritesAcrossBackendFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_WATCH_BACKEND_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_WATCH_BACKEND_FAILOVER_COMMAND to run destructive backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for watch backend failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	seed, err := cli.Put(ctx, prefix+"seed", "seed")
	require.NoError(t, err)
	watchCtx, stopWatch := context.WithCancel(ctx)
	watch := cli.Watch(watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(seed.Header.Revision+1), clientv3.WithCreatedNotify())

	type deliveredEvent struct {
		value    string
		revision int64
	}
	var (
		eventsMu      sync.Mutex
		events        = make(map[string][]deliveredEvent)
		eventOrder    []int64
		watchResponse atomic.Int64
	)
	created := make(chan struct{})
	watchErr := make(chan error, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		createdOnce := sync.Once{}
		for response := range watch {
			watchResponse.Add(1)
			if response.Err() != nil {
				watchErr <- response.Err()
				return
			}
			if response.Created {
				createdOnce.Do(func() { close(created) })
			}
			eventsMu.Lock()
			for _, event := range response.Events {
				if event.Type != mvccpb.PUT || event.Kv == nil {
					eventsMu.Unlock()
					watchErr <- fmt.Errorf("unexpected watch event: %#v", event)
					return
				}
				key := string(event.Kv.Key)
				events[key] = append(events[key], deliveredEvent{
					value: string(event.Kv.Value), revision: event.Kv.ModRevision,
				})
				eventOrder = append(eventOrder, event.Kv.ModRevision)
			}
			eventsMu.Unlock()
		}
		if watchCtx.Err() == nil {
			watchErr <- fmt.Errorf("watch channel closed while backend failover test was active")
		}
	}()
	defer func() {
		stopWatch()
		<-watchDone
	}()
	select {
	case <-created:
	case err := <-watchErr:
		require.NoError(t, err)
	case <-ctx.Done():
		require.NoError(t, ctx.Err(), "watch did not report creation")
	}

	writerStop := make(chan struct{})
	writerDone := make(chan struct{})
	writerErr := make(chan error, 1)
	var successfulWrites, transientWrites atomic.Int64
	go func() {
		defer close(writerDone)
		for operation := 0; ; operation++ {
			select {
			case <-writerStop:
				return
			default:
			}
			callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
			_, putErr := cli.Put(callCtx,
				fmt.Sprintf("%sevent-%08d", prefix, operation), fmt.Sprintf("value-%08d", operation))
			callCancel()
			if putErr != nil {
				if isMutationFailoverAmbiguous(putErr) {
					transientWrites.Add(1)
					continue
				}
				writerErr <- putErr
				return
			}
			successfulWrites.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	var writerStopOnce sync.Once
	defer func() {
		writerStopOnce.Do(func() { close(writerStop) })
		<-writerDone
	}()
	require.Eventually(t, func() bool { return successfulWrites.Load() >= 8 },
		10*time.Second, 10*time.Millisecond, "writer must begin before backend failover")

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "backend failover command: %s", strings.TrimSpace(string(output)))
	require.Eventually(t, func() bool { return transientWrites.Load() > 0 },
		15*time.Second, 10*time.Millisecond, "backend fault must overlap at least one writer request")
	recoveryBaseline := successfulWrites.Load()
	require.Eventually(t, func() bool { return successfulWrites.Load() >= recoveryBaseline+8 },
		45*time.Second, 100*time.Millisecond, "writes must recover after backend failover")
	writerStopOnce.Do(func() { close(writerStop) })
	<-writerDone
	select {
	case writerFailure := <-writerErr:
		require.NoError(t, writerFailure)
	default:
	}

	final, err := cli.Get(ctx, prefix+"event-", clientv3.WithPrefix())
	require.NoError(t, err)
	require.NotEmpty(t, final.Kvs)
	catchUpDeadline := time.NewTimer(30 * time.Second)
	defer catchUpDeadline.Stop()
	catchUpTicker := time.NewTicker(50 * time.Millisecond)
	defer catchUpTicker.Stop()
	for {
		eventsMu.Lock()
		deliveredCount := len(events)
		eventsMu.Unlock()
		if deliveredCount >= len(final.Kvs) {
			break
		}
		select {
		case watchFailure := <-watchErr:
			require.NoErrorf(t, watchFailure,
				"watch terminated before catch-up: delivered=%d committed=%d", deliveredCount, len(final.Kvs))
		case <-catchUpDeadline.C:
			require.FailNowf(t, "watch must catch up to the final committed key set",
				"delivered=%d committed=%d responses=%d", deliveredCount, len(final.Kvs), watchResponse.Load())
		case <-catchUpTicker.C:
		}
	}

	eventsMu.Lock()
	deliveredSnapshot := make(map[string][]deliveredEvent, len(events))
	for key, delivered := range events {
		deliveredSnapshot[key] = append([]deliveredEvent(nil), delivered...)
	}
	orderSnapshot := append([]int64(nil), eventOrder...)
	eventsMu.Unlock()
	require.Len(t, deliveredSnapshot, len(final.Kvs), "watch must not deliver keys absent from final state")
	for _, kv := range final.Kvs {
		delivered := deliveredSnapshot[string(kv.Key)]
		require.Len(t, delivered, 1, "committed key %q must be delivered exactly once", kv.Key)
		require.Equal(t, string(kv.Value), delivered[0].value, "key %q value", kv.Key)
		require.Equal(t, kv.ModRevision, delivered[0].revision, "key %q revision", kv.Key)
	}
	for i := 1; i < len(orderSnapshot); i++ {
		require.Greater(t, orderSnapshot[i], orderSnapshot[i-1],
			"watch event revisions must be strictly increasing without replay")
	}
	require.Positive(t, watchResponse.Load())
	t.Logf("committed and delivered events=%d watch responses=%d transient writes=%d final revision=%d",
		len(final.Kvs), watchResponse.Load(), transientWrites.Load(), final.Header.Revision)

	stopWatch()
	<-watchDone
	select {
	case watchFailure := <-watchErr:
		require.NoError(t, watchFailure)
	default:
	}
}

// TestWatchCreationSurvivesBackendFailover starts a Watch on a directly
// addressed follower after a mutation has proved that the backend/leader path
// is unavailable. The ingress RPC may acknowledge creation before its proxy
// has a usable successor. That transient creation window must remain an open
// Watch and deliver a post-recovery write instead of becoming a terminal
// cancellation.
func TestWatchCreationSurvivesBackendFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_WATCH_CREATE_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_WATCH_CREATE_FAILOVER_COMMAND to run destructive watch-creation failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_WATCH_CREATE_FAILOVER_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_WATCH_CREATE_FAILOVER_ENDPOINT to a directly addressed follower")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	seed, err := cli.Put(ctx, prefix+"seed", "seed")
	require.NoError(t, err)
	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, failoverCommand)
		commandDone <- commandResult{output: output, err: commandErr}
	}()

	transientWrites := 0
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		_, putErr := cli.Put(callCtx, prefix+"window", fmt.Sprintf("attempt-%d", transientWrites))
		callCancel()
		if putErr == nil {
			time.Sleep(10 * time.Millisecond)
			return false
		}
		require.Truef(t, isMutationFailoverAmbiguous(putErr), "unexpected mutation failure: %v", putErr)
		transientWrites++
		return true
	}, 30*time.Second, 10*time.Millisecond, "fault must expose a mutation-unavailable window")
	select {
	case result := <-commandDone:
		require.FailNowf(t, "failover command ended before Watch creation",
			"error=%v output=%s", result.err, strings.TrimSpace(string(result.output)))
	default:
	}

	watchStarted := time.Now()
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	watch := cli.Watch(watchCtx, prefix, clientv3.WithPrefix(),
		clientv3.WithRev(seed.Header.Revision+1), clientv3.WithCreatedNotify())
	created := false
	for !created {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "Watch closed before its Created response")
			require.NoError(t, response.Err())
			created = response.Created
		case <-time.After(10 * time.Second):
			t.Fatal("Watch did not report creation during the failover window")
		}
	}

	result := <-commandDone
	require.NoErrorf(t, result.err, "backend failover command: %s", strings.TrimSpace(string(result.output)))
	probeKey := prefix + "post-recovery"
	probeValue := fmt.Sprintf("probe-%d", time.Now().UnixNano())
	var probe *clientv3.PutResponse
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		defer callCancel()
		probe, err = cli.Put(callCtx, probeKey, probeValue)
		return err == nil
	}, 45*time.Second, 100*time.Millisecond, "writes must recover after backend failover")

	for {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "Watch created during failover closed before the recovery probe")
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if event.Type == mvccpb.PUT && event.Kv != nil && string(event.Kv.Key) == probeKey {
					require.Equal(t, probeValue, string(event.Kv.Value))
					require.Equal(t, probe.Header.Revision, event.Kv.ModRevision)
					t.Logf("Watch created in confirmed outage after %s delivered recovery revision=%d transient_writes=%d",
						time.Since(watchStarted), event.Kv.ModRevision, transientWrites)
					return
				}
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Watch created during failover did not deliver the post-recovery probe")
		}
	}
}
