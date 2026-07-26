// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientExperimentalRecipesBarrierAndQueues(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Context:     ctx,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	barrier := recipe.NewBarrier(client, "/a968/recipes/barrier")
	require.NoError(t, barrier.Hold())
	require.Error(t, barrier.Hold())
	waitResults := make(chan error, 3)
	for range 3 {
		go func() { waitResults <- recipe.NewBarrier(client, "/a968/recipes/barrier").Wait() }()
	}
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, waitResults, "barrier waiters must block until Release")
	require.NoError(t, barrier.Release())
	for range 3 {
		select {
		case waitErr := <-waitResults:
			require.NoError(t, waitErr)
		case <-ctx.Done():
			t.Fatalf("barrier waiters did not release: %v", ctx.Err())
		}
	}

	_, err = client.Put(ctx, "/a968/recipes/nonexistent-neighbor", "value")
	require.NoError(t, err)
	neighborWait := make(chan error, 1)
	go func() { neighborWait <- recipe.NewBarrier(client, "/a968/recipes/nonexistent").Wait() }()
	select {
	case waitErr := <-neighborWait:
		require.NoError(t, waitErr)
	case <-time.After(time.Second):
		t.Fatal("barrier wait on a nonexistent exact key was blocked by its prefix neighbor")
	}

	fifo := recipe.NewQueue(client, "/a968/recipes/fifo")
	for _, value := range []string{"zero", "one", "two", "three"} {
		require.NoError(t, fifo.Enqueue(value))
	}
	for _, want := range []string{"zero", "one", "two", "three"} {
		got, dequeueErr := fifo.Dequeue()
		require.NoError(t, dequeueErr)
		require.Equal(t, want, got)
	}

	priority := recipe.NewPriorityQueue(client, "/a968/recipes/priority")
	for _, item := range []struct {
		value    string
		priority uint16
	}{
		{value: "two-a", priority: 2},
		{value: "zero-a", priority: 0},
		{value: "one", priority: 1},
		{value: "zero-b", priority: 0},
		{value: "two-b", priority: 2},
	} {
		require.NoError(t, priority.Enqueue(item.value, item.priority))
	}
	for _, want := range []string{"zero-a", "zero-b", "one", "two-a", "two-b"} {
		got, dequeueErr := priority.Dequeue()
		require.NoError(t, dequeueErr)
		require.Equal(t, want, got)
	}

	concurrent := recipe.NewQueue(client, fmt.Sprintf("/a1056/recipes/concurrent/%d", time.Now().UnixNano()))
	const writerCount = 3
	const itemsPerWriter = 3
	var writers sync.WaitGroup
	writerErrors := make(chan error, writerCount)
	for writer := range writerCount {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			for item := range itemsPerWriter {
				if enqueueErr := concurrent.Enqueue(fmt.Sprintf("writer-%d-item-%d", writer, item)); enqueueErr != nil {
					writerErrors <- enqueueErr
					return
				}
			}
		}(writer)
	}
	writers.Wait()
	close(writerErrors)
	for writerErr := range writerErrors {
		require.NoError(t, writerErr)
	}

	const readerCount = 3
	values := make(chan string, writerCount*itemsPerWriter)
	readerErrors := make(chan error, readerCount)
	var readers sync.WaitGroup
	for range readerCount {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range itemsPerWriter {
				value, dequeueErr := concurrent.Dequeue()
				if dequeueErr != nil {
					readerErrors <- dequeueErr
					return
				}
				values <- value
			}
		}()
	}
	readers.Wait()
	close(readerErrors)
	close(values)
	for readerErr := range readerErrors {
		require.NoError(t, readerErr)
	}
	concurrentValues := make([]string, 0, writerCount*itemsPerWriter)
	for value := range values {
		concurrentValues = append(concurrentValues, value)
	}
	sort.Strings(concurrentValues)
	require.Equal(t, []string{
		"writer-0-item-0", "writer-0-item-1", "writer-0-item-2",
		"writer-1-item-0", "writer-1-item-1", "writer-1-item-2",
		"writer-2-item-0", "writer-2-item-1", "writer-2-item-2",
	}, concurrentValues)
}

func TestClientExperimentalLockRecipesOrderingAndSessionCleanup(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	newSession := func() *concurrency.Session {
		session, sessionErr := concurrency.NewSession(client, concurrency.WithTTL(10))
		require.NoError(t, sessionErr)
		return session
	}

	rwName := "/a969/lock-recipes/rw"
	readerOneSession, readerTwoSession := newSession(), newSession()
	writerSession, lateReaderSession := newSession(), newSession()
	t.Cleanup(readerOneSession.Orphan)
	t.Cleanup(readerTwoSession.Orphan)
	t.Cleanup(writerSession.Orphan)
	t.Cleanup(lateReaderSession.Orphan)
	readerOne := recipe.NewRWMutex(readerOneSession, rwName)
	readerTwo := recipe.NewRWMutex(readerTwoSession, rwName)
	writer := recipe.NewRWMutex(writerSession, rwName)
	lateReader := recipe.NewRWMutex(lateReaderSession, rwName)
	require.NoError(t, readerOne.RLock())
	require.NoError(t, readerTwo.RLock())
	require.Equal(t, 2, recipePrefixCount(ctx, client, rwName+"/read"))

	writerResult := make(chan error, 1)
	go func() { writerResult <- writer.Lock() }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, rwName+"/write") == 1
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, writerResult, "writer must wait for earlier readers")

	lateReaderResult := make(chan error, 1)
	go func() { lateReaderResult <- lateReader.RLock() }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, rwName+"/read") == 3
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, lateReaderResult, "late reader must wait behind queued writer")

	require.NoError(t, readerOne.RUnlock())
	require.NoError(t, readerTwo.RUnlock())
	require.NoError(t, waitRecipeResult(ctx, writerResult, "writer acquisition"))
	require.Empty(t, lateReaderResult, "writer must acquire before the late reader")
	require.NoError(t, writer.Unlock())
	require.NoError(t, waitRecipeResult(ctx, lateReaderResult, "late reader acquisition"))
	require.NoError(t, lateReader.RUnlock())
	require.Equal(t, 0, recipePrefixCount(ctx, client, rwName+"/"))

	mutexName := "/a969/lock-recipes/mutex"
	ownerSession, victimSession, successorSession := newSession(), newSession(), newSession()
	t.Cleanup(ownerSession.Orphan)
	t.Cleanup(successorSession.Orphan)
	owner := concurrency.NewMutex(ownerSession, mutexName)
	victim := concurrency.NewMutex(victimSession, mutexName)
	successor := concurrency.NewMutex(successorSession, mutexName)
	require.NoError(t, owner.Lock(ctx))
	victimResult := make(chan error, 1)
	go func() { victimResult <- victim.Lock(ctx) }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, mutexName) == 2
	}, 5*time.Second, 20*time.Millisecond)
	successorResult := make(chan error, 1)
	go func() { successorResult <- successor.Lock(ctx) }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, mutexName) == 3
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, victimSession.Close())
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, mutexName) == 2
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, successorResult, "successor must wait while owner still holds the mutex")
	require.NoError(t, owner.Unlock(ctx))
	require.NoError(t, waitRecipeResult(ctx, successorResult, "successor acquisition"))
	require.NoError(t, successor.Unlock(ctx))
	require.Equal(t, 0, recipePrefixCount(ctx, client, mutexName))
	victimErr := waitRecipeResult(ctx, victimResult, "victim cleanup")
	require.True(t, victimErr == nil || errors.Is(victimErr, concurrency.ErrSessionExpired), victimErr)
}

func TestClientExperimentalDoubleBarrierEnterLeaveAndSessionCleanup(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	newSession := func() *concurrency.Session {
		session, sessionErr := concurrency.NewSession(client, concurrency.WithTTL(10))
		require.NoError(t, sessionErr)
		return session
	}

	normalName := "/a970/double-barrier/normal"
	sessions := []*concurrency.Session{newSession(), newSession(), newSession()}
	for _, session := range sessions {
		t.Cleanup(session.Orphan)
	}
	barriers := make([]*recipe.DoubleBarrier, 0, len(sessions))
	for _, session := range sessions {
		barriers = append(barriers, recipe.NewDoubleBarrier(session, normalName, len(sessions)))
	}
	enterResults := make(chan error, len(barriers))
	go func() { enterResults <- barriers[0].Enter() }()
	go func() { enterResults <- barriers[1].Enter() }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, normalName+"/waiters") == 2
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, enterResults, "first two clients must wait until the barrier reaches its count")
	go func() { enterResults <- barriers[2].Enter() }()
	for range barriers {
		require.NoError(t, waitRecipeResult(ctx, enterResults, "barrier enter"))
	}

	extraSession := newSession()
	t.Cleanup(extraSession.Orphan)
	require.ErrorIs(t, recipe.NewDoubleBarrier(extraSession, normalName, len(sessions)).Enter(), recipe.ErrTooManyClients)

	leaveResults := make(chan error, len(barriers))
	go func() { leaveResults <- barriers[0].Leave() }()
	go func() { leaveResults <- barriers[1].Leave() }()
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, leaveResults, "first two leavers must wait for the last participant")
	go func() { leaveResults <- barriers[2].Leave() }()
	for range barriers {
		require.NoError(t, waitRecipeResult(ctx, leaveResults, "barrier leave"))
	}
	require.Equal(t, 0, recipePrefixCount(ctx, client, normalName+"/waiters"))

	failoverName := "/a970/double-barrier/failover"
	failoverSessions := []*concurrency.Session{newSession(), newSession(), newSession()}
	t.Cleanup(failoverSessions[1].Orphan)
	t.Cleanup(failoverSessions[2].Orphan)
	failoverBarriers := make([]*recipe.DoubleBarrier, 0, len(failoverSessions))
	for _, session := range failoverSessions {
		failoverBarriers = append(failoverBarriers, recipe.NewDoubleBarrier(session, failoverName, len(failoverSessions)))
	}
	failoverEnter := make(chan error, len(failoverBarriers))
	go func() { failoverEnter <- failoverBarriers[0].Enter() }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, failoverName+"/waiters") == 1
	}, 5*time.Second, 20*time.Millisecond)
	go func() { failoverEnter <- failoverBarriers[1].Enter() }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, failoverName+"/waiters") == 2
	}, 5*time.Second, 20*time.Millisecond)
	go func() { failoverEnter <- failoverBarriers[2].Enter() }()
	for range failoverBarriers {
		require.NoError(t, waitRecipeResult(ctx, failoverEnter, "failover enter"))
	}

	failoverLeave := make(chan error, 2)
	go func() { failoverLeave <- failoverBarriers[1].Leave() }()
	go func() { failoverLeave <- failoverBarriers[2].Leave() }()
	require.Eventually(t, func() bool {
		return recipePrefixCount(ctx, client, failoverName+"/waiters") == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, failoverSessions[0].Close())
	for range 2 {
		require.NoError(t, waitRecipeResult(ctx, failoverLeave, "failover leave"))
	}
	require.Equal(t, 0, recipePrefixCount(ctx, client, failoverName+"/waiters"))
}

func recipePrefixCount(ctx context.Context, client *clientv3.Client, prefix string) int {
	response, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return -1
	}
	return len(response.Kvs)
}

func waitRecipeResult(ctx context.Context, result <-chan error, phase string) error {
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%s did not complete: %w", phase, ctx.Err())
	}
}
