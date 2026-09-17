package etcdproxy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

type retirementTxnServer struct {
	etcdserverpb.UnimplementedKVServer
	effects  atomic.Int32
	admitted chan struct{}
	release  chan struct{}
}

func (s *retirementTxnServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, rpctypes.ErrGRPCNoLeader
}

func TestForwardErrorDoesNotDiscardOtherAdmittedTxn(t *testing.T) {
	old := &retirementTxnServer{admitted: make(chan struct{}, 1), release: make(chan struct{})}
	address := startPutResultServer(t, old)
	proxy := NewEtcdProxy(t.Context(), newSwitchingLeaderElection(address), nil, false, 0).(*etcdProxy)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	require.Eventually(t, func() bool { return proxy.Ready() == nil }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := proxy.Txn(ctx, &etcdserverpb.TxnRequest{}); done <- err }()
	select {
	case <-old.admitted:
	case <-ctx.Done():
		t.Fatal("request not admitted")
	}
	_, err := proxy.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("declined")})
	require.Error(t, err)
	close(old.release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("original response lost")
	}
	require.Equal(t, int32(1), old.effects.Load())
	require.Eventually(t, func() bool {
		proxy.lock.RLock()
		defer proxy.lock.RUnlock()
		return len(proxy.retiredClients) == 0 && len(proxy.unaryPins) == 0
	}, time.Second, time.Millisecond)
}

func TestRetiredTransportWaitsForLastAdmissionPin(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{startPutResultServer(t, &etcdserverpb.UnimplementedKVServer{})}})
	require.NoError(t, err)
	proxy := &etcdProxy{unaryPins: map[*clientv3.Client]int{client: 2}, retiredClientTimeout: time.Minute}
	proxy.lock.Lock()
	proxy.retireClientLocked(client)
	proxy.lock.Unlock()
	proxy.releaseCoreUnaryClient(client)
	proxy.lock.RLock()
	require.Len(t, proxy.retiredClients, 1)
	require.Equal(t, 1, proxy.unaryPins[client])
	proxy.lock.RUnlock()
	require.NotEqual(t, connectivity.Shutdown, client.ActiveConnection().GetState())
	proxy.releaseCoreUnaryClient(client)
	proxy.retirementWorkers.Wait()
	require.Empty(t, proxy.unaryPins)
	require.Empty(t, proxy.retiredClients)
	require.Equal(t, connectivity.Shutdown, client.ActiveConnection().GetState())
}

func TestRetirementExpiryReleaseAndCloseRace(t *testing.T) {
	address := startPutResultServer(t, &etcdserverpb.UnimplementedKVServer{})
	for attempt := 0; attempt < 20; attempt++ {
		client, err := clientv3.New(clientv3.Config{Endpoints: []string{address}})
		require.NoError(t, err)
		loopDone := make(chan struct{})
		close(loopDone)
		proxy := &etcdProxy{cancel: func() {}, loopDone: loopDone,
			unaryPins: map[*clientv3.Client]int{client: 1}, retiredClientTimeout: time.Nanosecond}
		proxy.lock.Lock()
		proxy.retireClientLocked(client)
		proxy.lock.Unlock()
		released := make(chan struct{})
		go func() { proxy.releaseCoreUnaryClient(client); close(released) }()
		require.NoError(t, proxy.Close())
		<-released
		require.Empty(t, proxy.retiredClients)
		require.Empty(t, proxy.unaryPins)
		require.Equal(t, connectivity.Shutdown, client.ActiveConnection().GetState())
	}
}

func TestRetiredTxnTransportIsBounded(t *testing.T) {
	for _, action := range []string{"timeout", "caller cancellation", "caller deadline", "proxy close"} {
		t.Run(action, func(t *testing.T) {
			old := &retirementTxnServer{admitted: make(chan struct{}, 1), release: make(chan struct{})}
			successor := &retirementTxnServer{admitted: make(chan struct{}, 1), release: make(chan struct{})}
			close(successor.release)
			oldAddress := startPutResultServer(t, old)
			newAddress := startPutResultServer(t, successor)
			election := newSwitchingLeaderElection(oldAddress)
			proxy := NewEtcdProxy(t.Context(), election, nil, false, 0).(*etcdProxy)
			t.Cleanup(func() { require.NoError(t, proxy.Close()) })
			proxy.lock.Lock()
			proxy.retiredClientTimeout = 10 * time.Second
			if action == "timeout" {
				proxy.retiredClientTimeout = 200 * time.Millisecond
			}
			proxy.lock.Unlock()
			require.Eventually(t, func() bool { return proxy.Ready() == nil }, time.Second, time.Millisecond)
			var ctx context.Context
			var cancel context.CancelFunc
			if action == "caller deadline" {
				ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			} else {
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := proxy.Txn(ctx, &etcdserverpb.TxnRequest{}); done <- err }()
			select {
			case <-old.admitted:
			case <-time.After(time.Second):
				t.Fatal("request not admitted")
			}
			election.address.Store(newAddress)
			proxy.requestClientUpdate()
			require.Eventually(t, func() bool {
				proxy.lock.RLock()
				defer proxy.lock.RUnlock()
				return proxy.curLeader == newAddress && proxy.client != nil
			}, time.Second, time.Millisecond)
			start := time.Now()
			switch action {
			case "caller cancellation":
				cancel()
			case "proxy close":
				require.NoError(t, proxy.Close())
			}
			select {
			case err := <-done:
				require.Error(t, err)
				if action == "caller deadline" {
					require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
					require.Equal(t, codes.DeadlineExceeded, status.Code(err))
				}
			case <-time.After(2 * time.Second):
				t.Fatal("retired request was not bounded")
			}
			require.Less(t, time.Since(start), 2*time.Second)
			require.Eventually(t, func() bool {
				proxy.lock.RLock()
				defer proxy.lock.RUnlock()
				return len(proxy.retiredClients) == 0 && len(proxy.unaryPins) == 0
			}, time.Second, time.Millisecond)
			require.Equal(t, int32(1), old.effects.Load())
			require.Zero(t, successor.effects.Load(), "no replay on retirement failure")
			if action != "proxy close" {
				response, err := proxy.Txn(t.Context(), &etcdserverpb.TxnRequest{})
				require.NoError(t, err)
				require.True(t, response.Succeeded)
			}
		})
	}
}

func TestRetiredClientGenerationCapAndStreamNotification(t *testing.T) {
	loopDone := make(chan struct{})
	close(loopDone)
	proxy := &etcdProxy{cancel: func() {}, loopDone: loopDone, retiredClientTimeout: time.Minute,
		unaryPins: make(map[*clientv3.Client]int)}
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	address := startPutResultServer(t, &etcdserverpb.UnimplementedKVServer{})
	var clients []*clientv3.Client
	for index := 0; index < proxyMaxRetiredClients+1; index++ {
		client, err := clientv3.New(clientv3.Config{Endpoints: []string{address}})
		require.NoError(t, err)
		clients = append(clients, client)
		proxy.lock.Lock()
		proxy.client = client
		proxy.unaryPins[client] = 1
		oldStream := make(chan struct{})
		proxy.closed = oldStream
		proxy.resetClient()
		count := len(proxy.retiredClients)
		proxy.lock.Unlock()
		require.LessOrEqual(t, count, proxyMaxRetiredClients)
		select {
		case <-oldStream:
		default:
			t.Fatal("unary drain must not delay stream generation notification")
		}
	}
	require.Equal(t, connectivity.Shutdown, clients[0].ActiveConnection().GetState(), "oldest generation evicted on churn")
	for _, client := range clients {
		proxy.releaseCoreUnaryClient(client)
	}
	require.Empty(t, proxy.unaryPins)
	require.Empty(t, proxy.retiredClients)
	for _, client := range clients {
		require.Equal(t, connectivity.Shutdown, client.ActiveConnection().GetState())
	}
	require.NoError(t, proxy.Close())
}

func (s *retirementTxnServer) Txn(ctx context.Context, _ *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	s.effects.Add(1)
	select {
	case s.admitted <- struct{}{}:
	default:
	}
	// Model a committed result whose response is briefly delayed. Do not infer
	// rollback from transport cancellation; the effect has already happened.
	select {
	case <-s.release:
		return &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Succeeded: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestAdmittedTxnResultSurvivesPeerReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "unchanged_peer"
		if replace {
			name = "replacement_peer"
		}
		t.Run(name, func(t *testing.T) {
			old := &retirementTxnServer{admitted: make(chan struct{}, 1), release: make(chan struct{})}
			successor := &retirementTxnServer{admitted: make(chan struct{}, 1), release: make(chan struct{})}
			close(successor.release)
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(old.release) }) })
			oldAddress := startPutResultServer(t, old)
			successorAddress := startPutResultServer(t, successor)
			election := newSwitchingLeaderElection(oldAddress)
			proxy := NewEtcdProxy(t.Context(), election, nil, false, 0).(*etcdProxy)
			t.Cleanup(func() { require.NoError(t, proxy.Close()) })
			require.Eventually(t, func() bool { return proxy.Ready() == nil }, time.Second, time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			type result struct {
				response *etcdserverpb.TxnResponse
				err      error
			}
			done := make(chan result, 1)
			go func() {
				response, err := proxy.Txn(ctx, &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
						RequestPut: &etcdserverpb.PutRequest{Key: []byte("committed-result"), Value: []byte("once")},
					}}},
				})
				done <- result{response, err}
			}()
			select {
			case <-old.admitted:
			case <-ctx.Done():
				t.Fatal("old peer never admitted the request")
			}
			if replace {
				election.address.Store(successorAddress)
				proxy.requestClientUpdate()
				require.Eventually(t, func() bool {
					proxy.lock.RLock()
					defer proxy.lock.RUnlock()
					return proxy.curLeader == successorAddress && proxy.client != nil
				}, time.Second, time.Millisecond, "replacement admission must not wait for the old request")
			}
			release.Do(func() { close(old.release) })
			var got result
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal("caller timed out")
			}
			require.NoError(t, ctx.Err())
			require.Equal(t, int32(1), old.effects.Load())
			require.Zero(t, successor.effects.Load(), "admitted Txn must never be replayed on successor")
			require.NoError(t, got.err, "topology publication alone should not discard an admitted response")
			require.True(t, got.response.Succeeded)
			require.Equal(t, int64(42), got.response.Header.Revision)
		})
	}
}
