package etcdproxy

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type generationWatchCall struct {
	ctx       context.Context
	responses chan clientv3.WatchResponse
	revision  int64
}

type wireGenerationCreate struct {
	id      int64
	request *etcdserverpb.WatchCreateRequest
}

type wireGenerationServer struct {
	etcdserverpb.UnimplementedWatchServer
	nextID   atomic.Int64
	created  chan wireGenerationCreate
	canceled chan int64
}

func (s *wireGenerationServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	active := make(map[int64]struct{})
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if create := request.GetCreateRequest(); create != nil {
			id := s.nextID.Add(1)
			active[id] = struct{}{}
			if err := stream.Send(&etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, WatchId: id, Created: true}); err != nil {
				return err
			}
			select {
			case s.created <- wireGenerationCreate{id: id, request: create}:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
			if string(create.Key) == "/generation-wire" && create.StartRevision == 43 {
				if err := stream.Send(&etcdserverpb.WatchResponse{
					Header: &etcdserverpb.ResponseHeader{Revision: 43}, WatchId: id,
					Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: create.Key, Value: []byte("next"), ModRevision: 43}}},
				}); err != nil {
					return err
				}
			}
		} else if cancel := request.GetCancelRequest(); cancel != nil {
			delete(active, cancel.WatchId)
			if err := stream.Send(&etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 43}, WatchId: cancel.WatchId, Canceled: true}); err != nil {
				return err
			}
			select {
			case s.canceled <- cancel.WatchId:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		} else if request.GetProgressRequest() != nil {
			for id := range active {
				if err := stream.Send(&etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 43}, WatchId: id}); err != nil {
					return err
				}
			}
		}
	}
}

// Unlike the context-isolation test, this uses the unmodified official Watcher.
// An unrelated subscription keeps the old multiplexed stream alive, so closing
// the entire client cannot masquerade as cancellation of the abandoned Watch.
func TestWatchAbandonedGenerationSendsWireCancel(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fixture := &wireGenerationServer{created: make(chan wireGenerationCreate, 8), canceled: make(chan int64, 8)}
	server := grpc.NewServer()
	etcdserverpb.RegisterWatchServer(server, fixture)
	registerServingHealth(server)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	address := listener.Addr().String()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{address}, DialTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, checkClientConn(client, nil, time.Second))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	keeper := client.Watch(ctx, "/keeper", clientv3.WithCreatedNotify())
	select {
	case response := <-keeper:
		require.True(t, response.Created)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	nextCreate := func() wireGenerationCreate {
		select {
		case create := <-fixture.created:
			return create
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return wireGenerationCreate{}
		}
	}
	keepCreate := nextCreate()
	require.Equal(t, "/keeper", string(keepCreate.request.Key))
	proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: address}, client: client, curLeader: address, closed: make(chan struct{})}
	results, err := proxy.Watch(ctx, []byte("/generation-wire"), nil, 42)
	require.NoError(t, err)
	nextResult := func() WatchResult {
		select {
		case result, ok := <-results:
			require.True(t, ok)
			require.NoError(t, result.Err)
			return result
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return WatchResult{}
		}
	}
	require.True(t, nextResult().Created)
	first := nextCreate()
	require.Equal(t, int64(42), first.request.StartRevision)
	proxy.lock.Lock()
	close(proxy.closed)
	proxy.closed = make(chan struct{})
	proxy.lock.Unlock()
	require.True(t, nextResult().Created)
	second := nextCreate()
	require.Equal(t, int64(43), second.request.StartRevision)
	require.NotEqual(t, first.id, second.id)
	result := nextResult()
	require.Len(t, result.Events, 1)
	require.Equal(t, int64(43), result.Events[0].Kv.ModRevision)
	require.Equal(t, "next", string(result.Events[0].Kv.Value))
	select {
	case id := <-fixture.canceled:
		require.Equal(t, first.id, id)
	case <-ctx.Done():
		t.Fatal("old subscription did not send wire cancellation", ctx.Err())
	}
	require.NoError(t, proxy.Ready(), "canceling one Watch must not close the shared transport")
	require.NoError(t, client.RequestProgress(ctx))
	select {
	case response, ok := <-keeper:
		require.True(t, ok)
		require.NoError(t, response.Err())
		require.True(t, response.IsProgressNotify(), "unrelated subscription must remain usable")
		require.Equal(t, int64(43), response.Header.Revision)
	case <-ctx.Done():
		t.Fatal("unrelated Watch did not receive progress", ctx.Err())
	}
}

type generationContextWatcher struct {
	clientv3.Watcher
	calls chan generationWatchCall
}

func (w *generationContextWatcher) Watch(ctx context.Context, key string, options ...clientv3.OpOption) clientv3.WatchChan {
	responses := make(chan clientv3.WatchResponse, 1)
	w.calls <- generationWatchCall{ctx: ctx, responses: responses, revision: clientv3.OpGet(key, options...).Rev()}
	return responses
}

// Keep a real ready transport but control Watch envelopes independently. A
// generation reset must release its subscription even if the shared client is
// not closed; the replacement must still inherit logical authorization state.
func TestWatchAbandonedGenerationCancelsSubscription(t *testing.T) {
	for _, reset := range []string{"leader-generation", "before-created-envelope"} {
		t.Run(reset, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			registerServingHealth(server)
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
			address := listener.Addr().String()
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{address}, DialTimeout: time.Second})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			require.NoError(t, checkClientConn(client, nil, time.Second))
			watcher := &generationContextWatcher{Watcher: client.Watcher, calls: make(chan generationWatchCall, 4)}
			client.Watcher = watcher
			proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: address}, client: client, curLeader: address, closed: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			results, err := proxy.Watch(ctx, []byte("/generation-context"), nil, 42)
			require.NoError(t, err)
			nextCall := func() generationWatchCall {
				select {
				case call := <-watcher.calls:
					return call
				case <-ctx.Done():
					t.Fatal("replacement Watch not opened", ctx.Err())
					return generationWatchCall{}
				}
			}
			first := nextCall()
			require.Equal(t, int64(42), first.revision)
			if reset == "leader-generation" {
				first.responses <- clientv3.WatchResponse{Created: true, Header: &etcdserverpb.ResponseHeader{Revision: 42}}
				select {
				case response := <-results:
					require.True(t, response.Created)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				proxy.lock.Lock()
				close(proxy.closed)
				proxy.closed = make(chan struct{})
				proxy.lock.Unlock()
			} else {
				first.responses <- clientv3.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}}
			}
			second := nextCall()
			require.ErrorIs(t, first.ctx.Err(), context.Canceled, "abandoned subscription must be canceled before replacement opens")
			require.NoError(t, second.ctx.Err(), "canceling the old generation must not cancel its replacement")
			md, _ := metadata.FromOutgoingContext(second.ctx)
			if reset == "leader-generation" {
				require.Equal(t, []string{"1"}, md.Get(AuthorizedWatchProxyMetadataKey))
				require.Equal(t, int64(43), second.revision)
			} else {
				require.Empty(t, md.Get(AuthorizedWatchProxyMetadataKey))
				require.Equal(t, int64(42), second.revision)
			}
			cancel()
			select {
			case _, ok := <-results:
				require.False(t, ok)
			case <-time.After(time.Second):
				t.Fatal("logical Watch did not close")
			}
			require.ErrorIs(t, second.ctx.Err(), context.Canceled)
		})
	}
}
