package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
)

func deliveryEvent(key string, revision int64) *mvccpb.Event {
	return &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(key), ModRevision: revision}}
}

func observeDelivery(r *watchDeliveryRecorder, events ...*mvccpb.Event) {
	r.HandleRPC(context.Background(), &stats.InPayload{Client: true, Payload: &etcdserverpb.WatchResponse{Events: events}})
}

func TestWatchDeliveryFiltersBoundsAndConsumes(t *testing.T) {
	at := time.Unix(10, 0)
	r := &watchDeliveryRecorder{Handler: newRPCAttemptRecorder(2, nil), key: []byte("probe"), now: func() time.Time { return at }}
	deleted := deliveryEvent("probe", 3)
	deleted.Type = mvccpb.DELETE
	observeDelivery(r, nil, &mvccpb.Event{}, deliveryEvent("other", 1), deliveryEvent("probe", 0), deleted)
	for _, revision := range []int64{0, 1, 3} {
		_, ok := r.take(revision)
		require.False(t, ok)
	}
	r.HandleRPC(context.Background(), &stats.InPayload{Client: false, Payload: &etcdserverpb.WatchResponse{Events: []*mvccpb.Event{deliveryEvent("probe", 4)}}})
	_, ok := r.take(4)
	require.False(t, ok)
	observeDelivery(r, deliveryEvent("probe", 5), deliveryEvent("probe", 5))
	o, ok := r.take(5)
	require.True(t, ok)
	require.True(t, o.duplicate)
	require.Equal(t, at, o.at)
	_, ok = r.take(5)
	require.False(t, ok)
	for revision := int64(10); revision <= 10+watchDeliveryCapacity; revision++ {
		observeDelivery(r, deliveryEvent("probe", revision))
	}
	_, ok = r.take(10)
	require.False(t, ok, "bounded ring must evict old observations")
	_, ok = r.take(10 + watchDeliveryCapacity)
	require.True(t, ok)
}

func TestWatchDeliveryPartitionsOnlyMatchedPostPutInterval(t *testing.T) {
	base := time.Unix(10, 0)
	r := &watchDeliveryRecorder{Handler: newRPCAttemptRecorder(2, nil), key: []byte("probe")}
	p := &watchDeliveryProgress{}
	for i, offset := range []time.Duration{-time.Millisecond, 2 * time.Millisecond, 20 * time.Millisecond} {
		r.now = func() time.Time { return base.Add(offset) }
		observeDelivery(r, deliveryEvent("probe", int64(i+1)))
		p.record(r, int64(i+1), base, base.Add(10*time.Millisecond))
	}
	r.now = func() time.Time { return base }
	observeDelivery(r, deliveryEvent("probe", 4), deliveryEvent("probe", 4))
	p.record(r, 4, base, base.Add(time.Millisecond))
	p.record(r, 99, base, base.Add(time.Millisecond))
	require.Equal(t, 2, p.matched)
	require.Equal(t, 1, p.beforePut)
	require.Equal(t, 1, p.invalid)
	require.Equal(t, 1, p.ambiguous)
	require.Equal(t, 1, p.missing)
	require.Equal(t, 2*time.Millisecond, p.prePayload)
	require.Equal(t, 18*time.Millisecond, p.postPayload)
	var output bytes.Buffer
	require.NoError(t, p.write(&output, true))
	require.Equal(t, "PROBE_WATCH_DELIVERY matched=2 missing=1 ambiguous=1 invalid=1 payload_before_put=1 post_put_to_payload_us=2000 payload_to_consume_after_put_us=18000 final=true scope=diagnostic_only\n", output.String())
}

func TestWatchDeliveryPreservesUnaryHandlerAndConcurrentAccess(t *testing.T) {
	unary := newRPCAttemptRecorder(2, nil)
	r := &watchDeliveryRecorder{Handler: unary, key: []byte("probe"), now: time.Now}
	ctx := r.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/etcdserverpb.KV/Put"})
	r.HandleRPC(ctx, &stats.Begin{Client: true, BeginTime: time.Now()})
	r.HandleRPC(ctx, &stats.End{Client: true, EndTime: time.Now()})
	require.Equal(t, 1, unary.count)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for revision := int64(1); revision <= 300; revision++ {
				observeDelivery(r, deliveryEvent("probe", revision))
				r.take(revision)
			}
		}()
	}
	wg.Wait()
}

type deliveryFailWriter struct{}

func (deliveryFailWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestWatchDeliveryProgressUsesExistingCadence(t *testing.T) {
	p := newProbeProgress(time.Now(), 6000)
	p.watchDelivery = &watchDeliveryProgress{}
	var out bytes.Buffer
	p.completed = 59
	require.NoError(t, p.write(&out, time.Now(), false))
	require.Empty(t, out.String())
	p.completed = 60
	require.NoError(t, p.write(&out, time.Now(), false))
	require.Equal(t, 1, bytes.Count(out.Bytes(), []byte("PROBE_WATCH_DELIVERY ")))
	require.Error(t, p.watchDelivery.write(deliveryFailWriter{}, true))
}

type deliveryTraceWatchServer struct {
	etcdserverpb.UnimplementedWatchServer
}

func (deliveryTraceWatchServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	key := request.GetCreateRequest().GetKey()
	if err := stream.Send(&etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 6}, WatchId: 1, Created: true}); err != nil {
		return err
	}
	if err := stream.Send(&etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 7}, WatchId: 1, Events: []*mvccpb.Event{deliveryEvent(string(key), 7)}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

// Exercise official clientv3 demultiplexing and real gRPC stats dispatch. This
// harness verifies ordering/association, not TiKV or deployment performance.
func TestWatchDeliveryOfficialClientGRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	etcdserverpb.RegisterWatchServer(server, deliveryTraceWatchServer{})
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	r := &watchDeliveryRecorder{Handler: newRPCAttemptRecorder(2, nil), key: []byte("probe"), now: time.Now}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{listener.Addr().String()}, DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(r)}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	ch := client.Watch(ctx, "probe", clientv3.WithRev(7))
	select {
	case response, ok := <-ch:
		received := time.Now()
		require.True(t, ok)
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		require.EqualValues(t, 7, response.Events[0].Kv.ModRevision)
		p := &watchDeliveryProgress{}
		p.record(r, 7, started, received)
		require.Equal(t, 1, p.matched)
		require.Zero(t, p.missing+p.invalid+p.ambiguous)
		require.Equal(t, received.Sub(started), p.prePayload+p.postPayload)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func BenchmarkWatchDeliveryObservation(b *testing.B) {
	r := &watchDeliveryRecorder{Handler: newRPCAttemptRecorder(2, nil), key: []byte("probe"), now: time.Now}
	ctx := context.Background()
	event := &stats.InPayload{Client: true, Payload: &etcdserverpb.WatchResponse{Events: []*mvccpb.Event{deliveryEvent("probe", 1)}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.HandleRPC(ctx, event)
		if _, ok := r.take(1); !ok {
			b.Fatal("missing observation")
		}
	}
}
