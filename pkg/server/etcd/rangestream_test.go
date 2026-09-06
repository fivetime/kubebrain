// Copyright 2022 ByteDance and/or its affiliates
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
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v2proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// fakeRangeStreamServer captures the chunks a RangeStream handler sends. It
// embeds the generated server-stream interface so the unexercised grpc.ServerStream
// methods are present; the handler only calls Send and Context.
type fakeRangeStreamServer struct {
	etcdserverpb.KV_RangeStreamServer
	ctx  context.Context
	sent []*etcdserverpb.RangeStreamResponse
}

type blockingRangeStreamServer struct {
	fakeRangeStreamServer
	firstSent chan struct{}
	release   chan struct{}
}

type failingRangeStreamServer struct {
	fakeRangeStreamServer
	err error
}

func (s *failingRangeStreamServer) Send(*etcdserverpb.RangeStreamResponse) error {
	return s.err
}

func (s *blockingRangeStreamServer) Send(resp *etcdserverpb.RangeStreamResponse) error {
	s.sent = append(s.sent, resp)
	if len(s.sent) == 1 {
		close(s.firstSent)
		<-s.release
	}
	return nil
}

type listCountingBackendShim struct {
	BackendShim
	listCalls int
}

type limitedRangeStreamCountProbeShim struct {
	BackendShim
	total        int
	produced     int
	stopped      bool
	done         chan struct{}
	countCalls   int
	countRequest *etcdserverpb.RangeRequest
}

func (b *limitedRangeStreamCountProbeShim) RangeStreamChan(
	ctx context.Context, _, _ []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	ch := make(chan rangeStreamChunk)
	go func() {
		defer close(ch)
		defer close(b.done)
		for index := 0; index < b.total; index++ {
			chunk := rangeStreamChunk{resp: &etcdserverpb.RangeResponse{
				Header: txnHeader(int64(revision)),
				Kvs: []*mvccpb.KeyValue{{
					Key:            []byte(fmt.Sprintf("/limit-stop/%03d", index)),
					Value:          []byte("value"),
					CreateRevision: int64(revision),
					ModRevision:    int64(revision),
					Version:        1,
				}},
			}}
			select {
			case ch <- chunk:
				b.produced++
			case <-ctx.Done():
				b.stopped = true
				return
			}
		}
		select {
		case ch <- rangeStreamChunk{resp: &etcdserverpb.RangeResponse{Header: txnHeader(int64(revision))}}:
		case <-ctx.Done():
			b.stopped = true
		}
	}()
	return ch, nil
}

func (b *limitedRangeStreamCountProbeShim) Count(
	_ context.Context, request *etcdserverpb.RangeRequest,
) (*etcdserverpb.RangeResponse, error) {
	b.countCalls++
	b.countRequest = proto.Clone(request).(*etcdserverpb.RangeRequest)
	return &etcdserverpb.RangeResponse{
		Header: txnHeader(request.Revision),
		Count:  int64(b.total),
	}, nil
}

type keysOnlyRangeStreamProbeShim struct {
	BackendShim
	ordinaryCalls int
	metadataCalls int
}

func (b *keysOnlyRangeStreamProbeShim) RangeStreamChan(
	context.Context, []byte, []byte, uint64,
) (<-chan rangeStreamChunk, error) {
	b.ordinaryCalls++
	return nil, errors.New("KeysOnly RangeStream unexpectedly requested full values")
}

func (b *keysOnlyRangeStreamProbeShim) RangeStreamKeysOnlyChan(
	ctx context.Context, start, end []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	b.metadataCalls++
	return b.BackendShim.RangeStreamChan(ctx, start, end, revision)
}

type prematureRangeStreamBackendShim struct {
	BackendShim
}

type nilRangeStreamBackendShim struct {
	BackendShim
}

type malformedRangeStreamBackendShim struct {
	BackendShim
	result func(uint64) rangeStreamChunk
}

func (b *nilRangeStreamBackendShim) RangeStreamChan(
	context.Context, []byte, []byte, uint64,
) (<-chan rangeStreamChunk, error) {
	return nil, nil
}

func (b *malformedRangeStreamBackendShim) RangeStreamChan(
	_ context.Context, _, _ []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	ch := make(chan rangeStreamChunk, 1)
	ch <- b.result(revision)
	close(ch)
	return ch, nil
}

type revisionRecordingRangeStreamBackendShim struct {
	BackendShim
	revision uint64
	latest   bool
}

type writeBeforeRangeStreamBackendShim struct {
	BackendShim
	before   func() error
	revision uint64
}

type checkpointRangeStreamBackendShim struct {
	*checkpointRangeBackendShim
	used bool
}

type liveBlockingCheckpointRangeStreamBackendShim struct {
	*checkpointRangeStreamBackendShim
	liveAttempts int
}

type partialFailureCheckpointRangeStreamBackendShim struct {
	*checkpointRangeStreamBackendShim
	liveAttempts int
}

func (b *checkpointRangeStreamBackendShim) RangeStreamChan(
	ctx context.Context, _, _ []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	timestamp, ok := storage.SnapshotTimestampFromContext(ctx)
	if !ok || timestamp != b.checkpoint.Timestamp || revision != b.checkpoint.Revision {
		return nil, storage.ErrUnavailable
	}
	b.used = true
	ch := make(chan rangeStreamChunk, 2)
	ch <- rangeStreamChunk{resp: &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(b.checkpoint.Revision)),
		Kvs: []*mvccpb.KeyValue{{
			Key: []byte("/checkpoint/key"), Value: []byte("checkpoint"), ModRevision: int64(b.checkpoint.Revision),
		}},
	}}
	ch <- rangeStreamChunk{resp: &etcdserverpb.RangeResponse{Header: txnHeader(int64(b.checkpoint.Revision))}}
	close(ch)
	return ch, nil
}

func (b *liveBlockingCheckpointRangeStreamBackendShim) RangeStreamChan(
	ctx context.Context, start, end []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	if _, ok := storage.SnapshotTimestampFromContext(ctx); ok {
		return b.checkpointRangeStreamBackendShim.RangeStreamChan(ctx, start, end, revision)
	}
	b.liveAttempts++
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *partialFailureCheckpointRangeStreamBackendShim) RangeStreamChan(
	ctx context.Context, start, end []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	if _, ok := storage.SnapshotTimestampFromContext(ctx); ok {
		return b.checkpointRangeStreamBackendShim.RangeStreamChan(ctx, start, end, revision)
	}
	b.liveAttempts++
	ch := make(chan rangeStreamChunk, 3)
	for _, key := range []string{"/partial/a", "/partial/b"} {
		ch <- rangeStreamChunk{resp: &etcdserverpb.RangeResponse{
			Header: txnHeader(int64(revision)),
			Kvs:    []*mvccpb.KeyValue{{Key: []byte(key), Value: []byte("live"), ModRevision: int64(revision)}},
		}}
	}
	ch <- rangeStreamChunk{err: storage.ErrUnavailable}
	close(ch)
	return ch, nil
}

type encodedErrorRangeStreamBackend struct {
	backend.Backend
	err error
}

func (b *encodedErrorRangeStreamBackend) RangeStream(
	context.Context, []byte, []byte, uint64,
) (<-chan *v2proto.StreamRangeResponse, error) {
	ch := make(chan *v2proto.StreamRangeResponse, 1)
	ch <- &v2proto.StreamRangeResponse{
		RangeResponse: &v2proto.RangeResponse{Header: &v2proto.ResponseHeader{Revision: 7}},
		Err:           streamerror.Encode(b.err),
	}
	close(ch)
	return ch, nil
}

func (b *revisionRecordingRangeStreamBackendShim) RangeStreamChan(
	ctx context.Context, start, end []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	b.revision = revision
	b.latest = backend.LatestRangeStreamFromContext(ctx)
	return b.BackendShim.RangeStreamChan(ctx, start, end, revision)
}

func (b *writeBeforeRangeStreamBackendShim) RangeStreamChan(
	ctx context.Context, start, end []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	b.revision = revision
	if err := b.before(); err != nil {
		return nil, err
	}
	return b.BackendShim.RangeStreamChan(ctx, start, end, revision)
}

func (b *prematureRangeStreamBackendShim) RangeStreamChan(
	_ context.Context, _, _ []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	ch := make(chan rangeStreamChunk, 1)
	ch <- rangeStreamChunk{resp: &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(revision)),
		Kvs:    []*mvccpb.KeyValue{{Key: []byte("/premature/key"), Value: []byte("value")}},
	}}
	close(ch)
	return ch, nil
}

func (b *listCountingBackendShim) List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	b.listCalls++
	return b.BackendShim.List(ctx, req)
}

func (s *fakeRangeStreamServer) Send(resp *etcdserverpb.RangeStreamResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func (s *fakeRangeStreamServer) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func recordedRangeStreamFailureValues(rec *recordingMetrics, stage string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name != "read.range_stream.failure" || len(counter.tags) != 1 ||
			counter.tags[0] != metrics.Tag("stage", stage) {
			continue
		}
		values = append(values, counter.value)
	}
	return values
}

func recordedRangeStreamProxyRetryValues(rec *recordingMetrics) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "read.range_stream.proxy_retry" && len(counter.tags) == 0 {
			values = append(values, counter.value)
		}
	}
	return values
}

// newRangeStreamTestServer builds a leader RPCServer over memkv.
func newRangeStreamTestServer(t *testing.T) (*RPCServer, func()) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	return newRangeStreamTestServerWithMetrics(t, m)
}

func newRangeStreamTestServerWithMetrics(t *testing.T, metricCli metrics.Metrics) (*RPCServer, func()) {
	t.Helper()
	kv := memkv.NewKvStorage()
	backendStore := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metricCli)
	server := New(backendStore, metricCli, testPeerService{isLeader: true})
	cleanup := func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
	}
	return server, cleanup
}

func TestRangeStreamStreamsAllKeys(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const n = 25
	want := map[string]string{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("/registry/pods/%03d", i)
		val := fmt.Sprintf("v-%03d", i)
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(val)})
		require.NoError(t, err)
		want[key] = val
	}

	rs := &fakeRangeStreamServer{ctx: ctx}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/"),
		RangeEnd: []byte("/registry/pods0"), // prefix end of "/registry/pods/"
	}, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rs.sent, "must send at least the final data/envelope chunk")

	// Kvs concatenate to the full disjoint set; only the final chunk carries the
	// pinned revision and merged response metadata.
	got := map[string]string{}
	for i, chunk := range rs.sent {
		require.NotNil(t, chunk.RangeResponse)
		if i != len(rs.sent)-1 {
			require.Nil(t, chunk.RangeResponse.Header)
			require.Zero(t, chunk.RangeResponse.Count)
			require.False(t, chunk.RangeResponse.More)
		}
		for _, kv := range chunk.RangeResponse.Kvs {
			_, dup := got[string(kv.Key)]
			require.False(t, dup, "key %s appeared in more than one chunk (chunks must be disjoint)", kv.Key)
			got[string(kv.Key)] = string(kv.Value)
		}
	}
	last := rs.sent[len(rs.sent)-1]
	require.NotEmpty(t, last.RangeResponse.Kvs, "final chunk must carry the last data batch with its envelope")
	require.NotNil(t, last.RangeResponse.Header)
	require.Greater(t, last.RangeResponse.Header.Revision, int64(0))
	require.EqualValues(t, n, last.RangeResponse.Count)
	for _, chunk := range rs.sent {
		require.False(t, chunk.RangeResponse.More, "unlimited RangeStream must not expose scanner chunking as Range.More")
	}
	require.Equal(t, want, got, "concatenated chunks must equal the full key set")
}

func TestRangeStreamKeysOnlyUsesMetadataStream(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/range-stream-keys-only/a"), Value: []byte("large-user-payload"),
	})
	require.NoError(t, err)
	probe := &keysOnlyRangeStreamProbeShim{BackendShim: server.backend}
	server.backend = probe

	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key:      []byte("/range-stream-keys-only/"),
		RangeEnd: []byte("/range-stream-keys-only0"),
		KeysOnly: true,
	}, stream))
	require.Zero(t, probe.ordinaryCalls)
	require.Equal(t, 1, probe.metadataCalls)

	var kvs []*mvccpb.KeyValue
	for _, chunk := range stream.sent {
		kvs = append(kvs, chunk.RangeResponse.Kvs...)
	}
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("/range-stream-keys-only/a"), kvs[0].Key)
	require.Empty(t, kvs[0].Value)
	require.Equal(t, put.Header.Revision, kvs[0].CreateRevision)
	require.Equal(t, put.Header.Revision, kvs[0].ModRevision)
	require.EqualValues(t, 1, kvs[0].Version)
	require.Zero(t, kvs[0].Lease)
}

func TestRangeStreamNormalizesNegativeRevisionBeforeBackend(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/negative-revision/key"), Value: []byte("value"),
	})
	require.NoError(t, err)
	recorder := &revisionRecordingRangeStreamBackendShim{BackendShim: server.backend}
	server.backend = recorder

	for _, tc := range []struct {
		name string
		wire int64
		want uint64
	}{
		{name: "minus one", wire: -1, want: uint64(put.Header.Revision)},
		{name: "minimum int64", wire: math.MinInt64, want: uint64(put.Header.Revision)},
		{name: "positive", wire: put.Header.Revision, want: uint64(put.Header.Revision)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := &fakeRangeStreamServer{ctx: ctx}
			require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/negative-revision/"), RangeEnd: []byte("/negative-revision0"), Revision: tc.wire,
			}, stream))
			require.Equal(t, tc.want, recorder.revision,
				"etcd revision <= 0 means latest; the backend must receive the pinned start revision without unsigned wrap")
			require.Equal(t, tc.wire <= 0, recorder.latest,
				"the backend must retain whether the explicit revision came from an original latest request")
		})
	}
}

func TestRangeStreamPinsLatestBeforeBackendStreamStarts(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	seed, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/pin-before-open/a"), Value: []byte("seed"),
	})
	require.NoError(t, err)
	underlying := server.backend
	shim := &writeBeforeRangeStreamBackendShim{BackendShim: underlying}
	shim.before = func() error {
		_, writeErr := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte("/pin-before-open/z"), Value: []byte("late"),
		})
		return writeErr
	}
	server.backend = shim

	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/pin-before-open/"), RangeEnd: []byte("/pin-before-open0"),
	}, stream))

	require.Equal(t, uint64(seed.Header.Revision), shim.revision,
		"latest RangeStream must pass its already observed header revision to the backend")
	var keys []string
	for _, chunk := range stream.sent {
		for _, kv := range chunk.RangeResponse.Kvs {
			keys = append(keys, string(kv.Key))
		}
	}
	require.Equal(t, []string{"/pin-before-open/a"}, keys,
		"a write after the start revision is observed must not leak into the stream")
	final := stream.sent[len(stream.sent)-1].RangeResponse
	require.Equal(t, seed.Header.Revision, final.Header.Revision)
	require.EqualValues(t, 1, final.Count)
}

func TestRangeStreamEmptyRangeStillSendsHeaderRevision(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	// One unrelated key so the store has a non-zero revision, then stream an
	// empty range.
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/other/x"), Value: []byte("v")})
	require.NoError(t, err)

	rs := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/"),
		RangeEnd: []byte("/registry/pods0"),
	}, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rs.sent, "empty range must still emit a header-only chunk")
	last := rs.sent[len(rs.sent)-1]
	require.Empty(t, last.RangeResponse.Kvs)
	require.Greater(t, last.RangeResponse.Header.Revision, int64(0),
		"apiserver reads Header.Revision as the sync's initial revision")
}

func TestHistoricalSerializableRangeStreamBypassesLeaderRevisionSync(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	prefix := []byte("/historical-serializable-stream/")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: append(append([]byte(nil), prefix...), 'a'), Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: append(append([]byte(nil), prefix...), 'a'), Value: []byte("v2")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: append(append([]byte(nil), prefix...), 'b'), Value: []byte("vb")})
	require.NoError(t, err)

	syncErr := errors.New("leader revision unavailable")
	server.peers = testPeerService{isLeader: true, syncReadFn: func(context.Context) error { return syncErr }}
	rs := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{
		Key: prefix, RangeEnd: []byte(clientv3.GetPrefixRangeEnd(string(prefix))),
		Revision: first.Header.Revision, Serializable: true,
	}, rs)
	require.NoError(t, err, "a leader's historical serializable RangeStream must bypass coordination like etcd")
	var values []string
	for _, chunk := range rs.sent {
		for _, kv := range chunk.RangeResponse.Kvs {
			values = append(values, string(kv.Value))
		}
	}
	require.Equal(t, []string{"v1"}, values)
}

func TestRangeStreamPointAndEmptyIntervalsMatchUnaryRange(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/shape/a"), Value: []byte("value"),
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "point hit", req: &etcdserverpb.RangeRequest{Key: []byte("/shape/a")}},
		{name: "point miss", req: &etcdserverpb.RangeRequest{Key: []byte("/shape/missing")}},
		{name: "equal interval", req: &etcdserverpb.RangeRequest{
			Key: []byte("/shape/a"), RangeEnd: []byte("/shape/a"),
		}},
		{name: "reversed interval", req: &etcdserverpb.RangeRequest{
			Key: []byte("/shape/z"), RangeEnd: []byte("/shape/a"),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			unary, rangeErr := server.Range(ctx, proto.Clone(test.req).(*etcdserverpb.RangeRequest))
			require.NoError(t, rangeErr)
			stream := &fakeRangeStreamServer{ctx: ctx}
			require.NoError(t, server.RangeStream(
				proto.Clone(test.req).(*etcdserverpb.RangeRequest), stream,
			))
			merged := &etcdserverpb.RangeResponse{}
			for _, chunk := range stream.sent {
				proto.Merge(merged, chunk.RangeResponse)
			}
			require.True(t, proto.Equal(unary, merged), "stream=%s unary=%s", merged, unary)
		})
	}
}

func TestRangeStreamPreservesBinaryUserKeyOrdering(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()

	ctx := context.Background()
	for i, key := range [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0x01}, {0x01}} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte{0x00}, RangeEnd: []byte{0x01},
	}, stream))

	var keys [][]byte
	var final *etcdserverpb.RangeResponse
	for _, response := range stream.sent {
		require.NotNil(t, response.RangeResponse)
		for _, kv := range response.RangeResponse.Kvs {
			keys = append(keys, kv.Key)
		}
		final = response.RangeResponse
	}
	require.Equal(t, [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0x01}}, keys)
	require.NotNil(t, final)
	require.Equal(t, int64(3), final.Count)
	require.False(t, final.More)
}

func TestRangeStreamMergesTerminalMetadataIntoFinalDataFrame(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/stream-final/key"), Value: []byte("value"),
	})
	require.NoError(t, err)
	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream-final/"), RangeEnd: []byte("/stream-final0"),
	}, stream))

	require.Len(t, stream.sent, 1)
	response := stream.sent[0].RangeResponse
	require.NotNil(t, response)
	require.NotNil(t, response.Header)
	require.Equal(t, put.Header.Revision, response.Header.Revision)
	require.Equal(t, int64(1), response.Count)
	require.False(t, response.More)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, []byte("/stream-final/key"), response.Kvs[0].Key)
}

func TestRangeStreamFullKeyspaceSentinelPreservesBinaryKeys(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	for i, key := range [][]byte{{0x00}, {0x00, 0x00}, {0x00, '$'}, {0x01}, []byte("ordinary")} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte{byte(i)}})
		require.NoError(t, err)
	}
	req := &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}
	unary, err := server.Range(ctx, proto.Clone(req).(*etcdserverpb.RangeRequest))
	require.NoError(t, err)
	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(req, stream))
	merged := &etcdserverpb.RangeResponse{}
	for _, chunk := range stream.sent {
		proto.Merge(merged, chunk.RangeResponse)
	}
	require.True(t, proto.Equal(unary, merged), "stream=%s unary=%s", merged, unary)
}

func TestSerializableRangeStreamBypassesLeaderRevisionSync(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/stream/key"), Value: []byte("value")})
	require.NoError(t, err)

	syncErr := errors.New("leader revision unavailable")
	server.peers = testPeerService{syncReadFn: func(context.Context) error { return syncErr }}
	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream/"), RangeEnd: []byte("/stream0"), Serializable: true,
	}, stream))
	require.NotEmpty(t, stream.sent)

	linearizable := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/stream/"), RangeEnd: []byte("/stream0")}, linearizable)
	requireReadBarrierUnavailable(t, err, syncErr.Error())
}

func TestFollowerLinearizableRangeStreamProxiesBeforeLocalBarrier(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	request := &etcdserverpb.RangeRequest{Key: []byte("/stream/"), RangeEnd: []byte("/stream0")}
	want := &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{
		Header: proxiedResponseHeader(server, 42), Count: 1,
		Kvs: []*mvccpb.KeyValue{{Key: []byte("/stream/key"), Value: []byte("value"), CreateRevision: 1, ModRevision: 1, Version: 1}},
	}}
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		syncReadFn: func(context.Context) error {
			t.Fatal("follower must proxy RangeStream before a local read barrier")
			return errors.New("unreachable")
		},
		rangeStreamFn: func(_ context.Context, got *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			require.Equal(t, request, got)
			results := make(chan etcdproxy.RangeStreamResult, 1)
			results <- etcdproxy.RangeStreamResult{Response: want}
			close(results)
			return results, nil
		},
	}
	stream := &fakeRangeStreamServer{ctx: context.Background()}
	require.NoError(t, server.RangeStream(request, stream))
	require.Equal(t, []*etcdserverpb.RangeStreamResponse{want}, stream.sent)
}

func TestFollowerSerializableLatestRangeStreamPrefersLeaderAndFallsBackBeforeFirstFrame(t *testing.T) {
	for _, tc := range []struct {
		name      string
		proxyErr  error
		wantValue string
		wantRev   int64
		wantUsed  bool
	}{
		{name: "healthy leader", wantValue: "leader", wantRev: 47},
		{name: "leader unavailable", proxyErr: status.Error(codes.Unavailable, "leader unavailable"), wantValue: "checkpoint", wantRev: 41, wantUsed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, cleanup := newRangeStreamTestServer(t)
			defer cleanup()
			checkpoint := backend.SerializableCheckpoint{
				Revision: 41, Timestamp: 111, CompactRevision: 9, ValidUntil: time.Now().Add(time.Minute),
			}
			checkpointRange := &checkpointRangeBackendShim{BackendShim: server.backend, checkpoint: checkpoint}
			checkpointStream := &checkpointRangeStreamBackendShim{checkpointRangeBackendShim: checkpointRange}
			server.backend = checkpointStream
			server.tokens.snapshots = newAuthSnapshotCache(checkpointStream)
			request := &etcdserverpb.RangeRequest{
				Key: []byte("/checkpoint/"), RangeEnd: []byte("/checkpoint0"), Serializable: true,
			}
			forwarded := 0
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				rangeStreamFn: func(_ context.Context, got *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
					forwarded++
					require.Equal(t, request, got)
					if tc.proxyErr != nil {
						return nil, tc.proxyErr
					}
					results := make(chan etcdproxy.RangeStreamResult, 1)
					results <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
						RangeResponse: &etcdserverpb.RangeResponse{
							Header: proxiedResponseHeader(server, tc.wantRev), Count: 1,
							Kvs: []*mvccpb.KeyValue{{
								Key: []byte("/checkpoint/key"), Value: []byte("leader"),
								CreateRevision: tc.wantRev, ModRevision: tc.wantRev, Version: 1,
							}},
						},
					}}
					close(results)
					return results, nil
				},
			}

			stream := &fakeRangeStreamServer{ctx: context.Background()}
			require.NoError(t, server.RangeStream(request, stream))
			require.Equal(t, 1, forwarded)
			require.Equal(t, tc.wantUsed, checkpointStream.used)
			require.NotEmpty(t, stream.sent)
			terminal := stream.sent[len(stream.sent)-1].GetRangeResponse()
			require.Equal(t, tc.wantRev, terminal.GetHeader().GetRevision())
			require.Equal(t, []byte(tc.wantValue), terminal.Kvs[0].Value)
		})
	}
}

func TestFollowerRangeStreamRejectsInvalidProxyTermination(t *testing.T) {
	tests := []struct {
		name       string
		results    func(*RPCServer) <-chan etcdproxy.RangeStreamResult
		message    string
		sentFrames int
	}{
		{
			name:    "nil channel",
			results: func(*RPCServer) <-chan etcdproxy.RangeStreamResult { return nil },
			message: "forwarded range stream returned a nil result channel",
		},
		{
			name: "closed without terminal",
			results: func(*RPCServer) <-chan etcdproxy.RangeStreamResult {
				ch := make(chan etcdproxy.RangeStreamResult)
				close(ch)
				return ch
			},
			message: "forwarded range stream ended without terminal metadata",
		},
		{
			name: "empty result",
			results: func(*RPCServer) <-chan etcdproxy.RangeStreamResult {
				ch := make(chan etcdproxy.RangeStreamResult, 1)
				ch <- etcdproxy.RangeStreamResult{}
				close(ch)
				return ch
			},
			message: "forwarded range stream returned an empty result",
		},
		{
			name: "mixed response and error",
			results: func(*RPCServer) <-chan etcdproxy.RangeStreamResult {
				ch := make(chan etcdproxy.RangeStreamResult, 1)
				ch <- etcdproxy.RangeStreamResult{
					Response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Header: txnHeader(42)}},
					Err:      errors.New("mixed proxy result"),
				}
				close(ch)
				return ch
			},
			message: "forwarded range stream result mixed response and error",
		},
		{
			name: "continued after terminal",
			results: func(server *RPCServer) <-chan etcdproxy.RangeStreamResult {
				ch := make(chan etcdproxy.RangeStreamResult, 2)
				ch <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Header: proxiedResponseHeader(server, 42)}}}
				ch <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("late")}}}}}
				close(ch)
				return ch
			},
			message:    "forwarded range stream continued after terminal metadata",
			sentFrames: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			server.peers = testPeerService{
				proxyEnabled: true,
				epochFn:      func() (uint64, bool) { return 7, false },
				rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
					return test.results(server), nil
				},
			}
			stream := &fakeRangeStreamServer{ctx: context.Background()}

			err := server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/forward-invalid/"), RangeEnd: []byte("/forward-invalid0"),
			}, stream)
			requireRangeStreamStatusError(t, err, codes.Unavailable, test.message)
			require.Len(t, stream.sent, test.sentFrames)
			require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureProtocol))
		})
	}
}

func TestFollowerRangeStreamStopsWaitingForProxyCloseOnCallerCancellation(t *testing.T) {
	server, cleanup := newRangeStreamTestServerWithMetrics(t, &recordingMetrics{})
	defer cleanup()

	results := make(chan etcdproxy.RangeStreamResult, 1)
	results <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
		RangeResponse: &etcdserverpb.RangeResponse{Header: proxiedResponseHeader(server, 42)},
	}}
	go func() {
		time.Sleep(250 * time.Millisecond)
		close(results)
	}()
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			return results, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stream := &fakeRangeStreamServer{ctx: ctx}
	started := time.Now()
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/forward-cancel/"), RangeEnd: []byte("/forward-cancel0"),
	}, stream)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 200*time.Millisecond)
	require.Len(t, stream.sent, 1, "the validated terminal frame may precede the final stream status")
}

func TestFollowerRangeStreamRejectsInvalidProxyPayload(t *testing.T) {
	validKV := func(key string) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), Value: []byte("value"), CreateRevision: 1, ModRevision: 1, Version: 1}
	}
	for _, test := range []struct {
		name    string
		request *etcdserverpb.RangeRequest
		frames  []*etcdserverpb.RangeResponse
		message string
		sent    int
	}{
		{name: "zero terminal revision", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(0)}}, message: "forwarded range stream returned a non-positive terminal revision"},
		{name: "nil key-value", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{nil}}}, message: "forwarded range stream returned a nil key-value"},
		{name: "outside range", request: &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned a key outside the requested range"},
		{name: "cross-frame descending", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Kvs: []*mvccpb.KeyValue{validKV("b")}}, {Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned key-values outside the requested sort order", sent: 1},
		{name: "default duplicate across frames", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Kvs: []*mvccpb.KeyValue{validKV("a")}}, {Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned a duplicate key", sent: 1},
		{name: "none version target descending", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_VERSION}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(3), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 2, Version: 2}, {Key: []byte("b"), CreateRevision: 3, ModRevision: 3, Version: 1}}}}, message: "forwarded range stream returned key-values outside the requested sort order"},
		{name: "duplicate key with equal version", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_VERSION}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a"), validKV("a")}}}, message: "forwarded range stream returned a duplicate key"},
		{name: "keys-only value", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), KeysOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned a value for a keys-only request"},
		{name: "fast keys-only lease", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), KeysOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 7}}}}, message: "forwarded range stream returned a lease for a fast keys-only request"},
		{name: "invalid lifecycle", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("a")}}}}, message: "forwarded range stream returned invalid key-value revision metadata"},
		{name: "future key-value", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Revision: 1}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 2, ModRevision: 2, Version: 1}}}}, message: "forwarded range stream returned a key-value newer than the requested snapshot"},
		{name: "early aggregate metadata", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Count: 1, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned aggregate metadata before the terminal frame"},
		{name: "empty non-terminal frame", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{}, {Header: txnHeader(2)}}, message: "forwarded range stream returned an empty non-terminal frame"},
		{name: "empty terminal after data", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Kvs: []*mvccpb.KeyValue{validKV("a")}}, {Header: txnHeader(2), Count: 1}}, message: "forwarded range stream returned terminal metadata without final key-values", sent: 1},
		{name: "count below sent", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned an invalid terminal count"},
		{name: "inconsistent more", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned inconsistent terminal count and more metadata"},
		{name: "count-only payload", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned key-values for a count-only request"},
		{name: "exact key count above cardinality", request: &etcdserverpb.RangeRequest{Key: []byte("a"), CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 2}}, message: "forwarded range stream returned count 2 above exact-key cardinality"},
		{name: "exact key limited pagination above cardinality", request: &etcdserverpb.RangeRequest{Key: []byte("a"), Limit: 1}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 2, More: true, Kvs: []*mvccpb.KeyValue{validKV("a")}}}, message: "forwarded range stream returned count 2 above exact-key cardinality"},
		{name: "reverse count-only range", request: &etcdserverpb.RangeRequest{Key: []byte("z"), RangeEnd: []byte("a"), CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1}}, message: "forwarded range stream returned non-empty metadata for an empty requested range"},
		{name: "equal count-only range", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("a"), CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1}}, message: "forwarded range stream returned non-empty metadata for an empty requested range"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			frames := make([]*etcdserverpb.RangeResponse, 0, len(test.frames))
			for _, frame := range test.frames {
				if frame.Header != nil {
					identity := proxiedResponseHeader(server, frame.Header.Revision)
					frame.Header.ClusterId = identity.ClusterId
					frame.Header.MemberId = identity.MemberId
					frame.Header.RaftTerm = identity.RaftTerm
					server.staticMembers = []*etcdserverpb.Member{{ID: identity.MemberId}}
				}
				frames = append(frames, frame)
			}
			server.peers = testPeerService{
				proxyEnabled: true,
				epochFn:      func() (uint64, bool) { return 7, false },
				rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
					ch := make(chan etcdproxy.RangeStreamResult, len(frames))
					for _, frame := range frames {
						ch <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{RangeResponse: frame}}
					}
					close(ch)
					return ch, nil
				},
			}
			stream := &fakeRangeStreamServer{ctx: context.Background()}

			err := server.RangeStream(test.request, stream)
			requireRangeStreamStatusError(t, err, codes.DataLoss, test.message)
			require.Len(t, stream.sent, test.sent)
			require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureProtocol))
		})
	}
}

func TestFollowerRangeStreamRejectsInvalidProxyHeaderIdentity(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*etcdserverpb.ResponseHeader)
		message string
	}{
		{name: "zero cluster", mutate: func(header *etcdserverpb.ResponseHeader) { header.ClusterId = 0 }, message: "forwarded range stream returned a response with a zero cluster ID"},
		{name: "foreign cluster", mutate: func(header *etcdserverpb.ResponseHeader) { header.ClusterId++ }, message: "forwarded range stream returned a response for a foreign cluster"},
		{name: "zero member", mutate: func(header *etcdserverpb.ResponseHeader) { header.MemberId = 0 }, message: "forwarded range stream returned a response with a zero member ID"},
		{name: "unknown member", mutate: func(header *etcdserverpb.ResponseHeader) { header.MemberId++ }, message: "forwarded range stream returned a response for an unknown member"},
		{name: "zero raft term", mutate: func(header *etcdserverpb.ResponseHeader) { header.RaftTerm = 0 }, message: "forwarded range stream returned a response with a zero raft term"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			beforeRevision := server.backend.GetCurrentRevision()
			header := proxiedResponseHeader(server, 2)
			server.staticMembers = []*etcdserverpb.Member{{ID: header.MemberId}}
			test.mutate(header)
			server.peers = testPeerService{
				proxyEnabled: true,
				epochFn:      func() (uint64, bool) { return 7, false },
				rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
					ch := make(chan etcdproxy.RangeStreamResult, 1)
					ch <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
						RangeResponse: &etcdserverpb.RangeResponse{Header: header},
					}}
					close(ch)
					return ch, nil
				},
			}
			stream := &fakeRangeStreamServer{ctx: context.Background()}

			err := server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, stream)
			requireRangeStreamStatusError(t, err, codes.DataLoss, test.message)
			require.Empty(t, stream.sent)
			require.Equal(t, beforeRevision, server.backend.GetCurrentRevision(), "invalid identity must not advance the follower watermark")
			require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureProtocol))
			require.Equal(t, []interface{}{int64(0), 1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCRange))
		})
	}
}

func TestRangeStreamProxyPayloadValidatorAcceptsCanonicalStreams(t *testing.T) {
	kv := func(key string) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), CreateRevision: 2, ModRevision: 2, Version: 1}
	}
	for _, test := range []struct {
		name    string
		request *etcdserverpb.RangeRequest
		frames  []*etcdserverpb.RangeResponse
	}{
		{name: "empty", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2)}}},
		{name: "multi-frame unlimited", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, frames: []*etcdserverpb.RangeResponse{{Kvs: []*mvccpb.KeyValue{kv("a")}}, {Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{kv("b")}}}},
		{name: "limited more", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 2, More: true, Kvs: []*mvccpb.KeyValue{kv("a")}}}},
		{name: "count only", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 3}}},
		{name: "exact key count only", request: &etcdserverpb.RangeRequest{Key: []byte("a"), CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1}}},
		{name: "exact key limited", request: &etcdserverpb.RangeRequest{Key: []byte("a"), Limit: 1}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a")}}}},
		{name: "from key count only", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, CountOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 3}}},
		{name: "keys only", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), KeysOnly: true}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a")}}}},
		{name: "value-sort keys only retains lease", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), KeysOnly: true, SortTarget: etcdserverpb.RangeRequest_VALUE}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 7}}}}},
		{name: "none version target ascending", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_VERSION}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(3), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("a"), CreateRevision: 1, ModRevision: 2, Version: 2}}}}},
		{name: "none value target keys-only projected order", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), KeysOnly: true, SortTarget: etcdserverpb.RangeRequest_VALUE}, frames: []*etcdserverpb.RangeResponse{{Header: txnHeader(3), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 7}, {Key: []byte("a"), CreateRevision: 1, ModRevision: 2, Version: 2, Lease: 8}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			validator := newRangeStreamProxyPayloadValidator(test.request)
			for _, frame := range test.frames {
				require.NoError(t, validator.validate(frame))
			}
		})
	}
}

func TestRangeStreamProxyPayloadValidatorBoundsDefaultKeyOrderingState(t *testing.T) {
	const keyCount = 10_000
	request := &etcdserverpb.RangeRequest{
		Key: []byte("/proxy-bounded/"), RangeEnd: []byte("/proxy-bounded0"),
	}
	validator := newRangeStreamProxyPayloadValidator(request)
	for index := 0; index < keyCount; index++ {
		response := &etcdserverpb.RangeResponse{Kvs: []*mvccpb.KeyValue{{
			Key: []byte(fmt.Sprintf("/proxy-bounded/%08d", index)), CreateRevision: 1, ModRevision: 1, Version: 1,
		}}}
		if index == keyCount-1 {
			response.Header = txnHeader(1)
			response.Count = keyCount
		}
		require.NoError(t, validator.validate(response))
	}
	// The default RangeStream order is strictly ascending by key, so adjacent
	// comparison detects both disorder and duplicates. Retaining every key here
	// would make a follower's validation memory grow with an unlimited LIST.
	require.Nil(t, validator.seenKeys)
	require.Equal(t, &mvccpb.KeyValue{Key: []byte("/proxy-bounded/00009999")}, validator.previousKV)
}

func TestFollowerRangeStreamRejectsNegativeTerminalRevision(t *testing.T) {
	rec := &recordingMetrics{}
	server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
	defer cleanup()
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			ch := make(chan etcdproxy.RangeStreamResult, 1)
			ch <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
				RangeResponse: &etcdserverpb.RangeResponse{Header: txnHeader(-1)},
			}}
			close(ch)
			return ch, nil
		},
	}
	stream := &fakeRangeStreamServer{ctx: context.Background()}

	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/forward-negative/"), RangeEnd: []byte("/forward-negative0"),
	}, stream)
	requireRangeStreamStatusError(t, err, codes.DataLoss, "forwarded range stream returned a non-positive terminal revision")
	require.Empty(t, stream.sent)
	require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureProtocol))
}

func TestFollowerRangeStreamSendFailureUsesCanonicalMetric(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []interface{}
	}{
		{name: "transport failure", err: status.Error(codes.Unavailable, "proxy client send failed"), want: []interface{}{int64(0), 1}},
		{name: "client cancellation", err: status.Error(codes.Canceled, "context canceled"), want: []interface{}{int64(0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			server.peers = testPeerService{
				proxyEnabled: true,
				epochFn:      func() (uint64, bool) { return 7, false },
				rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
					ch := make(chan etcdproxy.RangeStreamResult, 1)
					ch <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
						RangeResponse: &etcdserverpb.RangeResponse{Header: proxiedResponseHeader(server, 42)},
					}}
					close(ch)
					return ch, nil
				},
			}
			stream := &failingRangeStreamServer{
				fakeRangeStreamServer: fakeRangeStreamServer{ctx: context.Background()},
				err:                   test.err,
			}

			err := server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/forward-send/"), RangeEnd: []byte("/forward-send0"),
			}, stream)
			require.ErrorIs(t, err, test.err)
			require.Equal(t, test.want, recordedRangeStreamFailureValues(rec, rangeStreamFailureSend))
		})
	}
}

func TestFollowerRangeStreamMapsInternalProxyCancellationToLeaderChanged(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	calls := 0
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			calls++
			results := make(chan etcdproxy.RangeStreamResult, 1)
			results <- etcdproxy.RangeStreamResult{Err: status.Error(codes.Canceled, "grpc: the client connection is closing")}
			close(results)
			return results, nil
		},
	}
	ctx := context.Background()
	err := server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/stream/"), RangeEnd: []byte("/stream0")},
		&fakeRangeStreamServer{ctx: ctx})
	require.ErrorIs(t, err, rpctypes.ErrGRPCLeaderChanged)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, preResponseStreamProxyAttempts, calls)
}

func TestFollowerRangeStreamRetriesPeerDrainBeforeFirstPublicFrame(t *testing.T) {
	rec := &recordingMetrics{}
	server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
	defer cleanup()
	calls := 0
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			calls++
			if calls == 1 {
				return nil, proxyprotocol.ErrPeerStreamDrained
			}
			results := make(chan etcdproxy.RangeStreamResult, 1)
			results <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
				RangeResponse: &etcdserverpb.RangeResponse{Header: proxiedResponseHeader(server, 42)},
			}}
			close(results)
			return results, nil
		},
	}
	stream := &fakeRangeStreamServer{ctx: context.Background()}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream/"), RangeEnd: []byte("/stream0"),
	}, stream))
	require.Equal(t, 2, calls)
	require.Len(t, stream.sent, 1)
	require.EqualValues(t, 42, stream.sent[0].RangeResponse.Header.Revision)
	require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamProxyRetryValues(rec))
	require.Equal(t, []interface{}{int64(0)}, recordedRangeStreamFailureValues(rec, rangeStreamFailureBackend))
}

func TestFollowerRangeStreamDoesNotJoinLeadersAfterPublicFrame(t *testing.T) {
	rec := &recordingMetrics{}
	server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
	defer cleanup()
	calls := 0
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			calls++
			results := make(chan etcdproxy.RangeStreamResult, 2)
			results <- etcdproxy.RangeStreamResult{Response: &etcdserverpb.RangeStreamResponse{
				RangeResponse: &etcdserverpb.RangeResponse{Kvs: []*mvccpb.KeyValue{{
					Key: []byte("/stream/a"), Value: []byte("value"), CreateRevision: 1, ModRevision: 1, Version: 1,
				}}},
			}}
			results <- etcdproxy.RangeStreamResult{Err: proxyprotocol.ErrPeerStreamDrained}
			close(results)
			return results, nil
		},
	}
	stream := &fakeRangeStreamServer{ctx: context.Background()}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream/"), RangeEnd: []byte("/stream0"),
	}, stream)
	require.ErrorIs(t, err, rpctypes.ErrGRPCLeaderChanged)
	require.Equal(t, 1, calls)
	require.Len(t, stream.sent, 1)
	require.Equal(t, "/stream/a", string(stream.sent[0].RangeResponse.Kvs[0].Key))
	require.Equal(t, []interface{}{int64(0)}, recordedRangeStreamProxyRetryValues(rec))
	require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureBackend))
}

func TestFollowerRangeStreamPreservesCallerCancellation(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	calls := 0
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		rangeStreamFn: func(context.Context, *etcdserverpb.RangeRequest) (<-chan etcdproxy.RangeStreamResult, error) {
			calls++
			results := make(chan etcdproxy.RangeStreamResult, 1)
			results <- etcdproxy.RangeStreamResult{Err: context.Canceled}
			close(results)
			return results, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/stream/"), RangeEnd: []byte("/stream0")},
		&fakeRangeStreamServer{ctx: ctx})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestFreshLeaderSerializableLatestRangeStreamFallsBackBeforeFirstFrame(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 41, Timestamp: 111, CompactRevision: 9, ValidUntil: time.Now().Add(time.Minute),
	}
	checkpointRange := &checkpointRangeBackendShim{BackendShim: server.backend, checkpoint: checkpoint}
	checkpointStream := &checkpointRangeStreamBackendShim{checkpointRangeBackendShim: checkpointRange}
	shim := &liveBlockingCheckpointRangeStreamBackendShim{checkpointRangeStreamBackendShim: checkpointStream}
	server.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{
		isLeader: true,
		epochFn:  func() (uint64, bool) { return 5, true },
	}

	stream := &fakeRangeStreamServer{ctx: context.Background()}
	started := time.Now()
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/checkpoint/"), RangeEnd: []byte("/checkpoint0"), Serializable: true,
	}, stream)
	require.NoError(t, err)
	require.Equal(t, 1, shim.liveAttempts)
	require.True(t, checkpointStream.used)
	require.GreaterOrEqual(t, time.Since(started), serializableLiveReadBudget)
	require.NotEmpty(t, stream.sent)
	terminal := stream.sent[len(stream.sent)-1].GetRangeResponse()
	require.Equal(t, int64(checkpoint.Revision), terminal.GetHeader().GetRevision())
	require.EqualValues(t, 1, terminal.Count)
	require.Equal(t, []byte("checkpoint"), terminal.Kvs[0].Value)
}

func TestFreshLeaderSerializableLatestRangeStreamNeverFallsBackAfterFrame(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 41, Timestamp: 111, CompactRevision: 9, ValidUntil: time.Now().Add(time.Minute),
	}
	checkpointRange := &checkpointRangeBackendShim{BackendShim: server.backend, checkpoint: checkpoint}
	checkpointStream := &checkpointRangeStreamBackendShim{checkpointRangeBackendShim: checkpointRange}
	shim := &partialFailureCheckpointRangeStreamBackendShim{checkpointRangeStreamBackendShim: checkpointStream}
	server.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{
		isLeader: true,
		epochFn:  func() (uint64, bool) { return 5, true },
	}

	stream := &fakeRangeStreamServer{ctx: context.Background()}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/partial/"), RangeEnd: []byte("/partial0"), Serializable: true,
	}, stream)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, shim.liveAttempts)
	require.False(t, checkpointStream.used, "a partial public stream must never join a checkpoint snapshot")
	require.Len(t, stream.sent, 1)
	require.Equal(t, []byte("/partial/a"), stream.sent[0].GetRangeResponse().Kvs[0].Key)
}

func TestStaleLeaderSerializableLatestRangeStreamUsesProtectedCheckpoint(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 41, Timestamp: 111, CompactRevision: 9, ValidUntil: time.Now().Add(time.Minute),
	}
	checkpointRange := &checkpointRangeBackendShim{BackendShim: server.backend, checkpoint: checkpoint}
	shim := &checkpointRangeStreamBackendShim{checkpointRangeBackendShim: checkpointRange}
	server.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{
		isLeader: true,
		epochFn:  func() (uint64, bool) { return 5, false },
		syncReadFn: func(context.Context) error {
			t.Fatal("serializable RangeStream must not perform leader revision sync")
			return nil
		},
	}

	for _, revision := range []int64{0, -1, math.MinInt64} {
		t.Run(fmt.Sprintf("revision-%d", revision), func(t *testing.T) {
			shim.used = false
			stream := &fakeRangeStreamServer{ctx: context.Background()}
			err := server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/checkpoint/"), RangeEnd: []byte("/checkpoint0"),
				Revision: revision, Serializable: true,
			}, stream)
			require.NoError(t, err)
			require.True(t, shim.used)
			require.NotEmpty(t, stream.sent)
			terminal := stream.sent[len(stream.sent)-1].GetRangeResponse()
			require.NotNil(t, terminal.Header)
			require.Equal(t, int64(checkpoint.Revision), terminal.Header.Revision)
			require.EqualValues(t, 1, terminal.Count)
			require.Equal(t, []byte("checkpoint"), terminal.Kvs[0].Value)
		})
	}
}

func TestHistoricalRangeStreamUsesDurableFollowerWatermark(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	key := []byte("/stream-history/key")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		rev, getErr := server.backend.GetDurableRevision(ctx)
		return getErr == nil && rev > uint64(first.Header.Revision)
	}, time.Second, time.Millisecond)
	server.peers = testPeerService{isLeader: false, syncReadFn: func(context.Context) error {
		t.Fatal("bounded historical RangeStream must not sync with the leader")
		return nil
	}}

	rs := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream-history/"), RangeEnd: []byte("/stream-history0"),
		Revision: first.Header.Revision, Serializable: true,
	}, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rs.sent)
	var values [][]byte
	for _, chunk := range rs.sent {
		for _, kv := range chunk.RangeResponse.Kvs {
			values = append(values, kv.Value)
		}
	}
	require.Equal(t, [][]byte{[]byte("v1")}, values)
	require.Equal(t, second.Header.Revision,
		rs.sent[len(rs.sent)-1].RangeResponse.Header.Revision)
}

func TestRangeStreamRejectsUnsupportedShapes(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name    string
		req     *etcdserverpb.RangeRequest
		wantErr error
		notErr  error
		code    codes.Code
		message string
	}{
		{
			name:    "emptyKey",
			req:     &etcdserverpb.RangeRequest{},
			wantErr: rpctypes.ErrGRPCEmptyKey,
			code:    codes.InvalidArgument,
			message: "etcdserver: key is not provided",
		},
		{
			name:    "invalidSortOrder",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), SortOrder: etcdserverpb.RangeRequest_SortOrder(99)},
			wantErr: rpctypes.ErrGRPCInvalidSortOption,
			code:    codes.InvalidArgument,
			message: "etcdserver: invalid sort option",
		},
		{
			name:    "invalidSortTarget",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), SortTarget: etcdserverpb.RangeRequest_SortTarget(99)},
			wantErr: rpctypes.ErrGRPCInvalidSortOption,
			code:    codes.InvalidArgument,
			message: "etcdserver: invalid sort option",
		},
		{
			name:    "modRevisionFilter",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), MinModRevision: 5},
			notErr:  rpctypes.ErrGRPCInvalidSortOption,
			code:    codes.Unimplemented,
			message: "RangeStream does not support revision filters",
		},
		{
			name:    "maxModRevisionFilter",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), MaxModRevision: 5},
			code:    codes.Unimplemented,
			message: "RangeStream does not support revision filters",
		},
		{
			name:    "minCreateRevisionFilter",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), MinCreateRevision: 5},
			code:    codes.Unimplemented,
			message: "RangeStream does not support revision filters",
		},
		{
			name:    "maxCreateRevisionFilter",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), MaxCreateRevision: 5},
			code:    codes.Unimplemented,
			message: "RangeStream does not support revision filters",
		},
		{
			name:    "sortOrder",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY},
			notErr:  rpctypes.ErrGRPCInvalidSortOption,
			code:    codes.Unimplemented,
			message: "RangeStream does not support custom sort orders",
		},
		{
			name: "sortOrderBeforeRevisionFilter",
			req: &etcdserverpb.RangeRequest{
				Key: []byte("/a"), RangeEnd: []byte("/b"),
				SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
				MinModRevision: 5,
			},
			notErr:  rpctypes.ErrGRPCInvalidSortOption,
			code:    codes.Unimplemented,
			message: "RangeStream does not support custom sort orders",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rs := &fakeRangeStreamServer{ctx: ctx}
			err := server.RangeStream(c.req, rs)
			require.EqualError(t, err, status.Error(c.code, c.message).Error())
			if c.wantErr != nil {
				require.ErrorIs(t, err, c.wantErr)
			}
			if c.notErr != nil {
				require.False(t, errors.Is(err, c.notErr))
			}
			require.Equal(t, c.code, status.Code(err))
			require.Equal(t, c.message, status.Convert(err).Message())
			require.Empty(t, rs.sent, "no chunks on a rejected request")
		})
	}
}

func TestRangeStreamSupportedOptionsMatchUnaryRange(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("/options/%02d", i)), Value: []byte(fmt.Sprintf("value-%02d", 7-i)),
		})
		require.NoError(t, err)
	}
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 37161})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/options/08"), Value: []byte("leased-value"), Lease: lease.ID,
	})
	require.NoError(t, err)

	tests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "count only ignores limit", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), CountOnly: true, Limit: 1}},
		{name: "count only overrides keys limit and sort", req: &etcdserverpb.RangeRequest{
			Key: []byte("/options/"), RangeEnd: []byte("/options0"), CountOnly: true, KeysOnly: true, Limit: 1,
			SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
		}},
		{name: "limit", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), Limit: 3}},
		{name: "explicit ascending key", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), Limit: 3, SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY}},
		{name: "none promotes non-key target to ascending", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), Limit: 3, SortOrder: etcdserverpb.RangeRequest_NONE, SortTarget: etcdserverpb.RangeRequest_VALUE}},
		{name: "none value sort precedes keys-only projection", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), Limit: 3, KeysOnly: true, SortOrder: etcdserverpb.RangeRequest_NONE, SortTarget: etcdserverpb.RangeRequest_VALUE}},
		{name: "keys only", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), KeysOnly: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unary, err := server.Range(ctx, proto.Clone(tt.req).(*etcdserverpb.RangeRequest))
			require.NoError(t, err)
			rs := &fakeRangeStreamServer{ctx: ctx}
			require.NoError(t, server.RangeStream(proto.Clone(tt.req).(*etcdserverpb.RangeRequest), rs))

			merged := &etcdserverpb.RangeResponse{}
			for _, chunk := range rs.sent {
				proto.Merge(merged, chunk.RangeResponse)
			}
			require.True(t, proto.Equal(unary, merged), "stream=%s unary=%s", merged, unary)
			if tt.name == "keys only" {
				for _, kv := range merged.Kvs {
					require.Zero(t, kv.Lease)
				}
			}
		})
	}
}

// TestRangeStreamMatchesUnaryRange asserts the streamed result is byte-identical
// to a unary Range over the same window.
func TestRangeStreamMatchesUnaryRange(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("/registry/svc/%02d", i)), Value: []byte(fmt.Sprintf("s%02d", i))})
		require.NoError(t, err)
	}

	unary, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte("/registry/svc/"), RangeEnd: []byte("/registry/svc0")})
	require.NoError(t, err)

	rs := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/registry/svc/"), RangeEnd: []byte("/registry/svc0")}, rs))

	streamed := make([]*mvccpb.KeyValue, 0, len(unary.Kvs))
	for _, chunk := range rs.sent {
		streamed = append(streamed, chunk.RangeResponse.Kvs...)
	}
	require.Len(t, streamed, len(unary.Kvs))
	for i := range unary.Kvs {
		require.Equal(t, unary.Kvs[i].Key, streamed[i].Key, "kv %d key", i)
		require.Equal(t, unary.Kvs[i].Value, streamed[i].Value, "kv %d value", i)
		require.Equal(t, unary.Kvs[i].ModRevision, streamed[i].ModRevision, "kv %d modRevision", i)
	}
}

func TestRangeStreamChunksRespectConfiguredMessageTarget(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	server.SetRequestLimits(defaultMaxTxnOps, 256)
	ctx := context.Background()

	for i := 0; i < 12; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("/chunk-target/%02d", i)),
			Value: []byte(fmt.Sprintf("value-%02d-%090d", i, i)),
		})
		require.NoError(t, err)
	}

	rs := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/chunk-target/"), RangeEnd: []byte("/chunk-target0"),
	}, rs))

	var keys [][]byte
	for _, chunk := range rs.sent {
		require.LessOrEqual(t, proto.Size(chunk), 256,
			"multi-KV RangeStream chunks must honor the configured wire-size target")
		for _, kv := range chunk.RangeResponse.Kvs {
			keys = append(keys, kv.Key)
		}
	}
	require.Len(t, keys, 12)
	for i, key := range keys {
		require.Equal(t, fmt.Sprintf("/chunk-target/%02d", i), string(key))
	}

	tracked := &listCountingBackendShim{BackendShim: server.backend}
	server.backend = tracked
	limited := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/chunk-target/"), RangeEnd: []byte("/chunk-target0"), Limit: 5,
	}, limited))
	require.Greater(t, len(limited.sent), 1,
		"a bounded RangeStream must not collapse a large result into one message")
	var limitedKeys int
	for i, chunk := range limited.sent {
		require.LessOrEqual(t, proto.Size(chunk), 256)
		if i != len(limited.sent)-1 {
			require.Nil(t, chunk.RangeResponse.Header)
			require.Zero(t, chunk.RangeResponse.Count)
			require.False(t, chunk.RangeResponse.More)
		}
		limitedKeys += len(chunk.RangeResponse.Kvs)
	}
	require.NotNil(t, limited.sent[len(limited.sent)-1].RangeResponse.Header)
	require.EqualValues(t, 12, limited.sent[len(limited.sent)-1].RangeResponse.Count)
	require.True(t, limited.sent[len(limited.sent)-1].RangeResponse.More)
	require.Equal(t, 5, limitedKeys)
	require.Zero(t, tracked.listCalls,
		"bounded RangeStream must not materialize a unary List response")
}

func TestRangeStreamLimitStopsBackendAndCountsAtPinnedRevision(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	probe := &limitedRangeStreamCountProbeShim{
		BackendShim: server.backend,
		total:       100,
		done:        make(chan struct{}),
	}
	server.backend = probe

	request := &etcdserverpb.RangeRequest{
		Key:          []byte("/limit-stop/"),
		RangeEnd:     []byte("/limit-stop0"),
		Limit:        1,
		Serializable: true,
	}
	stream := &fakeRangeStreamServer{ctx: context.Background()}
	require.NoError(t, server.RangeStream(request, stream))
	select {
	case <-probe.done:
	case <-time.After(time.Second):
		t.Fatal("backend stream did not stop")
	}

	require.True(t, probe.stopped, "Limit must cancel the backend stream before it scans the complete range")
	require.Less(t, probe.produced, probe.total)
	require.Equal(t, 1, probe.countCalls)
	require.NotNil(t, probe.countRequest)
	require.Equal(t, request.Key, probe.countRequest.Key)
	require.Equal(t, request.RangeEnd, probe.countRequest.RangeEnd)
	require.Positive(t, probe.countRequest.Revision)
	require.True(t, probe.countRequest.CountOnly)
	require.Len(t, stream.sent, 1)
	response := stream.sent[0].RangeResponse
	require.Len(t, response.Kvs, 1)
	require.NotNil(t, response.Header)
	require.EqualValues(t, probe.total, response.Count)
	require.True(t, response.More)
}

func TestSplitRangeStreamResponsePreservesAllFields(t *testing.T) {
	response := &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: 1, MemberId: 2, Revision: 3, RaftTerm: 4,
		},
		Kvs: []*mvccpb.KeyValue{
			{Key: []byte("a"), Value: make([]byte, 64)},
			{Key: []byte("b"), Value: make([]byte, 64)},
			{Key: []byte("c"), Value: make([]byte, 64)},
		},
		More:  true,
		Count: 9,
	}

	chunks := splitRangeStreamResponse(response, 100, true)
	require.Greater(t, len(chunks), 1, "test must exercise response splitting")
	merged := &etcdserverpb.RangeResponse{}
	for i, chunk := range chunks {
		require.NotNil(t, chunk.RangeResponse)
		if i != len(chunks)-1 {
			require.Nil(t, chunk.RangeResponse.Header)
			require.False(t, chunk.RangeResponse.More)
			require.Zero(t, chunk.RangeResponse.Count)
		}
		proto.Merge(merged, chunk.RangeResponse)
	}
	require.True(t, proto.Equal(response, merged), "merged chunks must equal the source response")
}

func TestRangeStreamLargeValueExceedingMessageTargetStillProgresses(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	server.SetRequestLimits(defaultMaxTxnOps, 256)
	ctx := context.Background()

	value := make([]byte, 1024)
	for i := 0; i < 20; i++ {
		value[0] = byte(i)
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("/large-stream/%02d", i)),
			Value: append([]byte(nil), value...),
		})
		require.NoError(t, err)
	}

	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/large-stream/"), RangeEnd: []byte("/large-stream0"),
	}, stream))

	var keys int
	for _, chunk := range stream.sent {
		require.LessOrEqual(t, len(chunk.RangeResponse.Kvs), 1,
			"an indivisible KV over the target must be emitted alone")
		keys += len(chunk.RangeResponse.Kvs)
	}
	require.Equal(t, 20, keys)
	require.GreaterOrEqual(t, len(stream.sent), 20,
		"every oversized KV must make forward progress and the last must carry terminal metadata")
	require.EqualValues(t, 20, stream.sent[len(stream.sent)-1].RangeResponse.Count)
}

func TestRangeStreamPinsRevisionAcrossConcurrentWrite(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	server.SetRequestLimits(defaultMaxTxnOps, 256)
	ctx := context.Background()

	const n = 20
	for i := 0; i < n; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("/pinned-stream/%02d", i)),
			Value: []byte(fmt.Sprintf("value-%02d-%090d", i, i)),
		})
		require.NoError(t, err)
	}
	pinnedRevision := server.backend.GetCurrentRevision()

	stream := &blockingRangeStreamServer{
		fakeRangeStreamServer: fakeRangeStreamServer{ctx: ctx},
		firstSent:             make(chan struct{}),
		release:               make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- server.RangeStream(&etcdserverpb.RangeRequest{
			Key: []byte("/pinned-stream/"), RangeEnd: []byte("/pinned-stream0"),
		}, stream)
	}()

	<-stream.firstSent
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/pinned-stream/99"), Value: []byte("late"),
	})
	require.NoError(t, err)
	close(stream.release)
	require.NoError(t, <-done)

	var keys []string
	for _, chunk := range stream.sent {
		for _, kv := range chunk.RangeResponse.Kvs {
			keys = append(keys, string(kv.Key))
		}
	}
	require.Len(t, keys, n)
	require.NotContains(t, keys, "/pinned-stream/99")
	final := stream.sent[len(stream.sent)-1].RangeResponse
	require.Equal(t, int64(pinnedRevision), final.Header.Revision)
	require.EqualValues(t, n, final.Count)
}

func TestRangeStreamRejectsPrematureBackendClose(t *testing.T) {
	rec := &recordingMetrics{}
	server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
	defer cleanup()
	server.backend = &prematureRangeStreamBackendShim{BackendShim: server.backend}

	stream := &fakeRangeStreamServer{ctx: context.Background()}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/premature/"), RangeEnd: []byte("/premature0"),
	}, stream)
	requireRangeStreamStatusError(t, err, codes.Unavailable, "range stream ended without terminal metadata")
	require.Empty(t, stream.sent,
		"the final bounded data chunk stays buffered until terminal metadata validates completion")
	require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureProtocol))
}

func TestRangeStreamRejectsNilBackendChannel(t *testing.T) {
	rec := &recordingMetrics{}
	server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
	defer cleanup()
	server.backend = &nilRangeStreamBackendShim{BackendShim: server.backend}

	stream := &fakeRangeStreamServer{ctx: context.Background()}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/nil-backend/"), RangeEnd: []byte("/nil-backend0"), Serializable: true,
	}, stream)
	requireRangeStreamStatusError(t, err, codes.Unavailable, "range stream backend returned a nil result channel")
	require.Empty(t, stream.sent)
	require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureBackend))
}

func TestRangeStreamRejectsMalformedBackendRevisionMetadata(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  func(uint64) rangeStreamChunk
		message string
	}{
		{
			name:    "nil response",
			result:  func(uint64) rangeStreamChunk { return rangeStreamChunk{} },
			message: "range stream backend returned a response without a header",
		},
		{
			name: "nil header",
			result: func(uint64) rangeStreamChunk {
				return rangeStreamChunk{resp: &etcdserverpb.RangeResponse{}}
			},
			message: "range stream backend returned a response without a header",
		},
		{
			name: "negative revision",
			result: func(uint64) rangeStreamChunk {
				return rangeStreamChunk{resp: &etcdserverpb.RangeResponse{Header: txnHeader(-1)}}
			},
			message: "range stream backend returned revision -1 for pinned revision",
		},
		{
			name: "different revision",
			result: func(revision uint64) rangeStreamChunk {
				return rangeStreamChunk{resp: &etcdserverpb.RangeResponse{Header: txnHeader(int64(revision + 1))}}
			},
			message: "for pinned revision",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			server.backend = &malformedRangeStreamBackendShim{BackendShim: server.backend, result: test.result}
			stream := &fakeRangeStreamServer{ctx: context.Background()}

			err := server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/malformed-backend/"), RangeEnd: []byte("/malformed-backend0"), Serializable: true,
			}, stream)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, test.message)
			require.Empty(t, stream.sent)
			require.Equal(t, []interface{}{int64(0), 1},
				recordedRangeStreamFailureValues(rec, rangeStreamFailureProtocol))
		})
	}
}

func TestRangeStreamSendFailureExcludesClientCancellation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []interface{}
	}{
		{name: "transport failure", err: status.Error(codes.Unavailable, "injected send failure"), want: []interface{}{int64(0), 1}},
		{name: "client cancellation", err: status.Error(codes.Canceled, "context canceled"), want: []interface{}{int64(0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			stream := &failingRangeStreamServer{
				fakeRangeStreamServer: fakeRangeStreamServer{ctx: context.Background()},
				err:                   test.err,
			}
			err := server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/send-failure/"), RangeEnd: []byte("/send-failure0"),
			}, stream)
			require.ErrorIs(t, err, test.err)
			require.Equal(t, test.want, recordedRangeStreamFailureValues(rec, rangeStreamFailureSend))
		})
	}
}

func TestRangeStreamPreservesTypedBackendStreamError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		code    codes.Code
		message string
	}{
		{
			name:    "MVCC corruption",
			err:     coder.MarkInvalidMVCCMetadata(errors.New("decode streamed object key")),
			code:    codes.DataLoss,
			message: "decode streamed object key",
		},
		{
			name:    "untyped backend failure",
			err:     errors.New("ordinary stream failure"),
			code:    codes.Unavailable,
			message: "ordinary stream failure",
		},
		{
			name:    "existing gRPC status",
			err:     status.Error(codes.InvalidArgument, "stream rejected"),
			code:    codes.InvalidArgument,
			message: "stream rejected",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			server, cleanup := newRangeStreamTestServerWithMetrics(t, rec)
			defer cleanup()
			shim := server.backend.(*backendShim)
			shim.backend = &encodedErrorRangeStreamBackend{Backend: shim.backend, err: test.err}

			stream := &fakeRangeStreamServer{ctx: context.Background()}
			err := server.RangeStream(&etcdserverpb.RangeRequest{
				Key: []byte("/typed-error/"), RangeEnd: []byte("/typed-error0"), Serializable: true,
			}, stream)
			require.Equal(t, test.code, status.Code(err))
			require.Equal(t, test.message, status.Convert(err).Message())
			require.Empty(t, stream.sent)
			require.Equal(t, []interface{}{int64(0), 1}, recordedRangeStreamFailureValues(rec, rangeStreamFailureBackend))
		})
	}
}

func TestRangeStreamPartialThenCompacted(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	server.SetRequestLimits(defaultMaxTxnOps, 256)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("/partial-compact/%02d", i)),
			Value: []byte(fmt.Sprintf("value-%02d-%090d", i, i)),
		})
		require.NoError(t, err)
	}

	stream := &blockingRangeStreamServer{
		fakeRangeStreamServer: fakeRangeStreamServer{ctx: ctx},
		firstSent:             make(chan struct{}),
		release:               make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- server.RangeStream(&etcdserverpb.RangeRequest{
			Key: []byte("/partial-compact/"), RangeEnd: []byte("/partial-compact0"),
		}, stream)
	}()
	<-stream.firstSent
	advance, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/zz-partial-compact-advance"), Value: []byte("value"),
	})
	require.NoError(t, err)
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{
		Revision: advance.Header.Revision, Physical: true,
	})
	require.NoError(t, err)
	close(stream.release)

	err = <-done
	requireRangeStreamError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
	require.NotEmpty(t, stream.sent[0].RangeResponse.Kvs)
	received := 0
	for _, chunk := range stream.sent {
		received += len(chunk.RangeResponse.Kvs)
	}
	require.Positive(t, received)
	require.Less(t, received, 20, "a compacted RangeStream must not finish the pinned snapshot after a partial response")
}

func requireRangeStreamError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireRangeStreamStatusError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func TestRangeStreamPeerDrainIsRetryableAtPublicBoundary(t *testing.T) {
	err := rangeStreamForwardError(context.Background(), proxyprotocol.ErrPeerStreamDrained)
	requireRangeStreamStatusError(t, err, codes.Unavailable, "etcdserver: leader changed")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = rangeStreamForwardError(canceled, proxyprotocol.ErrPeerStreamDrained)
	require.ErrorIs(t, err, proxyprotocol.ErrPeerStreamDrained)
}

// TestWatchNegativeStartRevisionCanceledInStream pins the black-magic retirement: a
// negative StartRevision used to overload Watch into a range stream (watcher.List).
// That is now the native KV.RangeStream RPC. etcd rejects the create in-band
// while leaving the multiplexed stream available for later watch requests.
func TestWatchNegativeStartRevisionCanceledInStream(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ws := &controllableWatchServer{ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1)}
	ws.recv <- &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:           []byte("/registry/pods/"),
				RangeEnd:      []byte("/registry/pods0"),
				StartRevision: -1,
			},
		},
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(ws) }()
	require.Eventually(t, func() bool { return len(ws.snapshot()) == 1 }, time.Second, 10*time.Millisecond)
	resp := ws.snapshot()[0]
	require.Equal(t, int64(-1), resp.WatchId)
	require.True(t, resp.Created)
	require.True(t, resp.Canceled)
	require.NotEmpty(t, resp.CancelReason)

	cancel()
	require.EqualError(t, <-done, status.Error(codes.Canceled, "etcdserver: watch canceled").Error())
}
