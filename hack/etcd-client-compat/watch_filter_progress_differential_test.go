package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type watchFilterProgressOutcome struct {
	Created             bool
	CreatedCanonical    bool
	CreatedHeaderGap    int64
	PutRevisionGap      int64
	SuppressedBeforeAck bool
	ProgressWatchID     int64
	ProgressHeaderGap   int64
	ProgressEmpty       bool
	ProgressCanonical   bool
}

type watchAllFiltersProgressOutcome struct {
	CreatedHeaderGap    int64
	CreatedCanonical    bool
	PutRevisionGap      int64
	DeleteRevisionGap   int64
	SuppressedBeforeAck bool
	ProgressWatchID     int64
	ProgressHeaderGap   int64
	ProgressCanonical   bool
}

type watchHistoricalAllFiltersOutcome struct {
	CreatedHeaderGap  int64
	CreatedCanonical  bool
	PutRevisionGap    int64
	DeleteRevisionGap int64
	ReplaySuppressed  bool
	ProgressWatchID   int64
	ProgressHeaderGap int64
	ProgressCanonical bool
}

type watchMixedFilterReplayOutcome struct {
	CreatedHeaderGap  int64
	CreatedCanonical  bool
	DeleteRevisionGap int64
	PutRevisionGap    int64
	ResponseHeaderGap int64
	ResponseWatchID   int64
	ResponseCanonical bool
	EventModGap       int64
	DeleteObserved    bool
	PrevValue         string
}

type watchFutureFilteredOutcome struct {
	CreatedCanonical          bool
	CreatedHeaderGap          int64
	InitialProgressSuppressed bool
	UnrelatedRevisionGap      int64
	FilteredRevisionGap       int64
	FilteredEventSuppressed   bool
	ProgressWatchID           int64
	ProgressHeaderGap         int64
	ProgressCanonical         bool
}

type watchTxnPartialFilterOutcome struct {
	CreatedCanonical  bool
	TxnRevisionGap    int64
	ResponseWatchID   int64
	ResponseHeaderGap int64
	EventCount        int
	DeleteObserved    bool
	EventModGap       int64
	PrevValue         string
}

type watchStreamSlowestProgressOutcome struct {
	PrefixCreatedCanonical bool
	FutureCreatedCanonical bool
	CreatedHeaderGaps      []int64
	InitialProgressBlocked bool
	WriteRevisionGaps      []int64
	PrefixEventModGaps     []int64
	FutureEventModGaps     []int64
	ProgressWatchID        int64
	ProgressHeaderGap      int64
	ProgressCanonical      bool
}

func TestWatchFilterProgressDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchFilterProgressOutcome{
		Created: true, CreatedCanonical: true, PutRevisionGap: 1, SuppressedBeforeAck: true,
		ProgressWatchID: -1, ProgressHeaderGap: 1, ProgressEmpty: true, ProgressCanonical: true,
	}
	referenceOutcome := runWatchFilterProgressScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchFilterProgressScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchAllFiltersProgressDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchAllFiltersProgressOutcome{
		CreatedCanonical: true, PutRevisionGap: 1, DeleteRevisionGap: 2, SuppressedBeforeAck: true,
		ProgressWatchID: -1, ProgressHeaderGap: 2, ProgressCanonical: true,
	}
	referenceOutcome := runWatchAllFiltersProgressScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchAllFiltersProgressScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchHistoricalAllFiltersDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchHistoricalAllFiltersOutcome{
		CreatedHeaderGap: 2, CreatedCanonical: true, PutRevisionGap: 1, DeleteRevisionGap: 2,
		ReplaySuppressed: true, ProgressWatchID: -1, ProgressHeaderGap: 2, ProgressCanonical: true,
	}
	referenceOutcome := runWatchHistoricalAllFiltersScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchHistoricalAllFiltersScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchMixedFilterReplayWatermarkDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchMixedFilterReplayOutcome{
		CreatedHeaderGap: 2, CreatedCanonical: true, DeleteRevisionGap: 1, PutRevisionGap: 2,
		ResponseHeaderGap: 2, ResponseWatchID: 904, ResponseCanonical: true,
		EventModGap: 1, DeleteObserved: true, PrevValue: "seed-a",
	}
	referenceOutcome := runWatchMixedFilterReplayScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchMixedFilterReplayScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchFutureFilteredRevisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchFutureFilteredOutcome{
		CreatedCanonical: true, InitialProgressSuppressed: true,
		UnrelatedRevisionGap: 1, FilteredRevisionGap: 2, FilteredEventSuppressed: true,
		ProgressWatchID: -1, ProgressHeaderGap: 2, ProgressCanonical: true,
	}
	referenceOutcome := runWatchFutureFilteredRevisionScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchFutureFilteredRevisionScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchTxnPartialFilterDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchTxnPartialFilterOutcome{
		CreatedCanonical: true, TxnRevisionGap: 1, ResponseWatchID: 906, ResponseHeaderGap: 1,
		EventCount: 1, DeleteObserved: true, EventModGap: 1, PrevValue: "delete-me",
	}
	referenceOutcome := runWatchTxnPartialFilterScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchTxnPartialFilterScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchStreamProgressWaitsForSlowestWatcherDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchStreamSlowestProgressOutcome{
		PrefixCreatedCanonical: true, FutureCreatedCanonical: true, CreatedHeaderGaps: []int64{0, 0},
		InitialProgressBlocked: true, WriteRevisionGaps: []int64{1, 2},
		PrefixEventModGaps: []int64{1, 2}, FutureEventModGaps: []int64{2},
		ProgressWatchID: -1, ProgressHeaderGap: 2, ProgressCanonical: true,
	}
	referenceOutcome := runWatchStreamSlowestProgressScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchStreamSlowestProgressScenario(t, compatEndpoint(t), "kubebrain"))
}

func runWatchStreamSlowestProgressScenario(t *testing.T, endpoint, instance string) watchStreamSlowestProgressOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-stream-slowest/%s/%d/", instance, time.Now().UnixNano())
	firstKey := []byte(prefix + "first")
	futureKey := []byte(prefix + "future")
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix)})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	create := func(request *etcdserverpb.WatchCreateRequest) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: request},
		}))
		return recvWatchResponse(t, stream)
	}
	prefixCreated := create(&etcdserverpb.WatchCreateRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), WatchId: 907,
	})
	futureCreated := create(&etcdserverpb.WatchCreateRequest{
		Key: futureKey, WatchId: 908, StartRevision: base.Header.Revision + 2,
	})
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	initialBlocked := false
	select {
	case early := <-pending:
		require.Failf(t, "stream progress bypassed a future watcher", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		initialBlocked = true
	}
	first, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: firstKey, Value: []byte("first")})
	require.NoError(t, err)
	future, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: futureKey, Value: []byte("future")})
	require.NoError(t, err)

	eventGaps := map[int64][]int64{907: {}, 908: {}}
	for len(eventGaps[907]) < 2 || len(eventGaps[908]) < 1 {
		var response *etcdserverpb.WatchResponse
		if pending != nil {
			result := <-pending
			require.NoError(t, result.err)
			response = result.response
			pending = nil
		} else {
			response = recvWatchResponse(t, stream)
		}
		require.Contains(t, eventGaps, response.WatchId)
		for _, event := range response.Events {
			require.NotNil(t, event.Kv)
			eventGaps[response.WatchId] = append(eventGaps[response.WatchId], event.Kv.ModRevision-base.Header.Revision)
		}
	}
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progress := recvWatchResponse(t, stream)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"base": base.Header, "prefix-created": prefixCreated.Header, "future-created": futureCreated.Header,
		"first": first.Header, "future": future.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := base.Header.Revision
	return watchStreamSlowestProgressOutcome{
		PrefixCreatedCanonical: canonicalWatchControlResponse(prefixCreated, true, 907),
		FutureCreatedCanonical: canonicalWatchControlResponse(futureCreated, true, 908),
		CreatedHeaderGaps:      []int64{prefixCreated.Header.Revision - baseRevision, futureCreated.Header.Revision - baseRevision},
		InitialProgressBlocked: initialBlocked,
		WriteRevisionGaps:      []int64{first.Header.Revision - baseRevision, future.Header.Revision - baseRevision},
		PrefixEventModGaps:     eventGaps[907], FutureEventModGaps: eventGaps[908],
		ProgressWatchID: progress.WatchId, ProgressHeaderGap: progress.Header.Revision - baseRevision,
		ProgressCanonical: canonicalWatchControlResponse(progress, false, -1),
	}
}

func runWatchTxnPartialFilterScenario(t *testing.T, endpoint, instance string) watchTxnPartialFilterOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-txn-partial-filter/%s/%d/", instance, time.Now().UnixNano())
	putKey := []byte(prefix + "put")
	deleteKey := []byte(prefix + "delete")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: deleteKey, Value: []byte("delete-me")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), WatchId: 906,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT}, PrevKv: true,
		}},
	}))
	created := recvWatchResponse(t, stream)
	txn, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: putKey, Value: []byte("filtered")}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: deleteKey}}},
	}})
	require.NoError(t, err)
	response := recvWatchResponse(t, stream)
	require.NotNil(t, seed.Header)
	require.NotNil(t, txn.Header)
	require.NotNil(t, response.Header)
	require.Len(t, response.Events, 1)
	event := response.Events[0]
	require.NotNil(t, event.Kv)
	require.NotNil(t, event.PrevKv)
	baseRevision := seed.Header.Revision
	return watchTxnPartialFilterOutcome{
		CreatedCanonical: canonicalWatchControlResponse(created, true, 906),
		TxnRevisionGap:   txn.Header.Revision - baseRevision, ResponseWatchID: response.WatchId,
		ResponseHeaderGap: response.Header.Revision - baseRevision, EventCount: len(response.Events),
		DeleteObserved: event.Type == mvccpb.DELETE && string(event.Kv.Key) == string(deleteKey),
		EventModGap:    event.Kv.ModRevision - baseRevision, PrevValue: string(event.PrevKv.Value),
	}
}

func runWatchFutureFilteredRevisionScenario(t *testing.T, endpoint, instance string) watchFutureFilteredOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-future-filtered/%s/%d/", instance, time.Now().UnixNano())
	key := []byte(prefix + "watched")
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: 905, StartRevision: base.Header.Revision + 2,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT},
		}},
	}))
	created := recvWatchResponse(t, stream)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	initialSuppressed := false
	select {
	case early := <-pending:
		require.Failf(t, "future watch emitted progress before start revision", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		initialSuppressed = true
	}
	unrelated, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "unrelated"), Value: []byte("advance")})
	require.NoError(t, err)
	filtered, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("filtered")})
	require.NoError(t, err)
	filteredSuppressed := false
	select {
	case early := <-pending:
		require.Failf(t, "future filtered event emitted a response", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		filteredSuppressed = true
	}
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progressResult := <-pending
	require.NoError(t, progressResult.err)
	progress := progressResult.response
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"base": base.Header, "created": created.Header, "unrelated": unrelated.Header,
		"filtered": filtered.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := base.Header.Revision
	return watchFutureFilteredOutcome{
		CreatedCanonical:          canonicalWatchControlResponse(created, true, 905),
		CreatedHeaderGap:          created.Header.Revision - baseRevision,
		InitialProgressSuppressed: initialSuppressed,
		UnrelatedRevisionGap:      unrelated.Header.Revision - baseRevision,
		FilteredRevisionGap:       filtered.Header.Revision - baseRevision,
		FilteredEventSuppressed:   filteredSuppressed,
		ProgressWatchID:           progress.WatchId, ProgressHeaderGap: progress.Header.Revision - baseRevision,
		ProgressCanonical: canonicalWatchControlResponse(progress, false, -1),
	}
}

func runWatchMixedFilterReplayScenario(t *testing.T, endpoint, instance string) watchMixedFilterReplayOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-mixed-filter-replay/%s/%d/", instance, time.Now().UnixNano())
	keyA, keyB := []byte(prefix+"a"), []byte(prefix+"b")
	seed, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		putRequestOp(keyA, "seed-a"), putRequestOp(keyB, "seed-b"),
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})
	deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: keyA, PrevKv: true})
	require.NoError(t, err)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyB, Value: []byte("filtered-put")})
	require.NoError(t, err)

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), WatchId: 904,
			StartRevision: deleted.Header.Revision, PrevKv: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT},
		}},
	}))
	created := recvWatchResponse(t, stream)
	response := recvWatchResponse(t, stream)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "delete": deleted.Header, "put": put.Header,
		"created": created.Header, "response": response.Header,
	} {
		require.NotNil(t, header, name)
	}
	require.Len(t, response.Events, 1)
	event := response.Events[0]
	require.NotNil(t, event.Kv)
	baseRevision := seed.Header.Revision
	return watchMixedFilterReplayOutcome{
		CreatedHeaderGap:  created.Header.Revision - baseRevision,
		CreatedCanonical:  canonicalWatchControlResponse(created, true, 904),
		DeleteRevisionGap: deleted.Header.Revision - baseRevision, PutRevisionGap: put.Header.Revision - baseRevision,
		ResponseHeaderGap: response.Header.Revision - baseRevision, ResponseWatchID: response.WatchId,
		ResponseCanonical: !response.Created && !response.Canceled && response.CompactRevision == 0 &&
			response.CancelReason == "" && !response.Fragment && len(response.Events) == 1,
		EventModGap:    event.Kv.ModRevision - baseRevision,
		DeleteObserved: event.Type == mvccpb.DELETE && string(event.Kv.Key) == string(keyA),
		PrevValue:      string(event.GetPrevKv().GetValue()),
	}
}

func runWatchHistoricalAllFiltersScenario(t *testing.T, endpoint, instance string) watchHistoricalAllFiltersOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-watch-historical-all-filters/%s/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("historical")})
	require.NoError(t, err)
	deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
	require.NoError(t, err)

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: 903, StartRevision: put.Header.Revision, PrevKv: true, Fragment: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_NOPUT,
				etcdserverpb.WatchCreateRequest_NODELETE,
			},
		}},
	}))
	created := recvWatchResponse(t, stream)

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	replaySuppressed := false
	select {
	case early := <-pending:
		require.Failf(t, "fully filtered historical replay emitted a response", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		replaySuppressed = true
	}
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progressResult := <-pending
	require.NoError(t, progressResult.err)
	progress := progressResult.response
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "put": put.Header, "delete": deleted.Header,
		"created": created.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchHistoricalAllFiltersOutcome{
		CreatedHeaderGap: created.Header.Revision - baseRevision,
		CreatedCanonical: canonicalWatchControlResponse(created, true, 903),
		PutRevisionGap:   put.Header.Revision - baseRevision, DeleteRevisionGap: deleted.Header.Revision - baseRevision,
		ReplaySuppressed: replaySuppressed, ProgressWatchID: progress.WatchId,
		ProgressHeaderGap: progress.Header.Revision - baseRevision,
		ProgressCanonical: canonicalWatchControlResponse(progress, false, -1),
	}
}

func runWatchAllFiltersProgressScenario(t *testing.T, endpoint, instance string) watchAllFiltersProgressOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-watch-all-filters-progress/%s/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: 902, PrevKv: true, Fragment: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_NOPUT,
				etcdserverpb.WatchCreateRequest_NODELETE,
			},
		}},
	}))
	created := recvWatchResponse(t, stream)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("filtered")})
	require.NoError(t, err)
	deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
	require.NoError(t, err)

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	suppressed := false
	select {
	case early := <-pending:
		require.Failf(t, "combined filters emitted a response", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		suppressed = true
	}
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progressResult := <-pending
	require.NoError(t, progressResult.err)
	progress := progressResult.response
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "created": created.Header, "put": put.Header,
		"delete": deleted.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchAllFiltersProgressOutcome{
		CreatedHeaderGap: created.Header.Revision - baseRevision,
		CreatedCanonical: canonicalWatchControlResponse(created, true, 902),
		PutRevisionGap:   put.Header.Revision - baseRevision, DeleteRevisionGap: deleted.Header.Revision - baseRevision,
		SuppressedBeforeAck: suppressed, ProgressWatchID: progress.WatchId,
		ProgressHeaderGap: progress.Header.Revision - baseRevision,
		ProgressCanonical: canonicalWatchControlResponse(progress, false, -1),
	}
}

func runWatchFilterProgressScenario(t *testing.T, endpoint, instance string) watchFilterProgressOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-watch-filter-progress/%s/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: 901, Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT},
		}},
	}))
	created := recvWatchResponse(t, stream)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("filtered")})
	require.NoError(t, err)

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	suppressed := false
	select {
	case early := <-pending:
		require.Failf(t, "filtered PUT emitted a response", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		suppressed = true
	}
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progressResult := <-pending
	require.NoError(t, progressResult.err)
	progress := progressResult.response
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "created": created.Header, "put": put.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchFilterProgressOutcome{
		Created:             created.Created && !created.Canceled && created.WatchId == 901,
		CreatedCanonical:    canonicalWatchControlResponse(created, true, 901),
		CreatedHeaderGap:    created.Header.Revision - baseRevision,
		PutRevisionGap:      put.Header.Revision - baseRevision,
		SuppressedBeforeAck: suppressed,
		ProgressWatchID:     progress.WatchId,
		ProgressHeaderGap:   progress.Header.Revision - baseRevision,
		ProgressEmpty:       !progress.Created && !progress.Canceled && len(progress.Events) == 0,
		ProgressCanonical:   canonicalWatchControlResponse(progress, false, -1),
	}
}

func canonicalWatchControlResponse(response *etcdserverpb.WatchResponse, created bool, watchID int64) bool {
	return response != nil && response.Created == created && !response.Canceled &&
		response.WatchId == watchID && response.CompactRevision == 0 && response.CancelReason == "" &&
		!response.Fragment && len(response.Events) == 0
}

func TestCanonicalWatchControlResponseRejectsHiddenPayload(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *etcdserverpb.WatchResponse
	}{
		{name: "compact revision", response: &etcdserverpb.WatchResponse{WatchId: -1, CompactRevision: 7}},
		{name: "cancel reason", response: &etcdserverpb.WatchResponse{WatchId: -1, CancelReason: "unexpected"}},
		{name: "fragment", response: &etcdserverpb.WatchResponse{WatchId: -1, Fragment: true}},
		{name: "events", response: &etcdserverpb.WatchResponse{WatchId: -1, Events: []*mvccpb.Event{{}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.False(t, canonicalWatchControlResponse(test.response, false, -1))
		})
	}
	require.True(t, canonicalWatchControlResponse(&etcdserverpb.WatchResponse{WatchId: -1}, false, -1))
	require.True(t, canonicalWatchControlResponse(&etcdserverpb.WatchResponse{WatchId: 901, Created: true}, true, 901))
}
