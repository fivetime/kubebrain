package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type watchControlOutcome struct {
	WatchID            int64
	Created            bool
	Canceled           bool
	CancelReason       string
	HeaderMatchesSeed  bool
	HeaderIdentitySet  bool
	HeaderClusterMatch bool
	HeaderMemberMatch  bool
	HeaderTermPositive bool
	CompactRevisionSet bool
	CompactRevisionGap int64
	Fragment           bool
	EventCount         int
	EnvelopeObserved   bool
}

func TestObserveWatchControlResponsePreservesHiddenEnvelopeFields(t *testing.T) {
	response := &etcdserverpb.WatchResponse{
		WatchId: 17, Created: true, Canceled: true, CancelReason: "reason",
		CompactRevision: 23, Fragment: true,
		Events: []*mvccpb.Event{{}}, Header: &etcdserverpb.ResponseHeader{
			ClusterId: 2, MemberId: 3, Revision: 23, RaftTerm: 4,
		},
	}
	require.Equal(t, watchControlOutcome{
		WatchID: 17, Created: true, Canceled: true, CancelReason: "reason",
		HeaderMatchesSeed: true, HeaderIdentitySet: true, HeaderClusterMatch: true,
		HeaderMemberMatch: true, HeaderTermPositive: true,
		CompactRevisionSet: true, CompactRevisionGap: 0,
		Fragment: true, EventCount: 1, EnvelopeObserved: true,
	}, observeWatchControlResponse(response, &etcdserverpb.ResponseHeader{
		ClusterId: 2, MemberId: 3, Revision: 23, RaftTerm: 4,
	}))
}

func TestWatchControlDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceControl := runWatchControlScenario(t, reference)
	wantControl := []watchControlOutcome{
		{WatchID: 42, Created: true, HeaderMatchesSeed: true},
		{WatchID: -1, Created: true, Canceled: true, CancelReason: "mvcc: duplicate watch ID provided on the WatchStream", HeaderMatchesSeed: true},
		{WatchID: 43, Created: true, HeaderMatchesSeed: true},
		{WatchID: -1, Created: true, Canceled: true, CancelReason: rpctypes.ErrCompacted.Error(), HeaderMatchesSeed: true},
		{WatchID: 45, Created: true, HeaderMatchesSeed: true},
		{WatchID: 42, Canceled: true, HeaderMatchesSeed: true},
		{WatchID: 0, Created: true, HeaderMatchesSeed: true},
		{WatchID: 0, Canceled: true, HeaderMatchesSeed: true},
		{WatchID: 1, Created: true, HeaderMatchesSeed: true},
	}
	for i := range wantControl {
		wantControl[i].EnvelopeObserved = true
		wantControl[i].HeaderIdentitySet = true
		wantControl[i].HeaderClusterMatch = true
		wantControl[i].HeaderMemberMatch = true
		wantControl[i].HeaderTermPositive = true
	}
	require.Equal(t, wantControl, referenceControl)
	require.Equal(t, referenceControl, runWatchControlScenario(t, compatEndpoint(t)))
	referenceProgress := runSingleWatchProgressScenario(t, reference, "reference")
	require.Equal(t, watchProgressOutcome{
		Created: true, CreatedCanonical: true, PutRevisionGap: 1, EventHeaderGap: 1, EventModRevisionGap: 1,
		EventValue: "1", ProgressWatchID: -1, ProgressHeaderGap: 1, ProgressEmpty: true, ProgressCanonical: true,
	}, referenceProgress)
	require.Equal(t, referenceProgress, runSingleWatchProgressScenario(t, compatEndpoint(t), "kubebrain"))
	referenceFilters := runWatchFilterEnumScenario(t, reference, "reference")
	require.Equal(t, watchFilterEnumOutcome{
		Unknown: watchFilterRunOutcome{
			Types: []int32{0, 1}, DeleteRevisionGap: 1, CreatedHeaderGap: 1,
			CreatedEnvelope:    expectedWatchFilterEnvelope(0, true, 0),
			ResponseEnvelopes:  []watchControlOutcome{expectedWatchFilterEnvelope(0, false, 2)},
			ResponseHeaderGaps: []int64{1}, EventModRevisionGaps: []int64{0, 1},
			EventMetadata: watchEventMetadataOutcome{
				Types: []mvccpb.Event_EventType{mvccpb.PUT, mvccpb.DELETE}, KeyMatches: true,
				Values: []string{"value", ""}, CreateRevisionSet: []bool{true, false},
				CreateRevisionGaps: []int64{0, 0}, ModRevisionSet: []bool{true, true},
				ModRevisionGaps: []int64{0, 1}, Versions: []int64{1, 0}, Leases: []int64{0, 0},
				PrevKVAbsent: true, KVObserved: true,
			},
		},
		Duplicate: watchFilterRunOutcome{
			Types: []int32{1}, DeleteRevisionGap: 1, CreatedHeaderGap: 1,
			CreatedEnvelope:    expectedWatchFilterEnvelope(0, true, 0),
			ResponseEnvelopes:  []watchControlOutcome{expectedWatchFilterEnvelope(0, false, 1)},
			ResponseHeaderGaps: []int64{1}, EventModRevisionGaps: []int64{1},
			EventMetadata: watchEventMetadataOutcome{
				Types: []mvccpb.Event_EventType{mvccpb.DELETE}, KeyMatches: true,
				Values: []string{""}, CreateRevisionSet: []bool{false}, CreateRevisionGaps: []int64{0},
				ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{1}, Versions: []int64{0},
				Leases: []int64{0}, PrevKVAbsent: false,
				PrevMetadata: watchPrevKVMetadataOutcome{
					Present: []bool{true}, KeyMatches: true, Values: []string{"value"},
					CreateRevisionSet: []bool{true}, CreateRevisionGaps: []int64{0},
					ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{0}, Versions: []int64{1},
					Leases: []int64{0}, Observed: true,
				},
				KVObserved: true,
			},
		},
	}, referenceFilters)
	require.Equal(t, referenceFilters, runWatchFilterEnumScenario(t, compatEndpoint(t), "kubebrain"))
	referenceInvalid := runWatchInvalidControlScenario(t, reference, "reference")
	wantInvalid := []watchControlOutcome{
		{WatchID: 0, Created: true, HeaderMatchesSeed: true, EnvelopeObserved: true},
		{WatchID: 0, Canceled: true, HeaderMatchesSeed: true, EnvelopeObserved: true},
		{WatchID: 404, Created: true, HeaderMatchesSeed: true, EnvelopeObserved: true},
	}
	for i := range wantInvalid {
		wantInvalid[i].HeaderIdentitySet = true
		wantInvalid[i].HeaderClusterMatch = true
		wantInvalid[i].HeaderMemberMatch = true
		wantInvalid[i].HeaderTermPositive = true
	}
	require.Equal(t, wantInvalid, referenceInvalid)
	require.Equal(t, referenceInvalid, runWatchInvalidControlScenario(t, compatEndpoint(t), "kubebrain"))
	referenceFragments := runWatchFragmentScenario(t, reference, "reference")
	fragmentValue := string(make([]byte, 800*1024))
	require.Equal(t, watchFragmentOutcome{
		DeleteRevisionGap:       1,
		CreatedEnvelope:         expectedFragmentControlEnvelope(true, true, 0),
		ResponseEnvelopes:       []watchControlOutcome{expectedFragmentControlEnvelope(false, false, 2)},
		FragmentHeadersAtDelete: true,
		EventModsAtDelete:       true,
		EventCounts:             []int{2},
		FragmentFlags:           []bool{false},
		PrevValueBytes:          []int{800 * 1024, 800 * 1024},
		EventMetadata: []watchEventMetadataOutcome{
			expectedFragmentEventMetadata(-1, fragmentValue),
			expectedFragmentEventMetadata(0, fragmentValue),
		},
	}, referenceFragments)
	kubebrainFragments := runWatchFragmentScenario(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, referenceFragments, kubebrainFragments)
}

type watchFilterEnumOutcome struct {
	Unknown   watchFilterRunOutcome
	Duplicate watchFilterRunOutcome
}

type watchFilterRunOutcome struct {
	Types                []int32
	DeleteRevisionGap    int64
	CreatedHeaderGap     int64
	CreatedEnvelope      watchControlOutcome
	ResponseEnvelopes    []watchControlOutcome
	ResponseHeaderGaps   []int64
	EventModRevisionGaps []int64
	EventMetadata        watchEventMetadataOutcome
}

func expectedWatchFilterEnvelope(id int64, created bool, eventCount int) watchControlOutcome {
	return watchControlOutcome{
		WatchID: id, Created: created,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EventCount: eventCount, EnvelopeObserved: true,
	}
}

func runWatchInvalidControlScenario(t *testing.T, endpoint, instance string) []watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	prefix := fmt.Sprintf("/dbaas-watch-invalid-control/%s/%d", instance, time.Now().UnixNano())
	seedKey := []byte(prefix + "/seed")
	key := []byte(prefix + "/live")
	kv := etcdserverpb.NewKVClient(conn)
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
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
	for _, request := range []*etcdserverpb.WatchRequest{
		{},
		{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{}},
		{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{}},
		{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{}},
		{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 404},
		}},
	} {
		require.NoError(t, stream.Send(request))
	}

	outcomes := make([]watchControlOutcome, 0, 3)
	for len(outcomes) < 3 {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		outcomes = append(outcomes, observeWatchControlResponse(resp, seed.Header))
	}
	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	select {
	case extra := <-pending:
		require.Failf(t, "unexpected extra watch response", "response=%v error=%v", extra.response, extra.err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case result := <-pending:
		require.Error(t, result.err)
	case <-time.After(time.Second):
		require.Fail(t, "watch receive did not stop after context cancellation")
	}
	_ = stream.CloseSend()
	return outcomes
}

func runWatchFilterEnumScenario(t *testing.T, endpoint, instance string) watchFilterEnumOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-filter-enum/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	run := func(
		suffix string,
		filters []etcdserverpb.WatchCreateRequest_FilterType,
		prevKV bool,
		wantEvents int,
	) watchFilterRunOutcome {
		key := []byte(prefix + suffix)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
		require.NoError(t, putErr)
		deleted, deleteErr := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
		require.NoError(t, deleteErr)
		require.NotNil(t, put.Header)
		require.NotNil(t, deleted.Header)

		stream, watchErr := etcdserverpb.NewWatchClient(conn).Watch(ctx)
		require.NoError(t, watchErr)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: key, StartRevision: put.Header.Revision, Filters: filters, PrevKv: prevKV,
			}},
		}))
		created, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.True(t, created.Created)
		require.NotNil(t, created.Header)

		outcome := watchFilterRunOutcome{
			Types:             make([]int32, 0, wantEvents),
			DeleteRevisionGap: deleted.Header.Revision - put.Header.Revision,
			CreatedHeaderGap:  created.Header.Revision - put.Header.Revision,
			CreatedEnvelope:   observeWatchControlResponse(created, put.Header),
		}
		events := make([]*mvccpb.Event, 0, wantEvents)
		for len(outcome.Types) < wantEvents {
			response, eventErr := stream.Recv()
			require.NoError(t, eventErr)
			require.NotNil(t, response.Header)
			outcome.ResponseEnvelopes = append(outcome.ResponseEnvelopes,
				observeWatchControlResponse(response, put.Header))
			outcome.ResponseHeaderGaps = append(outcome.ResponseHeaderGaps,
				response.Header.Revision-put.Header.Revision)
			for _, event := range response.Events {
				require.NotNil(t, event.Kv)
				events = append(events, event)
				outcome.Types = append(outcome.Types, int32(event.Type))
				outcome.EventModRevisionGaps = append(outcome.EventModRevisionGaps,
					event.Kv.ModRevision-put.Header.Revision)
			}
		}
		require.Len(t, outcome.Types, wantEvents)
		outcome.EventMetadata = observeWatchEventMetadata(events, key, put.Header.Revision)
		require.NoError(t, stream.CloseSend())
		return outcome
	}

	return watchFilterEnumOutcome{
		Unknown: run("unknown",
			[]etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_FilterType(99)}, false, 2),
		Duplicate: run("duplicate",
			[]etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_NOPUT,
				etcdserverpb.WatchCreateRequest_NOPUT,
			}, true, 1),
	}
}

type watchFragmentOutcome struct {
	CreatedHeaderGap        int64
	DeleteRevisionGap       int64
	CreatedEnvelope         watchControlOutcome
	ResponseEnvelopes       []watchControlOutcome
	FragmentHeadersAtDelete bool
	EventModsAtDelete       bool
	EventCounts             []int
	FragmentFlags           []bool
	PrevValueBytes          []int
	EventMetadata           []watchEventMetadataOutcome
}

func expectedFragmentControlEnvelope(created, headerMatchesSeed bool, eventCount int) watchControlOutcome {
	return watchControlOutcome{
		WatchID: 0, Created: created, HeaderMatchesSeed: headerMatchesSeed,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EventCount: eventCount, EnvelopeObserved: true,
	}
}

func expectedFragmentEventMetadata(prevRevisionGap int64, value string) watchEventMetadataOutcome {
	return watchEventMetadataOutcome{
		Types: []mvccpb.Event_EventType{mvccpb.DELETE}, KeyMatches: true,
		Values: []string{""}, CreateRevisionSet: []bool{false}, CreateRevisionGaps: []int64{0},
		ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{1}, Versions: []int64{0},
		Leases: []int64{0}, PrevKVAbsent: false,
		PrevMetadata: watchPrevKVMetadataOutcome{
			Present: []bool{true}, KeyMatches: true, Values: []string{value},
			CreateRevisionSet: []bool{true}, CreateRevisionGaps: []int64{prevRevisionGap},
			ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{prevRevisionGap},
			Versions: []int64{1}, Leases: []int64{0}, Observed: true,
		},
		KVObserved: true,
	}
}

type watchProgressOutcome struct {
	Created             bool
	CreatedCanonical    bool
	CreatedHeaderGap    int64
	PutRevisionGap      int64
	EventHeaderGap      int64
	EventModRevisionGap int64
	EventValue          string
	ProgressWatchID     int64
	ProgressHeaderGap   int64
	ProgressEmpty       bool
	ProgressCanonical   bool
}

func runWatchFragmentScenario(t *testing.T, endpoint, instance string) watchFragmentOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	prefix := fmt.Sprintf("/dbaas-watch-fragment/%s/%d/", instance, time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b"}
	value := make([]byte, 800*1024)
	kv := etcdserverpb.NewKVClient(conn)
	var revision int64
	var baseHeader *etcdserverpb.ResponseHeader
	for _, key := range keys {
		resp, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: value})
		require.NoError(t, putErr)
		require.NotNil(t, resp.Header)
		baseHeader = resp.Header
		revision = baseHeader.Revision
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			StartRevision: revision + 1, PrevKv: true, Fragment: true,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.NotNil(t, created.Header)

	deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)

	outcome := watchFragmentOutcome{
		CreatedHeaderGap:        created.Header.Revision - revision,
		DeleteRevisionGap:       deleted.Header.Revision - revision,
		CreatedEnvelope:         observeWatchControlResponse(created, baseHeader),
		FragmentHeadersAtDelete: true,
		EventModsAtDelete:       true,
	}
	eventIndex := 0
	for {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotEmpty(t, resp.Events)
		outcome.ResponseEnvelopes = append(outcome.ResponseEnvelopes,
			observeWatchControlResponse(resp, baseHeader))
		outcome.EventCounts = append(outcome.EventCounts, len(resp.Events))
		outcome.FragmentFlags = append(outcome.FragmentFlags, resp.Fragment)
		outcome.FragmentHeadersAtDelete = outcome.FragmentHeadersAtDelete &&
			resp.Header != nil && resp.Header.Revision == deleted.Header.Revision
		for _, event := range resp.Events {
			require.Less(t, eventIndex, len(keys))
			outcome.PrevValueBytes = append(outcome.PrevValueBytes, len(event.PrevKv.GetValue()))
			outcome.EventModsAtDelete = outcome.EventModsAtDelete &&
				event.Kv != nil && event.Kv.ModRevision == deleted.Header.Revision
			outcome.EventMetadata = append(outcome.EventMetadata,
				observeWatchEventMetadata([]*mvccpb.Event{event}, []byte(keys[eventIndex]), revision))
			eventIndex++
		}
		if !resp.Fragment {
			break
		}
	}
	require.Equal(t, len(keys), eventIndex)
	require.NoError(t, stream.CloseSend())
	return outcome
}

func runSingleWatchProgressScenario(t *testing.T, endpoint, instance string) watchProgressOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	watchKey := []byte(fmt.Sprintf("/dbaas-watch-control/progress/%s/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: watchKey})
	})
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: watchKey, WatchId: 51,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("1")})
	require.NoError(t, err)
	events, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, events.Events, 1)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progress, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "created": created.Header, "put": put.Header,
		"event": events.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchProgressOutcome{
		Created:             created.Created && created.WatchId == 51 && !created.Canceled,
		CreatedCanonical:    canonicalWatchControlResponse(created, true, 51),
		CreatedHeaderGap:    created.Header.Revision - baseRevision,
		PutRevisionGap:      put.Header.Revision - baseRevision,
		EventHeaderGap:      events.Header.Revision - baseRevision,
		EventModRevisionGap: events.Events[0].Kv.ModRevision - baseRevision,
		EventValue:          string(events.Events[0].Kv.Value),
		ProgressWatchID:     progress.WatchId,
		ProgressHeaderGap:   progress.Header.Revision - baseRevision,
		ProgressEmpty:       !progress.Created && !progress.Canceled && len(progress.Events) == 0 && progress.CancelReason == "",
		ProgressCanonical:   canonicalWatchControlResponse(progress, false, -1),
	}
}

func runWatchControlScenario(t *testing.T, endpoint string) []watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	seedKey := []byte("/dbaas-watch-control/seed")
	seed, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("1")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = etcdserverpb.NewKVClient(conn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
	})
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)

	create := func(key string, id, revision int64) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte(key), WatchId: id, StartRevision: revision,
			}},
		}))
	}
	recv := func() watchControlOutcome {
		resp, err := stream.Recv()
		require.NoError(t, err)
		return observeWatchControlResponse(resp, seed.Header)
	}

	create("/dbaas-watch-control/a", 42, 0)
	outcomes := []watchControlOutcome{recv()}
	create("/dbaas-watch-control/duplicate", 42, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999}},
	}))
	create("/dbaas-watch-control/b", 43, 0)
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/negative", 44, -1)
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/c", 45, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 42}},
	}))
	outcomes = append(outcomes, recv())

	create("/dbaas-watch-control/auto-first", 0, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 0}},
	}))
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/auto-second", 0, 0)
	outcomes = append(outcomes, recv())

	require.NoError(t, stream.CloseSend())
	return outcomes
}

func observeWatchControlResponse(resp *etcdserverpb.WatchResponse, seed *etcdserverpb.ResponseHeader) watchControlOutcome {
	header := resp.GetHeader()
	return watchControlOutcome{
		WatchID: resp.WatchId, Created: resp.Created,
		Canceled: resp.Canceled, CancelReason: resp.CancelReason,
		HeaderMatchesSeed:  header != nil && header.Revision == seed.GetRevision(),
		HeaderIdentitySet:  header.GetClusterId() != 0 && header.GetMemberId() != 0,
		HeaderClusterMatch: header.GetClusterId() == seed.GetClusterId(),
		HeaderMemberMatch:  header.GetMemberId() == seed.GetMemberId(),
		HeaderTermPositive: header.GetRaftTerm() > 0,
		CompactRevisionSet: resp.CompactRevision != 0,
		CompactRevisionGap: normalizeWatchControlRevision(resp.CompactRevision, seed.GetRevision()),
		Fragment:           resp.Fragment,
		EventCount:         len(resp.Events),
		EnvelopeObserved:   true,
	}
}

func normalizeWatchControlRevision(revision, baseRevision int64) int64 {
	if revision == 0 {
		return 0
	}
	return revision - baseRevision
}
