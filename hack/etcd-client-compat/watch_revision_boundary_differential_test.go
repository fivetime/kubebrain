package compat

import (
	"bytes"
	"context"
	"fmt"
	"math"
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

type watchRevisionBoundaryOutcome struct {
	Name                     string
	CreatedControl           watchControlOutcome
	EventEnvelope            watchControlOutcome
	EventTypes               []mvccpb.Event_EventType
	EventKeyMatches          bool
	EventValues              []string
	EventCreateAtWrite       bool
	EventAtWriteRevision     bool
	EventVersions            []int64
	EventLeases              []int64
	EventPrevKVAbsent        bool
	EventKVObserved          bool
	ProgressSuppressedFuture bool
	ProgressControl          watchControlOutcome
	CancelControl            watchControlOutcome
}

func TestNormalizeWatchRevisionOutcomeObservesEventMetadata(t *testing.T) {
	header := &etcdserverpb.ResponseHeader{ClusterId: 1, MemberId: 2, Revision: 7, RaftTerm: 3}
	created := &etcdserverpb.WatchResponse{Header: header, WatchId: 9, Created: true}
	event := &etcdserverpb.WatchResponse{
		Header: header, WatchId: 9,
		Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{
			Key: []byte("key"), Value: []byte("value"), CreateRevision: 7,
			ModRevision: 7, Version: 1,
		}}},
	}
	outcome := normalizeWatchRevisionOutcome("metadata", created, event, []byte("key"), header, header)
	require.Equal(t, []mvccpb.Event_EventType{mvccpb.PUT}, outcome.EventTypes)
	require.True(t, outcome.EventKeyMatches)
	require.True(t, outcome.EventCreateAtWrite)
	require.Equal(t, []int64{1}, outcome.EventVersions)
	require.Equal(t, []int64{0}, outcome.EventLeases)
	require.True(t, outcome.EventPrevKVAbsent)
	require.True(t, outcome.EventKVObserved)

	event.Events = []*mvccpb.Event{nil}
	nilOutcome := normalizeWatchRevisionOutcome("nil-event", created, event, []byte("key"), header, header)
	require.False(t, nilOutcome.EventKeyMatches)
	require.False(t, nilOutcome.EventCreateAtWrite)
	require.False(t, nilOutcome.EventAtWriteRevision)
	require.False(t, nilOutcome.EventKVObserved)
}

func TestWatchRevisionBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []watchRevisionBoundaryOutcome{
		{
			Name:                 "latest-zero",
			CreatedControl:       expectedRevisionCreatedControl(301),
			EventEnvelope:        expectedRevisionEventEnvelope(301),
			EventTypes:           []mvccpb.Event_EventType{mvccpb.PUT},
			EventKeyMatches:      true,
			EventValues:          []string{"after-create"},
			EventCreateAtWrite:   true,
			EventAtWriteRevision: true,
			EventVersions:        []int64{1},
			EventLeases:          []int64{0},
			EventPrevKVAbsent:    true,
			EventKVObserved:      true,
		},
		{
			Name:                 "historical-current",
			CreatedControl:       expectedRevisionCreatedControl(302),
			EventEnvelope:        expectedRevisionEventEnvelope(302),
			EventTypes:           []mvccpb.Event_EventType{mvccpb.PUT},
			EventKeyMatches:      true,
			EventValues:          []string{"seed"},
			EventCreateAtWrite:   true,
			EventAtWriteRevision: true,
			EventVersions:        []int64{1},
			EventLeases:          []int64{0},
			EventPrevKVAbsent:    true,
			EventKVObserved:      true,
		},
		{
			Name:                     "future-next",
			CreatedControl:           expectedRevisionCreatedControl(303),
			EventEnvelope:            expectedRevisionEventEnvelope(303),
			EventTypes:               []mvccpb.Event_EventType{mvccpb.PUT},
			EventKeyMatches:          true,
			EventValues:              []string{"future"},
			EventCreateAtWrite:       true,
			EventAtWriteRevision:     true,
			EventVersions:            []int64{1},
			EventLeases:              []int64{0},
			EventPrevKVAbsent:        true,
			EventKVObserved:          true,
			ProgressSuppressedFuture: true,
			ProgressControl:          expectedRevisionProgressControl(),
		},
		{
			Name:           "maximum",
			CreatedControl: expectedRevisionCreatedControl(304),
			EventValues:    []string{},
			CancelControl:  expectedRevisionCancelControl(304),
		},
	}
	referenceOutcomes := runWatchRevisionBoundaryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runWatchRevisionBoundaryScenario(t, compatEndpoint(t), "kubebrain"))
}

func expectedRevisionCreatedControl(id int64) watchControlOutcome {
	return watchControlOutcome{
		WatchID: id, Created: true, HeaderMatchesSeed: true,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EnvelopeObserved: true,
	}
}

func expectedRevisionProgressControl() watchControlOutcome {
	return watchControlOutcome{
		WatchID: -1, HeaderMatchesSeed: true,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EnvelopeObserved: true,
	}
}

func expectedRevisionEventEnvelope(id int64) watchControlOutcome {
	return watchControlOutcome{
		WatchID: id, HeaderMatchesSeed: true,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EventCount: 1, EnvelopeObserved: true,
	}
}

func expectedRevisionCancelControl(id int64) watchControlOutcome {
	return watchControlOutcome{
		WatchID: id, Canceled: true, HeaderMatchesSeed: true,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EnvelopeObserved: true,
	}
}

func runWatchRevisionBoundaryScenario(
	t *testing.T,
	endpoint string,
	instance string,
) []watchRevisionBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	watch := etcdserverpb.NewWatchClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-revision/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	var outcomes []watchRevisionBoundaryOutcome
	t.Run(instance+"/latest-zero", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "latest")
		base, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 301, 0)
		created := recvWatchResponse(t, stream)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-create")})
		require.NoError(t, putErr)
		event := recvWatchResponse(t, stream)
		outcomes = append(outcomes, normalizeWatchRevisionOutcome("latest-zero", created, event, key, base.Header, put.Header))
		require.NoError(t, stream.CloseSend())
	})

	t.Run(instance+"/historical-current", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "historical")
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
		require.NoError(t, putErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 302, put.Header.Revision)
		created := recvWatchResponse(t, stream)
		event := recvWatchResponse(t, stream)
		outcomes = append(outcomes, normalizeWatchRevisionOutcome(
			"historical-current", created, event, key, put.Header, put.Header,
		))
		require.NoError(t, stream.CloseSend())
	})

	t.Run(instance+"/future-next", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "future")
		base, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		startRevision := base.Header.Revision + 2
		sendWatchCreate(t, stream, key, 303, startRevision)
		created := recvWatchResponse(t, stream)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
				ProgressRequest: &etcdserverpb.WatchProgressRequest{},
			},
		}))
		type receiveResult struct {
			response *etcdserverpb.WatchResponse
			err      error
		}
		pending := make(chan receiveResult, 1)
		go func() {
			response, receiveErr := stream.Recv()
			pending <- receiveResult{response: response, err: receiveErr}
		}()
		var received receiveResult
		progressSuppressed := false
		receivedEarly := false
		select {
		case received = <-pending:
			receivedEarly = true
		case <-time.After(150 * time.Millisecond):
			progressSuppressed = true
		}
		_, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "future-unrelated"), Value: []byte("advance"),
		})
		require.NoError(t, putErr)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("future")})
		require.NoError(t, putErr)
		if !receivedEarly {
			received = <-pending
		}
		require.NoError(t, received.err)
		event := received.response
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
				ProgressRequest: &etcdserverpb.WatchProgressRequest{},
			},
		}))
		progress := recvWatchResponse(t, stream)
		outcome := normalizeWatchRevisionOutcome(
			"future-next", created, event, key, base.Header, put.Header,
		)
		outcome.ProgressSuppressedFuture = progressSuppressed
		outcome.ProgressControl = observeWatchControlResponse(progress, put.Header)
		outcomes = append(outcomes, outcome)
		require.NoError(t, stream.CloseSend())
	})

	t.Run(instance+"/maximum", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "maximum")
		base, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 304, math.MaxInt64)
		created := recvWatchResponse(t, stream)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("below-maximum")})
		require.NoError(t, putErr)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 304},
			},
		}))
		canceled := recvWatchResponse(t, stream)
		outcomes = append(outcomes, watchRevisionBoundaryOutcome{
			Name:           "maximum",
			CreatedControl: observeWatchControlResponse(created, base.Header),
			EventValues:    []string{},
			CancelControl:  observeWatchControlResponse(canceled, put.Header),
		})
		require.NoError(t, stream.CloseSend())
	})
	return outcomes
}

func sendWatchCreate(
	t *testing.T,
	stream etcdserverpb.Watch_WatchClient,
	key []byte,
	id int64,
	revision int64,
) {
	t.Helper()
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: key, WatchId: id, StartRevision: revision,
			},
		},
	}))
}

func recvWatchResponse(t *testing.T, stream etcdserverpb.Watch_WatchClient) *etcdserverpb.WatchResponse {
	t.Helper()
	resp, err := stream.Recv()
	require.NoError(t, err)
	return resp
}

func normalizeWatchRevisionOutcome(
	name string,
	created *etcdserverpb.WatchResponse,
	event *etcdserverpb.WatchResponse,
	expectedKey []byte,
	baseHeader *etcdserverpb.ResponseHeader,
	writeHeader *etcdserverpb.ResponseHeader,
) watchRevisionBoundaryOutcome {
	values := make([]string, 0, len(event.Events))
	types := make([]mvccpb.Event_EventType, 0, len(event.Events))
	versions := make([]int64, 0, len(event.Events))
	leases := make([]int64, 0, len(event.Events))
	eventKeyMatches := len(event.Events) > 0
	eventCreateAtWrite := len(event.Events) > 0
	eventAtWriteRevision := len(event.Events) > 0
	eventPrevKVAbsent := len(event.Events) > 0
	eventKVObserved := len(event.Events) > 0
	for _, item := range event.Events {
		types = append(types, item.GetType())
		eventPrevKVAbsent = eventPrevKVAbsent && item.GetPrevKv() == nil
		kv := item.GetKv()
		if kv == nil {
			eventKeyMatches = false
			eventCreateAtWrite = false
			eventAtWriteRevision = false
			eventKVObserved = false
			continue
		}
		values = append(values, string(kv.Value))
		versions = append(versions, kv.Version)
		leases = append(leases, kv.Lease)
		eventKeyMatches = eventKeyMatches && bytes.Equal(kv.Key, expectedKey)
		eventCreateAtWrite = eventCreateAtWrite && kv.CreateRevision == writeHeader.GetRevision()
		eventAtWriteRevision = eventAtWriteRevision && kv.ModRevision == writeHeader.GetRevision()
	}
	return watchRevisionBoundaryOutcome{
		Name:                 name,
		CreatedControl:       observeWatchControlResponse(created, baseHeader),
		EventEnvelope:        observeWatchControlResponse(event, writeHeader),
		EventTypes:           types,
		EventKeyMatches:      eventKeyMatches,
		EventValues:          values,
		EventCreateAtWrite:   eventCreateAtWrite,
		EventAtWriteRevision: eventAtWriteRevision,
		EventVersions:        versions,
		EventLeases:          leases,
		EventPrevKVAbsent:    eventPrevKVAbsent,
		EventKVObserved:      eventKVObserved,
	}
}
