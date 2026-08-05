package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type watchEventMetadataOutcome struct {
	Types              []mvccpb.Event_EventType
	KeyMatches         bool
	Values             []string
	CreateRevisionSet  []bool
	CreateRevisionGaps []int64
	ModRevisionSet     []bool
	ModRevisionGaps    []int64
	Versions           []int64
	Leases             []int64
	PrevKVAbsent       bool
	PrevMetadata       watchPrevKVMetadataOutcome
	KVObserved         bool
}

type watchPrevKVMetadataOutcome struct {
	Present            []bool
	KeyMatches         bool
	Values             []string
	CreateRevisionSet  []bool
	CreateRevisionGaps []int64
	ModRevisionSet     []bool
	ModRevisionGaps    []int64
	Versions           []int64
	Leases             []int64
	Observed           bool
}

type watchCompactedRevisionOutcome struct {
	FirstPutGap          int64
	SecondPutGap         int64
	CompactionHeaderGap  int64
	PostCompactPutGap    int64
	OldCreatedHeaderGap  int64
	OldCreatedEnvelope   watchControlOutcome
	CancelHeaderZero     bool
	CanceledEnvelope     watchControlOutcome
	NextCreatedHeaderGap int64
	NextCreatedEnvelope  watchControlOutcome
	FinalPutGap          int64
	EventHeaderGap       int64
	EventEnvelope        watchControlOutcome
	EventMetadata        watchEventMetadataOutcome
}

func TestWatchCompactedRevisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchCompactedRevisionOutcome{
		FirstPutGap: 1, SecondPutGap: 2, CompactionHeaderGap: 2, PostCompactPutGap: 3,
		OldCreatedHeaderGap:  3,
		OldCreatedEnvelope:   expectedCompactedWatchEnvelope(707, true, false, false, 0),
		CancelHeaderZero:     true,
		CanceledEnvelope:     expectedCompactedWatchEnvelope(707, false, true, true, 2),
		NextCreatedHeaderGap: 3,
		NextCreatedEnvelope:  expectedCompactedWatchEnvelope(708, true, false, false, 0),
		FinalPutGap:          4, EventHeaderGap: 4,
		EventEnvelope: expectedCompactedWatchEventEnvelope(708),
		EventMetadata: watchEventMetadataOutcome{
			Types: []mvccpb.Event_EventType{mvccpb.PUT}, KeyMatches: true,
			Values: []string{"v4"}, CreateRevisionSet: []bool{true}, CreateRevisionGaps: []int64{1},
			ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{4}, Versions: []int64{4}, Leases: []int64{0},
			PrevKVAbsent: true, KVObserved: true,
		},
	}
	referenceOutcome := runWatchCompactedRevisionScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchCompactedRevisionScenario(t, compatEndpoint(t), "kubebrain"))
}

func observeWatchEventMetadata(
	events []*mvccpb.Event,
	expectedKey []byte,
	baseRevision int64,
) watchEventMetadataOutcome {
	outcome := watchEventMetadataOutcome{
		Types:              make([]mvccpb.Event_EventType, 0, len(events)),
		Values:             make([]string, 0, len(events)),
		CreateRevisionSet:  make([]bool, 0, len(events)),
		CreateRevisionGaps: make([]int64, 0, len(events)),
		ModRevisionSet:     make([]bool, 0, len(events)),
		ModRevisionGaps:    make([]int64, 0, len(events)),
		Versions:           make([]int64, 0, len(events)),
		Leases:             make([]int64, 0, len(events)),
		KeyMatches:         len(events) > 0, PrevKVAbsent: len(events) > 0, KVObserved: len(events) > 0,
	}
	for _, event := range events {
		outcome.Types = append(outcome.Types, event.GetType())
		outcome.PrevKVAbsent = outcome.PrevKVAbsent && event.GetPrevKv() == nil
		kv := event.GetKv()
		if kv == nil {
			outcome.KeyMatches = false
			outcome.KVObserved = false
			continue
		}
		outcome.KeyMatches = outcome.KeyMatches && bytes.Equal(kv.Key, expectedKey)
		outcome.Values = append(outcome.Values, string(kv.Value))
		outcome.CreateRevisionSet = append(outcome.CreateRevisionSet, kv.CreateRevision != 0)
		outcome.CreateRevisionGaps = append(outcome.CreateRevisionGaps,
			revisionGapPreservingZero(kv.CreateRevision, baseRevision))
		outcome.ModRevisionSet = append(outcome.ModRevisionSet, kv.ModRevision != 0)
		outcome.ModRevisionGaps = append(outcome.ModRevisionGaps,
			revisionGapPreservingZero(kv.ModRevision, baseRevision))
		outcome.Versions = append(outcome.Versions, kv.Version)
		outcome.Leases = append(outcome.Leases, kv.Lease)
	}
	outcome.PrevMetadata = observeWatchPrevKVMetadata(events, expectedKey, baseRevision)
	return outcome
}

func observeWatchPrevKVMetadata(
	events []*mvccpb.Event,
	expectedKey []byte,
	baseRevision int64,
) watchPrevKVMetadataOutcome {
	anyPresent := false
	for _, event := range events {
		if event.GetPrevKv() != nil {
			anyPresent = true
			break
		}
	}
	if !anyPresent {
		return watchPrevKVMetadataOutcome{}
	}
	outcome := watchPrevKVMetadataOutcome{
		Present: make([]bool, len(events)), KeyMatches: true, Observed: true,
		Values:            make([]string, 0, len(events)),
		CreateRevisionSet: make([]bool, 0, len(events)), CreateRevisionGaps: make([]int64, 0, len(events)),
		ModRevisionSet: make([]bool, 0, len(events)), ModRevisionGaps: make([]int64, 0, len(events)),
		Versions: make([]int64, 0, len(events)), Leases: make([]int64, 0, len(events)),
	}
	for index, event := range events {
		prevKV := event.GetPrevKv()
		if prevKV == nil {
			continue
		}
		outcome.Present[index] = true
		outcome.KeyMatches = outcome.KeyMatches && bytes.Equal(prevKV.Key, expectedKey)
		outcome.Values = append(outcome.Values, string(prevKV.Value))
		outcome.CreateRevisionSet = append(outcome.CreateRevisionSet, prevKV.CreateRevision != 0)
		outcome.CreateRevisionGaps = append(outcome.CreateRevisionGaps,
			revisionGapPreservingZero(prevKV.CreateRevision, baseRevision))
		outcome.ModRevisionSet = append(outcome.ModRevisionSet, prevKV.ModRevision != 0)
		outcome.ModRevisionGaps = append(outcome.ModRevisionGaps,
			revisionGapPreservingZero(prevKV.ModRevision, baseRevision))
		outcome.Versions = append(outcome.Versions, prevKV.Version)
		outcome.Leases = append(outcome.Leases, prevKV.Lease)
	}
	return outcome
}

func revisionGapPreservingZero(revision, baseRevision int64) int64 {
	if revision == 0 {
		return 0
	}
	return revision - baseRevision
}

func TestObserveWatchEventMetadataHandlesNilPayload(t *testing.T) {
	event := &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{
		Key: []byte("key"), Value: []byte("v4"), CreateRevision: 11,
		ModRevision: 14, Version: 4,
	}}
	require.Equal(t, watchEventMetadataOutcome{
		Types: []mvccpb.Event_EventType{mvccpb.PUT}, KeyMatches: true,
		Values: []string{"v4"}, CreateRevisionSet: []bool{true}, CreateRevisionGaps: []int64{1},
		ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{4}, Versions: []int64{4}, Leases: []int64{0},
		PrevKVAbsent: true, KVObserved: true,
	}, observeWatchEventMetadata([]*mvccpb.Event{event}, []byte("key"), 10))

	nilOutcome := observeWatchEventMetadata([]*mvccpb.Event{nil}, []byte("key"), 10)
	require.False(t, nilOutcome.KeyMatches)
	require.False(t, nilOutcome.KVObserved)

	require.Equal(t, watchEventMetadataOutcome{
		Types: []mvccpb.Event_EventType{mvccpb.DELETE}, KeyMatches: true,
		Values: []string{""}, CreateRevisionSet: []bool{false}, CreateRevisionGaps: []int64{0},
		ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{1}, Versions: []int64{0}, Leases: []int64{0},
		PrevKVAbsent: true, KVObserved: true,
	}, observeWatchEventMetadata([]*mvccpb.Event{{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 11},
	}}, []byte("key"), 10))

	require.Equal(t, watchPrevKVMetadataOutcome{
		Present: []bool{false, true}, KeyMatches: true, Values: []string{"value"},
		CreateRevisionSet: []bool{true}, CreateRevisionGaps: []int64{0},
		ModRevisionSet: []bool{true}, ModRevisionGaps: []int64{0}, Versions: []int64{1},
		Leases: []int64{0}, Observed: true,
	}, observeWatchEventMetadata([]*mvccpb.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 10}},
		{
			Type: mvccpb.DELETE,
			Kv:   &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 11},
			PrevKv: &mvccpb.KeyValue{
				Key: []byte("key"), Value: []byte("value"), CreateRevision: 10,
				ModRevision: 10, Version: 1,
			},
		},
	}, []byte("key"), 10).PrevMetadata)
}

func expectedCompactedWatchEventEnvelope(id int64) watchControlOutcome {
	return watchControlOutcome{
		WatchID:           id,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, EventCount: 1, EnvelopeObserved: true,
	}
}

func expectedCompactedWatchEnvelope(
	id int64,
	created bool,
	canceled bool,
	compactRevisionSet bool,
	compactRevisionGap int64,
) watchControlOutcome {
	return watchControlOutcome{
		WatchID: id, Created: created, Canceled: canceled,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, CompactRevisionSet: compactRevisionSet,
		CompactRevisionGap: compactRevisionGap, EnvelopeObserved: true,
	}
}

func runWatchCompactedRevisionScenario(t *testing.T, endpoint, instance string) watchCompactedRevisionOutcome {
	t.Helper()
	endpoint = grpcTarget(endpoint)
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-compacted/%s/%d/", instance, time.Now().UnixNano())
	seedKey := []byte(prefix + "seed")
	key := []byte(prefix + "key")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})
	put1, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	put2, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	compacted, err := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put2.Header.Revision})
	require.NoError(t, err)
	put3, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v3")})
	require.NoError(t, err)

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	sendWatchCreate(t, stream, key, 707, put1.Header.Revision)
	oldCreated := recvWatchResponse(t, stream)
	canceled := recvWatchResponse(t, stream)
	sendWatchCreate(t, stream, key, 708, put3.Header.Revision+1)
	created := recvWatchResponse(t, stream)
	put4, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v4")})
	require.NoError(t, err)
	event := recvWatchResponse(t, stream)
	require.Len(t, event.Events, 1)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "put1": put1.Header, "put2": put2.Header,
		"compact": compacted.Header, "put3": put3.Header, "old-created": oldCreated.Header,
		"canceled": canceled.Header,
		"created":  created.Header, "put4": put4.Header, "event": event.Header,
	} {
		require.NotNil(t, header, name)
	}
	require.NotNil(t, event.Events[0].Kv)
	baseRevision := seed.Header.Revision
	return watchCompactedRevisionOutcome{
		FirstPutGap:          put1.Header.Revision - baseRevision,
		SecondPutGap:         put2.Header.Revision - baseRevision,
		CompactionHeaderGap:  compacted.Header.Revision - baseRevision,
		PostCompactPutGap:    put3.Header.Revision - baseRevision,
		OldCreatedHeaderGap:  oldCreated.Header.Revision - baseRevision,
		OldCreatedEnvelope:   observeWatchControlResponse(oldCreated, seed.Header),
		CancelHeaderZero:     canceled.Header.Revision == 0,
		CanceledEnvelope:     observeWatchControlResponse(canceled, seed.Header),
		NextCreatedHeaderGap: created.Header.Revision - baseRevision,
		NextCreatedEnvelope:  observeWatchControlResponse(created, seed.Header),
		FinalPutGap:          put4.Header.Revision - baseRevision,
		EventHeaderGap:       event.Header.Revision - baseRevision,
		EventEnvelope:        observeWatchControlResponse(event, seed.Header),
		EventMetadata:        observeWatchEventMetadata(event.Events, key, baseRevision),
	}
}
