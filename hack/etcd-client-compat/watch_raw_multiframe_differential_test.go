package compat

import (
	"bytes"
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
	"google.golang.org/protobuf/proto"
)

type watchRawMultiframeOutcome struct {
	CreatedEnvelope     watchControlOutcome
	ResponseEnvelopes   []watchControlOutcome
	ResponseHeaderGaps  []int64
	ResponseBelowTwoMiB []bool
	EventMetadata       []watchRawMultiframeEventOutcome
	TotalEvents         int
}

type watchRawMultiframeEventOutcome struct {
	Type              mvccpb.Event_EventType
	KeyMatches        bool
	ValueMatches      bool
	CreateRevisionGap int64
	ModRevisionGap    int64
	Version           int64
	Lease             int64
	PrevKVAbsent      bool
	PrevKeyMatches    bool
	PrevValueMatches  bool
	PrevCreateGap     int64
	PrevModGap        int64
	PrevVersion       int64
	PrevLease         int64
	KVObserved        bool
}

func TestWatchRawMultiframeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := expectedWatchRawMultiframeOutcome()
	referenceOutcome := runWatchRawMultiframeScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome,
		runWatchRawMultiframeScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchRawMultiframePrevKVDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := expectedWatchRawMultiframePrevKVOutcome()
	referenceOutcome := runWatchRawMultiframePrevKVScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome,
		runWatchRawMultiframePrevKVScenario(t, compatEndpoint(t), "kubebrain"))
}

func expectedWatchRawMultiframeOutcome() watchRawMultiframeOutcome {
	outcome := watchRawMultiframeOutcome{
		CreatedEnvelope: expectedFragmentControlEnvelope(true, true, 0),
		TotalEvents:     3,
	}
	for revisionGap := int64(1); revisionGap <= 3; revisionGap++ {
		envelope := expectedFragmentControlEnvelope(false, true, 1)
		envelope.Fragment = revisionGap < 3
		outcome.ResponseEnvelopes = append(outcome.ResponseEnvelopes, envelope)
		outcome.ResponseHeaderGaps = append(outcome.ResponseHeaderGaps, 3)
		outcome.ResponseBelowTwoMiB = append(outcome.ResponseBelowTwoMiB, true)
		outcome.EventMetadata = append(outcome.EventMetadata, watchRawMultiframeEventOutcome{
			Type: mvccpb.PUT, KeyMatches: true, ValueMatches: true,
			CreateRevisionGap: revisionGap, ModRevisionGap: revisionGap,
			Version: 1, Lease: 0, PrevKVAbsent: true, KVObserved: true,
		})
	}
	return outcome
}

func expectedWatchRawMultiframePrevKVOutcome() watchRawMultiframeOutcome {
	outcome := watchRawMultiframeOutcome{
		CreatedEnvelope: expectedFragmentControlEnvelope(true, true, 0),
		TotalEvents:     3,
	}
	for index := int64(1); index <= 3; index++ {
		envelope := expectedFragmentControlEnvelope(false, true, 1)
		envelope.Fragment = index < 3
		outcome.ResponseEnvelopes = append(outcome.ResponseEnvelopes, envelope)
		outcome.ResponseHeaderGaps = append(outcome.ResponseHeaderGaps, 6)
		outcome.ResponseBelowTwoMiB = append(outcome.ResponseBelowTwoMiB, false)
		outcome.EventMetadata = append(outcome.EventMetadata, watchRawMultiframeEventOutcome{
			Type: mvccpb.PUT, KeyMatches: true, ValueMatches: true,
			CreateRevisionGap: index, ModRevisionGap: index + 3,
			Version: 2, Lease: 0, PrevKVAbsent: false,
			PrevKeyMatches: true, PrevValueMatches: true,
			PrevCreateGap: index, PrevModGap: index, PrevVersion: 1, PrevLease: 0,
			KVObserved: true,
		})
	}
	return outcome
}

func runWatchRawMultiframeScenario(t *testing.T, endpoint, instance string) watchRawMultiframeOutcome {
	return runWatchRawMultiframeScenarioWithPrevKV(t, endpoint, instance, false)
}

func runWatchRawMultiframeScenarioWithPrevKV(
	t *testing.T,
	endpoint, instance string,
	withPrevKV bool,
) watchRawMultiframeOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	prefixRoot := "/dbaas-watch-raw-multiframe/"
	if withPrevKV {
		prefixRoot = "/dbaas-watch-raw-multiframe-prevkv/"
	}
	prefix := fmt.Sprintf("%s%s/%d/", prefixRoot, instance, time.Now().UnixNano())
	kv := etcdserverpb.NewKVClient(conn)
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
	})
	require.NoError(t, err)
	require.NotNil(t, base.Header)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	const eventCount = 3
	value := []byte(strings.Repeat("x", 1024*1024))
	oldValue := []byte(strings.Repeat("o", 1024*1024))
	keys := make([][]byte, 0, eventCount)
	for index := 0; index < eventCount; index++ {
		keys = append(keys, []byte(fmt.Sprintf("%s%d", prefix, index)))
	}
	eventStartRevision := base.Header.Revision + 1
	var finalHeader *etcdserverpb.ResponseHeader
	if withPrevKV {
		for _, key := range keys {
			put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: oldValue})
			require.NoError(t, putErr)
			require.NotNil(t, put.Header)
			finalHeader = put.Header
		}
		eventStartRevision = finalHeader.Revision + 1
	}
	for _, key := range keys {
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value})
		require.NoError(t, putErr)
		require.NotNil(t, put.Header)
		finalHeader = put.Header
	}

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			StartRevision: eventStartRevision, PrevKv: withPrevKV, Fragment: true,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.NotNil(t, created.Header)

	outcome := watchRawMultiframeOutcome{
		CreatedEnvelope: observeWatchControlResponse(created, finalHeader),
	}
	eventIndex := 0
	for {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotEmpty(t, response.Events)
		outcome.ResponseEnvelopes = append(outcome.ResponseEnvelopes,
			observeWatchControlResponse(response, finalHeader))
		outcome.ResponseHeaderGaps = append(outcome.ResponseHeaderGaps,
			response.Header.GetRevision()-base.Header.Revision)
		outcome.ResponseBelowTwoMiB = append(outcome.ResponseBelowTwoMiB,
			proto.Size(response) < 2*1024*1024)
		for _, event := range response.Events {
			require.Less(t, eventIndex, len(keys))
			metadata := watchRawMultiframeEventOutcome{
				Type: event.GetType(), PrevKVAbsent: event.GetPrevKv() == nil, KVObserved: event.GetKv() != nil,
			}
			if event.Kv != nil {
				metadata.KeyMatches = bytes.Equal(event.Kv.Key, keys[eventIndex])
				metadata.ValueMatches = bytes.Equal(event.Kv.Value, value)
				metadata.CreateRevisionGap = event.Kv.CreateRevision - base.Header.Revision
				metadata.ModRevisionGap = event.Kv.ModRevision - base.Header.Revision
				metadata.Version = event.Kv.Version
				metadata.Lease = event.Kv.Lease
			}
			if event.PrevKv != nil {
				metadata.PrevKeyMatches = bytes.Equal(event.PrevKv.Key, keys[eventIndex])
				metadata.PrevValueMatches = bytes.Equal(event.PrevKv.Value, oldValue)
				metadata.PrevCreateGap = event.PrevKv.CreateRevision - base.Header.Revision
				metadata.PrevModGap = event.PrevKv.ModRevision - base.Header.Revision
				metadata.PrevVersion = event.PrevKv.Version
				metadata.PrevLease = event.PrevKv.Lease
			}
			outcome.EventMetadata = append(outcome.EventMetadata, metadata)
			eventIndex++
		}
		if !response.Fragment {
			break
		}
	}
	outcome.TotalEvents = eventIndex
	require.Equal(t, eventCount, eventIndex)
	require.NoError(t, stream.CloseSend())
	return outcome
}

func runWatchRawMultiframePrevKVScenario(t *testing.T, endpoint, instance string) watchRawMultiframeOutcome {
	return runWatchRawMultiframeScenarioWithPrevKV(t, endpoint, instance, true)
}
