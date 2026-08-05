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
	"google.golang.org/protobuf/proto"
)

const watchFragmentExactLimit = 2 * 1024 * 1024

type watchExactLimitOutcome struct {
	CandidateMatchesTarget bool
	FrameEventCounts       []int
	FragmentFlags          []bool
	FrameBelowLimit        []bool
	HeaderRevisionGaps     []int64
	HeadersMatchCreated    bool
	KeysOrdered            bool
	ValuesMatch            bool
	MetadataMatches        bool
	TotalEvents            int
}

func TestWatchExactFragmentLimitDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := watchExactLimitOutcome{
		CandidateMatchesTarget: true,
		FrameEventCounts:       []int{1, 1},
		FragmentFlags:          []bool{true, false},
		FrameBelowLimit:        []bool{true, true},
		HeaderRevisionGaps:     []int64{3, 3},
		HeadersMatchCreated:    true,
		KeysOrdered:            true,
		ValuesMatch:            true,
		MetadataMatches:        true,
		TotalEvents:            2,
	}
	referenceOutcome := runWatchExactLimitScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome,
		runWatchExactLimitScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchFragmentLimitBoundaryMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	tests := []struct {
		name      string
		sizeDelta int
		want      watchExactLimitOutcome
	}{
		{name: "one byte below", sizeDelta: -1, want: watchExactLimitOutcome{
			CandidateMatchesTarget: true, FrameEventCounts: []int{2},
			FragmentFlags: []bool{false}, FrameBelowLimit: []bool{true},
			HeaderRevisionGaps: []int64{3}, HeadersMatchCreated: true, KeysOrdered: true, ValuesMatch: true,
			MetadataMatches: true, TotalEvents: 2,
		}},
		{name: "exact", want: watchExactLimitOutcome{
			CandidateMatchesTarget: true, FrameEventCounts: []int{1, 1},
			FragmentFlags: []bool{true, false}, FrameBelowLimit: []bool{true, true},
			HeaderRevisionGaps: []int64{3, 3}, HeadersMatchCreated: true, KeysOrdered: true, ValuesMatch: true,
			MetadataMatches: true, TotalEvents: 2,
		}},
		{name: "one byte above", sizeDelta: 1, want: watchExactLimitOutcome{
			CandidateMatchesTarget: true, FrameEventCounts: []int{1, 1},
			FragmentFlags: []bool{true, false}, FrameBelowLimit: []bool{true, true},
			HeaderRevisionGaps: []int64{3, 3}, HeadersMatchCreated: true, KeysOrdered: true, ValuesMatch: true,
			MetadataMatches: true, TotalEvents: 2,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := watchFragmentExactLimit + tc.sizeDelta
			referenceOutcome := runWatchLimitBoundaryScenario(t, reference, "etcd", target)
			require.Equal(t, tc.want, referenceOutcome)
			require.Equal(t, referenceOutcome,
				runWatchLimitBoundaryScenario(t, compatEndpoint(t), "kubebrain", target))
		})
	}
}

func TestWatchFragmentLimitBoundaryAcrossDirectReplicas(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}
	endpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, endpoints)

	for _, sizeDelta := range []int{-1, 0, 1} {
		target := watchFragmentExactLimit + sizeDelta
		referenceOutcome := runWatchLimitBoundaryScenario(t, reference, fmt.Sprintf("etcd-%d", sizeDelta), target)
		require.True(t, referenceOutcome.HeadersMatchCreated)
		for index, endpoint := range endpoints {
			t.Run(fmt.Sprintf("delta_%d/replica_%d", sizeDelta, index), func(t *testing.T) {
				require.Equal(t, referenceOutcome,
					runWatchLimitBoundaryScenario(t, endpoint, fmt.Sprintf("kubebrain-%d-%d", sizeDelta, index), target))
			})
		}
	}
}

func runWatchExactLimitScenario(t *testing.T, endpoint, instance string) watchExactLimitOutcome {
	return runWatchLimitBoundaryScenario(t, endpoint, instance, watchFragmentExactLimit)
}

func runWatchLimitBoundaryScenario(t *testing.T, endpoint, instance string, targetSize int) watchExactLimitOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	prefix := fmt.Sprintf("/dbaas-watch-limit-boundary/%d/%s/%d/", targetSize, instance, time.Now().UnixNano())
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

	const watchID = 3640
	keys := [][]byte{[]byte(prefix + "0"), []byte(prefix + "1")}
	values := [][]byte{make([]byte, 1024*1024), nil}
	seedRevisions := []int64{base.Header.Revision + 1, base.Header.Revision + 2}
	revision := base.Header.Revision + 3
	candidate := &etcdserverpb.WatchResponse{
		Header:  proto.Clone(base.Header).(*etcdserverpb.ResponseHeader),
		WatchId: watchID,
		Events: []*mvccpb.Event{
			{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: keys[0], ModRevision: revision}, PrevKv: &mvccpb.KeyValue{Key: keys[0], Value: values[0], CreateRevision: seedRevisions[0], ModRevision: seedRevisions[0], Version: 1}},
			{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: keys[1], ModRevision: revision}, PrevKv: &mvccpb.KeyValue{Key: keys[1], CreateRevision: seedRevisions[1], ModRevision: seedRevisions[1], Version: 1}},
		},
	}
	candidate.Header.Revision = revision
	values[1] = fitWatchResponseToExactSize(t, candidate, targetSize)

	for index := range keys {
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keys[index], Value: values[index]})
		require.NoError(t, putErr)
		require.Equal(t, seedRevisions[index], put.Header.GetRevision())
	}

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			StartRevision: revision, WatchId: watchID, Fragment: true, PrevKv: true,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.Equal(t, int64(watchID), created.WatchId)
	require.NotNil(t, created.Header)
	require.Equal(t, candidate.Header.ClusterId, created.Header.ClusterId)
	require.Equal(t, candidate.Header.MemberId, created.Header.MemberId)
	require.Equal(t, candidate.Header.RaftTerm, created.Header.RaftTerm)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: keys[0]}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: keys[1]}}},
	}})
	require.NoError(t, err)

	outcome := watchExactLimitOutcome{
		CandidateMatchesTarget: proto.Size(candidate) == targetSize,
		HeadersMatchCreated:    true, KeysOrdered: true, ValuesMatch: true, MetadataMatches: true,
	}
	reassembled := &etcdserverpb.WatchResponse{}
	for {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotEmpty(t, response.Events)
		if reassembled.Header == nil {
			reassembled.Header = response.Header
			reassembled.WatchId = response.WatchId
		}
		reassembled.Events = append(reassembled.Events, response.Events...)
		outcome.FrameEventCounts = append(outcome.FrameEventCounts, len(response.Events))
		outcome.FragmentFlags = append(outcome.FragmentFlags, response.Fragment)
		outcome.FrameBelowLimit = append(outcome.FrameBelowLimit, proto.Size(response) < watchFragmentExactLimit)
		outcome.HeaderRevisionGaps = append(outcome.HeaderRevisionGaps, response.Header.GetRevision()-base.Header.Revision)
		outcome.HeadersMatchCreated = outcome.HeadersMatchCreated && response.Header != nil &&
			response.Header.ClusterId == created.Header.ClusterId && response.Header.MemberId == created.Header.MemberId &&
			response.Header.RaftTerm == created.Header.RaftTerm
		for _, event := range response.Events {
			index := outcome.TotalEvents
			require.Less(t, index, len(keys))
			outcome.KeysOrdered = outcome.KeysOrdered && bytes.Equal(event.GetKv().GetKey(), keys[index])
			outcome.ValuesMatch = outcome.ValuesMatch && bytes.Equal(event.GetPrevKv().GetValue(), values[index])
			outcome.MetadataMatches = outcome.MetadataMatches && event.GetType() == mvccpb.DELETE &&
				event.GetKv().GetModRevision() == revision && len(event.GetKv().GetValue()) == 0 &&
				bytes.Equal(event.GetPrevKv().GetKey(), keys[index]) &&
				event.GetPrevKv().GetCreateRevision() == seedRevisions[index] &&
				event.GetPrevKv().GetModRevision() == seedRevisions[index] &&
				event.GetPrevKv().GetVersion() == 1 && event.GetPrevKv().GetLease() == 0
			outcome.TotalEvents++
		}
		if !response.Fragment {
			break
		}
	}
	outcome.CandidateMatchesTarget = outcome.CandidateMatchesTarget && proto.Size(reassembled) == targetSize
	require.NoError(t, stream.CloseSend())
	return outcome
}

func fitWatchResponseToExactSize(t *testing.T, response *etcdserverpb.WatchResponse, target int) []byte {
	t.Helper()
	require.Len(t, response.Events, 2)
	kv := response.Events[1].PrevKv
	require.NotNil(t, kv)

	sizeWithoutValue := proto.Size(response)
	length := target - sizeWithoutValue
	for attempt := 0; attempt < 8; attempt++ {
		require.Positive(t, length)
		kv.Value = make([]byte, length)
		delta := target - proto.Size(response)
		if delta == 0 {
			return kv.Value
		}
		length += delta
	}
	t.Fatalf("cannot fit WatchResponse to exactly %d bytes; closest size is %d", target, proto.Size(response))
	return nil
}
