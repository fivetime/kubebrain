package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type multiFrameRangeKV struct {
	Key, ValueHash              string
	CreateRevision, ModRevision int
	Version                     int64
}

type multiFrameRangeOutcome struct {
	HeaderRevision int
	Count          int64
	More           bool
	KVs            []multiFrameRangeKV
}

func TestRangeStreamMultiFrameDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run multi-frame RangeStream differential tests")
	}

	want := runRangeStreamMultiFrameScenario(t, reference, "reference")
	require.Equal(t, want, runRangeStreamMultiFrameScenario(t, compatEndpoint(t), "kubebrain"))
}

func runRangeStreamMultiFrameScenario(t *testing.T, endpoint, instance string) []multiFrameRangeOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/a3726/rangestream-multiframe/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	revisionOrdinal := make(map[int64]int, 18)
	var historicalRevision int64
	for index := 0; index < 17; index++ {
		value := make([]byte, 192*1024)
		for offset := range value {
			value[offset] = byte('a' + index)
		}
		put, putErr := cli.Put(ctx, fmt.Sprintf("%s%02d", prefix, index), string(value))
		require.NoError(t, putErr)
		revisionOrdinal[put.Header.Revision] = index + 1
		historicalRevision = put.Header.Revision
	}
	updated := make([]byte, 192*1024)
	for offset := range updated {
		updated[offset] = 'z'
	}
	put, err := cli.Put(ctx, prefix+"00", string(updated))
	require.NoError(t, err)
	revisionOrdinal[put.Header.Revision] = 18
	seedFinalRevision := put.Header.Revision

	requests := []*etcdserverpb.RangeRequest{
		{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix))},
		{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), Revision: historicalRevision},
		{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), Limit: 12},
		{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), Revision: historicalRevision, Limit: 12},
	}

	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	outcomes := make([]multiFrameRangeOutcome, 0, len(requests))
	for index, request := range requests {
		unary, unaryErr := kv.Range(ctx, request)
		require.NoError(t, unaryErr, "case %d", index)
		want := normalizeMultiFrameRange(unary, prefix, revisionOrdinal, seedFinalRevision)

		stream, streamErr := kv.RangeStream(ctx, request)
		require.NoError(t, streamErr, "case %d", index)
		merged := &etcdserverpb.RangeResponse{}
		frames, envelopeFrame, envelopeKVs := 0, -1, 0
		for {
			chunk, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			require.NoError(t, recvErr, "case %d frame %d", index, frames)
			response := chunk.GetRangeResponse()
			require.NotNil(t, response, "case %d frame %d", index, frames)
			if response.Header != nil {
				require.Nil(t, merged.Header, "case %d has more than one envelope frame", index)
				merged.Header, merged.Count, merged.More = response.Header, response.Count, response.More
				envelopeFrame, envelopeKVs = frames, len(response.Kvs)
			} else {
				require.Zero(t, response.Count, "case %d frame %d", index, frames)
				require.False(t, response.More, "case %d frame %d", index, frames)
			}
			merged.Kvs = append(merged.Kvs, response.Kvs...)
			frames++
		}
		require.Greater(t, frames, 1, "case %d must exercise multiple wire frames", index)
		require.NotNil(t, merged.Header, "case %d final envelope is missing", index)
		// A3725's contract: the final envelope is attached to the final data
		// frame, not emitted as a separate empty terminal frame.
		require.Equal(t, frames-1, envelopeFrame, "case %d envelope must be on the last frame", index)
		require.Positive(t, envelopeKVs, "case %d final envelope frame must carry data", index)
		got := normalizeMultiFrameRange(merged, prefix, revisionOrdinal, seedFinalRevision)
		require.Equal(t, want, got, "case %d stream must merge to unary Range", index)
		outcomes = append(outcomes, got)
	}
	return outcomes
}

func normalizeMultiFrameRange(
	response *etcdserverpb.RangeResponse,
	prefix string,
	revisionOrdinal map[int64]int,
	seedFinalRevision int64,
) multiFrameRangeOutcome {
	outcome := multiFrameRangeOutcome{
		HeaderRevision: observeGeneratedRangeHeaderRevision(
			response.Header.Revision, revisionOrdinal, seedFinalRevision,
		),
		Count: response.Count,
		More:  response.More,
		KVs:   make([]multiFrameRangeKV, 0, len(response.Kvs)),
	}
	for _, item := range response.Kvs {
		outcome.KVs = append(outcome.KVs, normalizeMultiFrameRangeKV(item, prefix, revisionOrdinal))
	}
	return outcome
}

func normalizeMultiFrameRangeKV(
	item *mvccpb.KeyValue,
	prefix string,
	revisionOrdinal map[int64]int,
) multiFrameRangeKV {
	digest := sha256.Sum256(item.Value)
	return multiFrameRangeKV{
		Key: string(item.Key[len(prefix):]), ValueHash: fmt.Sprintf("%x", digest),
		CreateRevision: revisionOrdinal[item.CreateRevision], ModRevision: revisionOrdinal[item.ModRevision],
		Version: item.Version,
	}
}
