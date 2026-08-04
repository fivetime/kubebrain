package compat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type rangeStreamConcurrentWriteOutcome struct {
	Chunks              int
	Keys                int
	Count               int64
	More                bool
	HeaderPinned        bool
	OriginalTailValue   bool
	LateInsertExcluded  bool
	KeysStrictlyOrdered bool
}

func TestRangeStreamConcurrentWriteDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run RangeStream concurrent-write differential tests")
	}

	want := runRangeStreamConcurrentWriteScenario(t, reference, "reference")
	require.Greater(t, want.Chunks, 1)
	require.Equal(t, rangeStreamConcurrentWriteOutcome{
		Chunks: want.Chunks, Keys: 12, Count: 12, HeaderPinned: true,
		OriginalTailValue: true, LateInsertExcluded: true, KeysStrictlyOrdered: true,
	}, want)
	got := runRangeStreamConcurrentWriteScenario(t, compatEndpoint(t), "kubebrain")
	// Chunk counts are implementation details; all snapshot-visible fields are not.
	want.Chunks = got.Chunks
	require.Equal(t, want, got)
}

func runRangeStreamConcurrentWriteScenario(t *testing.T, endpoint, instance string) rangeStreamConcurrentWriteOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/dbaas-rangestream-concurrent/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, deleteErr := client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, deleteErr)
		remaining, getErr := client.Get(cleanupCtx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
	})

	valuePadding := strings.Repeat("x", 320*1024)
	var pinnedRevision int64
	for i := 0; i < 12; i++ {
		response, putErr := client.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), fmt.Sprintf("old-%02d-%s", i, valuePadding))
		require.NoError(t, putErr)
		pinnedRevision = response.Header.Revision
	}

	stream, err := etcdserverpb.NewKVClient(client.ActiveConnection()).RangeStream(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
	})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, first.GetRangeResponse().GetKvs())
	require.Less(t, len(first.GetRangeResponse().GetKvs()), 12, "fixture must force more than one data chunk")
	for _, kv := range first.GetRangeResponse().GetKvs() {
		require.NotEqual(t, prefix+"11", string(kv.Key), "tail key must remain unread at the write barrier")
	}

	_, err = client.Put(ctx, prefix+"11", "new-tail")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"99-late", "late-insert")
	require.NoError(t, err)

	merged := &etcdserverpb.RangeResponse{}
	proto.Merge(merged, first.GetRangeResponse())
	chunks := 1
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		chunks++
		proto.Merge(merged, chunk.GetRangeResponse())
	}

	outcome := rangeStreamConcurrentWriteOutcome{
		Chunks: chunks, Keys: len(merged.Kvs), Count: merged.Count, More: merged.More,
		HeaderPinned:       merged.GetHeader().GetRevision() == pinnedRevision,
		LateInsertExcluded: true, KeysStrictlyOrdered: true,
	}
	for i, kv := range merged.Kvs {
		key := string(kv.Key)
		if key == prefix+"11" {
			outcome.OriginalTailValue = strings.HasPrefix(string(kv.Value), "old-11-")
		}
		if key == prefix+"99-late" {
			outcome.LateInsertExcluded = false
		}
		if i > 0 && string(merged.Kvs[i-1].Key) >= key {
			outcome.KeysStrictlyOrdered = false
		}
	}
	return outcome
}
