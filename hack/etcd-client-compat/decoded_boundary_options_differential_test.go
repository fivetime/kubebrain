package compat

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type decodedBoundaryRangeProjection struct {
	Keys   []string
	Values []string
	Count  int64
	More   bool
}

func TestDecodedBoundaryRangeOptionsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run decoded-boundary option differential tests")
	}
	lower := []byte("$" + testPrefix(t) + "/decoded-options")
	referenceOutcome := runDecodedBoundaryRangeOptionsScenario(t, reference, lower)
	require.Equal(t, referenceOutcome, runDecodedBoundaryRangeOptionsScenario(t, compatEndpoint(t), lower))
}

func TestDecodedBoundaryRangeStreamPagedDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run decoded-boundary paged stream differential tests")
	}
	lower := []byte("$" + testPrefix(t) + "/decoded-paged")
	referenceOutcome := runDecodedBoundaryPagedStreamScenario(t, reference, lower)
	require.Equal(t, referenceOutcome, runDecodedBoundaryPagedStreamScenario(t, compatEndpoint(t), lower))
}

func runDecodedBoundaryPagedStreamScenario(t *testing.T, endpoint string, lower []byte) decodedBoundaryRangeProjection {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	end := append(append([]byte(nil), lower...), 1)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, cleanupErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: lower, RangeEnd: end})
		require.NoError(t, cleanupErr)
	})

	keys := make([][]byte, 303)
	keys[0] = append([]byte(nil), lower...)
	keys[1] = append(append([]byte(nil), lower...), 0)
	for index := 2; index < len(keys); index++ {
		keys[index] = append(append(append([]byte(nil), lower...), 0), []byte(fmt.Sprintf("/%03d", index))...)
	}
	var historicalRevision int64
	for index, key := range keys {
		putResponse, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte(fmt.Sprintf("value-%03d", index))})
		require.NoError(t, putErr)
		historicalRevision = putResponse.Header.Revision
	}
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keys[0], Value: []byte("current-only")})
	require.NoError(t, err)
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: keys[2]})
	require.NoError(t, err)
	newCurrentKey := append(append(append([]byte(nil), lower...), 0), []byte("/current-only")...)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: newCurrentKey, Value: []byte("current-only")})
	require.NoError(t, err)

	// Warm the target replica's ordering index strictly after historicalRevision.
	// KubeBrain must then move BaseRev backwards and replay these three durable
	// changes forward; serving the historical request by materializing all KVs
	// would remain wire-compatible but fail the resource invariant under test.
	warm, err := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{Key: lower, RangeEnd: end})
	require.NoError(t, err)
	var warmCount int
	for {
		response, recvErr := warm.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
		warmCount += len(response.GetRangeResponse().Kvs)
	}
	require.Equal(t, len(keys), warmCount, "delete+create keeps the current cardinality stable")

	stream, err := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{
		Key: lower, RangeEnd: end, Revision: historicalRevision,
	})
	require.NoError(t, err)
	projection := decodedBoundaryRangeProjection{}
	for {
		response, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
		rangeResponse := response.GetRangeResponse()
		projection.Count = rangeResponse.Count
		projection.More = rangeResponse.More
		for _, item := range rangeResponse.Kvs {
			projection.Keys = append(projection.Keys, string(item.Key))
			projection.Values = append(projection.Values, string(item.Value))
		}
	}
	require.Len(t, projection.Keys, len(keys))
	return projection
}

func runDecodedBoundaryRangeOptionsScenario(t *testing.T, endpoint string, lower []byte) map[string]decodedBoundaryRangeProjection {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	end := append(append([]byte(nil), lower...), make([]byte, 17)...)
	keys := make([][]byte, 17)
	seedRevisions := make([]int64, len(keys))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, cleanupErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: lower, RangeEnd: end})
		require.NoError(t, cleanupErr)
	})
	for index := range keys {
		keys[index] = append(append([]byte(nil), lower...), make([]byte, index)...)
		response, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: keys[index], Value: []byte(fmt.Sprintf("seed-%02d", len(keys)-index)),
		})
		require.NoError(t, putErr)
		seedRevisions[index] = response.Header.Revision
	}
	seedRevision := seedRevisions[len(seedRevisions)-1]
	var firstUpdateRevision int64
	for updateIndex, keyIndex := range []int{0, 8, 16} {
		response, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: keys[keyIndex], Value: []byte(fmt.Sprintf("updated-%02d", keyIndex)),
		})
		require.NoError(t, putErr)
		if updateIndex == 0 {
			firstUpdateRevision = response.Header.Revision
		}
	}
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: keys[4]})
	require.NoError(t, err)

	requests := map[string]*etcdserverpb.RangeRequest{
		"default-limit": {Key: lower, RangeEnd: end, Limit: 3},
		"key-desc": {
			Key: lower, RangeEnd: end, Limit: 4,
			SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
		},
		"value-asc": {
			Key: lower, RangeEnd: end, Limit: 5,
			SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
		},
		"keys-only":  {Key: lower, RangeEnd: end, Limit: 6, KeysOnly: true},
		"count-only": {Key: lower, RangeEnd: end, Limit: 1, CountOnly: true},
		"historical": {Key: lower, RangeEnd: end, Revision: seedRevision, Limit: 7},
		"min-mod": {
			Key: lower, RangeEnd: end, MinModRevision: firstUpdateRevision, Limit: 2,
		},
		"max-mod": {
			Key: lower, RangeEnd: end, MaxModRevision: seedRevision, Limit: 5,
		},
		"min-create": {
			Key: lower, RangeEnd: end, MinCreateRevision: seedRevisions[8], Limit: 4,
		},
		"max-create": {
			Key: lower, RangeEnd: end, MaxCreateRevision: seedRevisions[8], Limit: 4,
		},
	}
	outcome := make(map[string]decodedBoundaryRangeProjection, len(requests))
	for name, request := range requests {
		response, rangeErr := kv.Range(ctx, request)
		require.NoError(t, rangeErr, name)
		projection := decodedBoundaryRangeProjection{Count: response.Count, More: response.More}
		for _, item := range response.Kvs {
			projection.Keys = append(projection.Keys, string(item.Key))
			projection.Values = append(projection.Values, string(item.Value))
		}
		outcome[name] = projection
	}
	streamRequests := map[string]*etcdserverpb.RangeRequest{
		"stream/default-limit": {Key: lower, RangeEnd: end, Limit: 3},
		"stream/keys-only":     {Key: lower, RangeEnd: end, Limit: 6, KeysOnly: true},
		"stream/count-only":    {Key: lower, RangeEnd: end, Limit: 1, CountOnly: true},
		"stream/historical":    {Key: lower, RangeEnd: end, Revision: seedRevision, Limit: 7},
	}
	for name, request := range streamRequests {
		stream, streamErr := kv.RangeStream(ctx, request)
		require.NoError(t, streamErr, name)
		projection := decodedBoundaryRangeProjection{}
		for {
			response, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			require.NoError(t, recvErr, name)
			rangeResponse := response.GetRangeResponse()
			projection.Count = rangeResponse.Count
			projection.More = rangeResponse.More
			for _, item := range rangeResponse.Kvs {
				projection.Keys = append(projection.Keys, string(item.Key))
				projection.Values = append(projection.Values, string(item.Value))
			}
		}
		outcome[name] = projection
	}
	return outcome
}
