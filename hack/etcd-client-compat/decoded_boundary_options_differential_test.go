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
