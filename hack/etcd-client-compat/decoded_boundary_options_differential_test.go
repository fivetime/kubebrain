package compat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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

type decodedBoundaryLargeValueProjection struct {
	Keys   []string
	Hashes [][sha256.Size]byte
	Count  int64
	More   bool
}

type decodedBoundaryLargeKeyProjection struct {
	KeyHashes   [][sha256.Size]byte
	ValueHashes [][sha256.Size]byte
	Count       int64
	More        bool
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

func TestDecodedBoundaryLargeValueRangeStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	candidate := ""
	if reference != "" {
		candidate = compatEndpoint(t)
	} else {
		reference = decodedBoundaryDirectFollower(t, splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS"))
		candidate = decodedBoundaryDirectFollower(t, splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS"))
	}
	lower := []byte("$" + testPrefix(t) + "/decoded-large-values")
	referenceOutcome := runDecodedBoundaryLargeValueStreamScenario(t, reference, lower)
	require.Equal(t, referenceOutcome, runDecodedBoundaryLargeValueStreamScenario(t, candidate, lower))
}

func TestDecodedBoundaryLargeKeyRangeStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run decoded-boundary large-key stream differential tests")
	}
	lower := []byte("$" + testPrefix(t) + "/decoded-large-keys/")
	referenceOutcome := runDecodedBoundaryLargeKeyStreamScenario(t, reference, lower)
	require.Equal(t, referenceOutcome, runDecodedBoundaryLargeKeyStreamScenario(t, compatEndpoint(t), lower))
}

func runDecodedBoundaryLargeKeyStreamScenario(
	t *testing.T, endpoint string, lower []byte,
) map[string]decodedBoundaryLargeKeyProjection {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	end := append(append([]byte(nil), lower...), 0xff)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, cleanupErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: lower, RangeEnd: end})
		require.NoError(t, cleanupErr)
	})

	// TiKV v7.5 limits physical keys to 8KiB. Use enough 7KiB user keys to
	// exceed the 1.5MiB ordering-page budget without conflating this pagination
	// test with the separately tracked etcd-vs-TiKV oversized-key schema gap.
	keys := make([][]byte, 220)
	var historicalRevision int64
	for index := range keys {
		keys[index] = make([]byte, 7<<10)
		copy(keys[index], lower)
		binary.BigEndian.PutUint16(keys[index][len(lower):], uint16(index+1))
		for offset := len(lower) + 2; offset < len(keys[index]); offset++ {
			keys[index][offset] = 'z'
		}
		response, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: keys[index], Value: []byte(fmt.Sprintf("value-%d", index)),
		})
		require.NoError(t, putErr)
		historicalRevision = response.Header.Revision
	}
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keys[0], Value: []byte("current")})
	require.NoError(t, err)

	collect := func(revision int64) decodedBoundaryLargeKeyProjection {
		stream, streamErr := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{Key: lower, RangeEnd: end, Revision: revision})
		require.NoError(t, streamErr)
		projection := decodedBoundaryLargeKeyProjection{}
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
				projection.KeyHashes = append(projection.KeyHashes, sha256.Sum256(item.Key))
				projection.ValueHashes = append(projection.ValueHashes, sha256.Sum256(item.Value))
			}
		}
		require.Len(t, projection.KeyHashes, len(keys))
		return projection
	}
	return map[string]decodedBoundaryLargeKeyProjection{
		"current":    collect(0),
		"historical": collect(historicalRevision),
	}
}

func decodedBoundaryDirectFollower(t *testing.T, endpoints []string) string {
	t.Helper()
	type member struct {
		endpoint string
		status   *etcdserverpb.StatusResponse
	}
	members := make([]member, 0, len(endpoints))
	statuses := make([]*etcdserverpb.StatusResponse, 0, len(endpoints))
	for _, endpoint := range endpoints {
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		statusResponse, statusErr := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
		cancel()
		require.NoError(t, conn.Close())
		require.NoError(t, statusErr, endpoint)
		members = append(members, member{endpoint: endpoint, status: statusResponse})
		statuses = append(statuses, statusResponse)
	}
	require.NoError(t, validateDirectReplicaTopology(statuses))
	for _, candidate := range members {
		if candidate.status.Header.MemberId != candidate.status.Leader {
			return candidate.endpoint
		}
	}
	t.Fatal("healthy direct topology did not expose a follower")
	return ""
}

func runDecodedBoundaryLargeValueStreamScenario(
	t *testing.T, endpoint string, lower []byte,
) map[string]decodedBoundaryLargeValueProjection {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	end := append(append([]byte(nil), lower...), 0xff)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, cleanupErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: lower, RangeEnd: end})
		require.NoError(t, cleanupErr)
	})

	keys := make([][]byte, 33)
	var historicalRevision int64
	for index := range keys {
		keys[index] = append(append([]byte(nil), lower...), []byte(fmt.Sprintf("/%03d", index))...)
		value := bytes.Repeat([]byte{byte(index + 1)}, 256<<10)
		response, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keys[index], Value: value})
		require.NoError(t, putErr)
		historicalRevision = response.Header.Revision
	}
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keys[0], Value: bytes.Repeat([]byte("current"), 32<<10)})
	require.NoError(t, err)

	collect := func(revision int64) decodedBoundaryLargeValueProjection {
		stream, streamErr := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{
			Key: lower, RangeEnd: end, Revision: revision,
		})
		require.NoError(t, streamErr)
		projection := decodedBoundaryLargeValueProjection{}
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
				projection.Hashes = append(projection.Hashes, sha256.Sum256(item.Value))
			}
		}
		require.Len(t, projection.Keys, len(keys))
		return projection
	}
	return map[string]decodedBoundaryLargeValueProjection{
		"current":    collect(0),
		"historical": collect(historicalRevision),
	}
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
