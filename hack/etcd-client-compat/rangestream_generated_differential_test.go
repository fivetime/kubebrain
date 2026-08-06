package compat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type generatedRangeStreamSpec struct {
	Shape        int
	Historical   bool
	Limit        int64
	KeysOnly     bool
	CountOnly    bool
	Serializable bool
}

type generatedRangeStreamOutcome struct {
	Unary, Stream generatedRangeOutcome
	Frames        int
	EndedWithEOF  bool
}

func TestGeneratedRangeStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	specs := generatedRangeStreamSpecs()
	referenceOutcomes := runGeneratedRangeStreamScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcomes,
		runGeneratedRangeStreamScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedRangeStreamSeedCoversSupportedOptionFamilies(t *testing.T) {
	shapes := map[int]bool{}
	limits := map[int64]bool{}
	var current, historical, keysOnly, countOnly, serializable, linearizable bool
	var keysOnlyLimited, keysOnlyNegative, keysOnlyCountOnly, leasedPoint, leasedPrefix bool
	for _, spec := range generatedRangeStreamSpecs() {
		shapes[spec.Shape], limits[spec.Limit] = true, true
		current, historical = current || !spec.Historical, historical || spec.Historical
		keysOnly, countOnly = keysOnly || spec.KeysOnly, countOnly || spec.CountOnly
		serializable, linearizable = serializable || spec.Serializable, linearizable || !spec.Serializable
		keysOnlyLimited = keysOnlyLimited || spec.KeysOnly && spec.Limit > 0
		keysOnlyNegative = keysOnlyNegative || spec.KeysOnly && spec.Limit < 0
		keysOnlyCountOnly = keysOnlyCountOnly || spec.KeysOnly && spec.CountOnly
		leasedPoint = leasedPoint || spec.Shape == 6 && spec.KeysOnly && spec.Limit > 0
		leasedPrefix = leasedPrefix || spec.Shape == 0 && spec.KeysOnly && spec.Limit > 0
	}
	require.Len(t, shapes, 11)
	require.Len(t, limits, 5)
	require.True(t, current)
	require.True(t, historical)
	require.True(t, keysOnly)
	require.True(t, countOnly)
	require.True(t, serializable)
	require.True(t, linearizable)
	require.True(t, keysOnlyLimited)
	require.True(t, keysOnlyNegative)
	require.True(t, keysOnlyCountOnly)
	require.True(t, leasedPoint)
	require.True(t, leasedPrefix)
}

func generatedRangeStreamSpecs() []generatedRangeStreamSpec {
	rng := rand.New(rand.NewSource(3725))
	limits := []int64{-1, 0, 1, 2, math.MaxInt64}
	specs := make([]generatedRangeStreamSpec, 64)
	for index := range specs {
		specs[index] = generatedRangeStreamSpec{
			Shape: rng.Intn(11), Historical: rng.Intn(2) == 0,
			Limit: limits[rng.Intn(len(limits))], KeysOnly: rng.Intn(3) == 0,
			CountOnly: rng.Intn(4) == 0, Serializable: rng.Intn(2) == 0,
		}
	}
	for shape := 0; shape < 11; shape++ {
		specs[shape].Shape = shape
	}
	specs[11] = generatedRangeStreamSpec{Shape: 0, Limit: 1, KeysOnly: true}
	specs[12] = generatedRangeStreamSpec{Shape: 6, Limit: 1, KeysOnly: true}
	specs[13] = generatedRangeStreamSpec{Shape: 0, Limit: -1, KeysOnly: true, Historical: true}
	specs[14] = generatedRangeStreamSpec{Shape: 0, Limit: 2, KeysOnly: true, CountOnly: true, Serializable: true}
	return specs
}

func runGeneratedRangeStreamScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedRangeStreamSpec,
) []generatedRangeStreamOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a3725/generated-rangestream/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	revisions := make([]int64, 10)
	revisions[1] = generatedRangePut(t, ctx, cli, prefix+"a", "va1")
	revisions[2] = generatedRangePut(t, ctx, cli, prefix+"b", "vb1")
	revisions[3] = generatedRangePut(t, ctx, cli, prefix+"c", "vc1")
	revisions[4] = generatedRangePut(t, ctx, cli, prefix+"d", "vd1")
	revisions[5] = generatedRangePut(t, ctx, cli, prefix+"b", "vb2")
	deleted, err := cli.Delete(ctx, prefix+"c")
	require.NoError(t, err)
	revisions[6] = deleted.Header.Revision
	revisions[7] = generatedRangePut(t, ctx, cli, prefix+"c", "vc2")
	revisions[8] = generatedRangePut(t, ctx, cli, prefix+"e", "ve1")
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
	})
	leased, err := cli.Put(ctx, prefix+"f", "vf1", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	revisions[9] = leased.Header.Revision
	revisionOrdinal := make(map[int64]int, len(revisions))
	for ordinal := 1; ordinal < len(revisions); ordinal++ {
		revisionOrdinal[revisions[ordinal]] = ordinal
	}

	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	outcomes := make([]generatedRangeStreamOutcome, 0, len(specs))
	for index, spec := range specs {
		request := generatedRangeStreamRequest(prefix, spec, revisions)
		unary, unaryErr := kv.Range(ctx, request)
		outcome := generatedRangeStreamOutcome{}
		outcome.Unary = observeGeneratedRangeCall(unary, unaryErr, prefix, revisionOrdinal)

		stream, streamErr := kv.RangeStream(ctx, request)
		outcome.Stream.Code = status.Code(streamErr).String()
		outcome.Stream.Message = status.Convert(streamErr).Message()
		if streamErr == nil {
			for {
				chunk, recvErr := stream.Recv()
				if errors.Is(recvErr, io.EOF) {
					outcome.EndedWithEOF = true
					break
				}
				if recvErr != nil {
					outcome.Stream.Code = status.Code(recvErr).String()
					outcome.Stream.Message = status.Convert(recvErr).Message()
					break
				}
				outcome.Frames++
				response := chunk.GetRangeResponse()
				require.NotNil(t, response, "case %d", index)
				outcome.Stream.KVs = append(outcome.Stream.KVs,
					observeGeneratedRangeKVs(response, prefix, revisionOrdinal)...)
				// etcd deliberately omits Header/Count/More on intermediate
				// RangeStream chunks and supplies the aggregate envelope only on
				// the final frame.
				if response.Header != nil {
					outcome.Stream.HeaderRevision = revisionOrdinal[response.Header.Revision]
					outcome.Stream.Count, outcome.Stream.More = response.Count, response.More
				}
			}
		}
		require.Equal(t, outcome.Unary, outcome.Stream, "case %d stream must equal unary", index)
		require.True(t, outcome.EndedWithEOF, "case %d", index)
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func generatedRangeStreamRequest(
	prefix string,
	spec generatedRangeStreamSpec,
	revisions []int64,
) *etcdserverpb.RangeRequest {
	request := generatedRangeRequest(prefix, generatedRangeSpec{Shape: spec.Shape}, revisions)
	request.Limit = spec.Limit
	request.KeysOnly = spec.KeysOnly
	request.CountOnly = spec.CountOnly
	request.Serializable = spec.Serializable
	if spec.Historical {
		request.Revision = revisions[5]
	}
	return request
}

func observeGeneratedRangeCall(
	response *etcdserverpb.RangeResponse,
	callErr error,
	prefix string,
	revisionOrdinal map[int64]int,
) generatedRangeOutcome {
	outcome := generatedRangeOutcome{Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message()}
	if callErr != nil {
		return outcome
	}
	outcome.HeaderRevision = revisionOrdinal[response.Header.Revision]
	outcome.Count, outcome.More = response.Count, response.More
	outcome.KVs = observeGeneratedRangeKVs(response, prefix, revisionOrdinal)
	return outcome
}

func observeGeneratedRangeKVs(
	response *etcdserverpb.RangeResponse,
	prefix string,
	revisionOrdinal map[int64]int,
) []generatedRangeKV {
	if len(response.Kvs) == 0 {
		return nil
	}
	kvs := make([]generatedRangeKV, 0, len(response.Kvs))
	for _, item := range response.Kvs {
		kvs = append(kvs, generatedRangeKV{
			Key: string(item.Key[len(prefix):]), Value: string(item.Value),
			CreateRevision: revisionOrdinal[item.CreateRevision], ModRevision: revisionOrdinal[item.ModRevision],
			Version: item.Version, Leased: item.Lease != 0,
		})
	}
	return kvs
}
