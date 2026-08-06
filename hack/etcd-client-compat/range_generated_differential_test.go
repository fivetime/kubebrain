package compat

import (
	"context"
	"fmt"
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

type generatedRangeSpec struct {
	Point      int
	Historical int
	SortTarget etcdserverpb.RangeRequest_SortTarget
	SortOrder  etcdserverpb.RangeRequest_SortOrder
	Limit      int64
	KeysOnly   bool
	CountOnly  bool
	Filter     int
	FilterRev  int
}

type generatedRangeKV struct {
	Key, Value                  string
	CreateRevision, ModRevision int
	Version, Lease              int64
}

type generatedRangeOutcome struct {
	Code, Message  string
	HeaderRevision int
	Count          int64
	More           bool
	KVs            []generatedRangeKV
}

func TestGeneratedRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	specs := generatedRangeSpecs()
	require.Len(t, specs, 64)
	referenceOutcome := runGeneratedRangeScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcome, runGeneratedRangeScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedRangeSeedCoversEveryOptionFamily(t *testing.T) {
	points, historical, targets, orders := map[int]bool{}, map[bool]bool{}, map[int32]bool{}, map[int32]bool{}
	limits, filters := map[int64]bool{}, map[int]bool{}
	var keysOnly, countOnly bool
	for _, spec := range generatedRangeSpecs() {
		points[spec.Point], historical[spec.Historical > 0] = true, true
		targets[int32(spec.SortTarget)], orders[int32(spec.SortOrder)] = true, true
		limits[spec.Limit], filters[spec.Filter] = true, true
		keysOnly, countOnly = keysOnly || spec.KeysOnly, countOnly || spec.CountOnly
	}
	require.Len(t, points, 5)
	require.Len(t, historical, 2)
	require.Len(t, targets, 5)
	require.Len(t, orders, 3)
	require.Len(t, limits, 5)
	require.Len(t, filters, 7)
	require.True(t, keysOnly)
	require.True(t, countOnly)
}

func generatedRangeSpecs() []generatedRangeSpec {
	rng := rand.New(rand.NewSource(3713))
	limits := []int64{0, 1, 2, 4, math.MaxInt64}
	specs := make([]generatedRangeSpec, 0, 64)
	for i := 0; i < 64; i++ {
		specs = append(specs, generatedRangeSpec{
			Point: rng.Intn(5), Historical: rng.Intn(9),
			SortTarget: etcdserverpb.RangeRequest_SortTarget(rng.Intn(5)),
			SortOrder:  etcdserverpb.RangeRequest_SortOrder(rng.Intn(3)),
			Limit:      limits[rng.Intn(len(limits))], KeysOnly: rng.Intn(4) == 0,
			CountOnly: rng.Intn(5) == 0, Filter: rng.Intn(7), FilterRev: 1 + rng.Intn(8),
		})
	}
	return specs
}

func runGeneratedRangeScenario(t *testing.T, endpoint, instance string, specs []generatedRangeSpec) []generatedRangeOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/a3713/generated-range/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	revisions := make([]int64, 9)
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
	revisionOrdinal := make(map[int64]int, len(revisions))
	for ordinal := 1; ordinal < len(revisions); ordinal++ {
		revisionOrdinal[revisions[ordinal]] = ordinal
	}

	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	outcomes := make([]generatedRangeOutcome, 0, len(specs))
	for index, spec := range specs {
		request := generatedRangeRequest(prefix, spec, revisions)
		response, callErr := kv.Range(ctx, request)
		outcome := generatedRangeOutcome{Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message()}
		if callErr == nil {
			require.NotNil(t, response.Header, "case %d", index)
			outcome.HeaderRevision = revisionOrdinal[response.Header.Revision]
			outcome.Count, outcome.More = response.Count, response.More
			for _, item := range response.Kvs {
				outcome.KVs = append(outcome.KVs, generatedRangeKV{
					Key: string(item.Key[len(prefix):]), Value: string(item.Value),
					CreateRevision: revisionOrdinal[item.CreateRevision], ModRevision: revisionOrdinal[item.ModRevision],
					Version: item.Version, Lease: item.Lease,
				})
			}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func generatedRangePut(t *testing.T, ctx context.Context, cli *clientv3.Client, key, value string) int64 {
	t.Helper()
	response, err := cli.Put(ctx, key, value)
	require.NoError(t, err)
	return response.Header.Revision
}

func generatedRangeRequest(prefix string, spec generatedRangeSpec, revisions []int64) *etcdserverpb.RangeRequest {
	request := &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		SortTarget: spec.SortTarget, SortOrder: spec.SortOrder, Limit: spec.Limit,
		KeysOnly: spec.KeysOnly, CountOnly: spec.CountOnly,
	}
	if spec.Point > 0 {
		request.Key = []byte(prefix + string(rune('a'+spec.Point-1)))
		request.RangeEnd = nil
	}
	if spec.Historical > 0 {
		request.Revision = revisions[spec.Historical]
	}
	filterRevision := revisions[spec.FilterRev]
	switch spec.Filter {
	case 1:
		request.MinModRevision = filterRevision
	case 2:
		request.MaxModRevision = filterRevision
	case 3:
		request.MinCreateRevision = filterRevision
	case 4:
		request.MaxCreateRevision = filterRevision
	case 5:
		request.MinModRevision, request.MaxModRevision = filterRevision, revisions[1]
	case 6:
		request.MinCreateRevision, request.MaxCreateRevision = filterRevision, revisions[1]
	}
	return request
}
