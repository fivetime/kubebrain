package compat

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type generatedDeleteSpec struct {
	Shape  int
	PrevKV bool
}

type generatedDeleteKV struct {
	Key, Value                  string
	CreateRevision, ModRevision int
	Version                     int64
	Leased                      bool
}

type generatedDeleteOutcome struct {
	Code, Message string
	HeaderGap     int64
	Deleted       int64
	PrevKVs       []generatedDeleteKV
	Remaining     []generatedDeleteKV
}

func TestGeneratedDeleteDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run generated DeleteRange differential tests")
	}

	specs := generatedDeleteSpecs()
	require.Len(t, specs, 32)
	referenceOutcome := runGeneratedDeleteScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcome, runGeneratedDeleteScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedDeleteSeedCoversEveryShapeAndProjection(t *testing.T) {
	shapes, prev := map[int]bool{}, map[bool]bool{}
	for _, spec := range generatedDeleteSpecs() {
		shapes[spec.Shape], prev[spec.PrevKV] = true, true
	}
	require.Len(t, shapes, 7)
	require.Len(t, prev, 2)
}

func generatedDeleteSpecs() []generatedDeleteSpec {
	rng := rand.New(rand.NewSource(3719))
	specs := make([]generatedDeleteSpec, 32)
	for index := range specs {
		specs[index] = generatedDeleteSpec{Shape: rng.Intn(7), PrevKV: rng.Intn(2) == 0}
	}
	for shape := 0; shape < 7; shape++ {
		specs[shape].Shape = shape
	}
	specs[7].PrevKV = true
	specs[8].PrevKV = false
	return specs
}

func runGeneratedDeleteScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedDeleteSpec,
) []generatedDeleteOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root := fmt.Sprintf("/a3719/generated-delete/%s/%d/", instance, time.Now().UnixNano())
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Revoke(cleanupCtx, lease.ID)
		_, _ = client.Delete(cleanupCtx, root, clientv3.WithPrefix())
	})

	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	outcomes := make([]generatedDeleteOutcome, 0, len(specs))
	for index, spec := range specs {
		prefix := fmt.Sprintf("%scase-%02d/", root, index)
		revisions := make([]int64, 6)
		revisions[1] = generatedDeletePut(t, ctx, client, prefix+"a", "va", 0)
		revisions[2] = generatedDeletePut(t, ctx, client, prefix+"b", "vb1", 0)
		revisions[3] = generatedDeletePut(t, ctx, client, prefix+"c", "vc", lease.ID)
		revisions[4] = generatedDeletePut(t, ctx, client, prefix+"d", "vd", 0)
		revisions[5] = generatedDeletePut(t, ctx, client, prefix+"b", "vb2", 0)
		revisionOrdinal := make(map[int64]int, len(revisions))
		for ordinal := 1; ordinal < len(revisions); ordinal++ {
			revisionOrdinal[revisions[ordinal]] = ordinal
		}

		request := generatedDeleteRequest(prefix, spec)
		response, callErr := kv.DeleteRange(ctx, request)
		outcome := generatedDeleteOutcome{
			Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		}
		if callErr == nil {
			require.NotNil(t, response.Header, "case %d", index)
			outcome.HeaderGap = response.Header.Revision - revisions[5]
			outcome.Deleted = response.Deleted
			outcome.PrevKVs = normalizeGeneratedDeleteKVs(response.PrevKvs, prefix, revisionOrdinal)
			remaining, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			})
			require.NoError(t, rangeErr)
			require.Equal(t, response.Header.Revision, remaining.Header.Revision, "case %d", index)
			outcome.Remaining = normalizeGeneratedDeleteKVs(remaining.Kvs, prefix, revisionOrdinal)
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func generatedDeletePut(
	t *testing.T,
	ctx context.Context,
	client *clientv3.Client,
	key, value string,
	lease clientv3.LeaseID,
) int64 {
	t.Helper()
	options := []clientv3.OpOption(nil)
	if lease != 0 {
		options = append(options, clientv3.WithLease(lease))
	}
	response, err := client.Put(ctx, key, value, options...)
	require.NoError(t, err)
	return response.Header.Revision
}

func generatedDeleteRequest(prefix string, spec generatedDeleteSpec) *etcdserverpb.DeleteRangeRequest {
	request := &etcdserverpb.DeleteRangeRequest{PrevKv: spec.PrevKV}
	switch spec.Shape {
	case 0:
		request.Key = []byte(prefix + "a")
	case 1:
		request.Key = []byte(prefix + "c")
	case 2:
		request.Key = []byte(prefix + "z")
	case 3:
		request.Key = []byte(prefix)
		request.RangeEnd = []byte(clientv3.GetPrefixRangeEnd(prefix))
	case 4:
		request.Key = []byte(prefix + "b")
		request.RangeEnd = []byte(prefix + "d")
	case 5:
		request.Key = []byte(prefix + "b")
		request.RangeEnd = []byte(prefix + "b")
	case 6:
		request.Key = []byte(prefix + "z")
		request.RangeEnd = []byte(prefix + "a")
	}
	return request
}

func normalizeGeneratedDeleteKVs(
	items []*mvccpb.KeyValue,
	prefix string,
	revisionOrdinal map[int64]int,
) []generatedDeleteKV {
	result := make([]generatedDeleteKV, 0, len(items))
	for _, item := range items {
		result = append(result, generatedDeleteKV{
			Key: string(item.Key[len(prefix):]), Value: string(item.Value),
			CreateRevision: revisionOrdinal[item.CreateRevision], ModRevision: revisionOrdinal[item.ModRevision],
			Version: item.Version, Leased: item.Lease != 0,
		})
	}
	return result
}
