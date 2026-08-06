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

type generatedPutSpec struct {
	Mode   int
	Exists bool
	PrevKV bool
}

type generatedPutKV struct {
	Value                       string
	CreateRevision, ModRevision int
	Version                     int64
	Lease                       int
}

type generatedPutOutcome struct {
	Code, Message  string
	HeaderGap      int64
	FinalHeaderGap int64
	PrevKV         *generatedPutKV
	Final          *generatedPutKV
}

func TestGeneratedPutDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run generated Put differential tests")
	}
	specs := generatedPutSpecs()
	require.Len(t, specs, 32)
	referenceOutcome := runGeneratedPutScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcome, runGeneratedPutScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedPutSeedCoversEveryCombination(t *testing.T) {
	combinations := map[generatedPutSpec]bool{}
	for _, spec := range generatedPutSpecs() {
		require.False(t, combinations[spec], "duplicate generated Put combination: %+v", spec)
		combinations[spec] = true
	}
	for mode := 0; mode < 8; mode++ {
		for _, exists := range []bool{false, true} {
			for _, prevKV := range []bool{false, true} {
				require.True(t, combinations[generatedPutSpec{Mode: mode, Exists: exists, PrevKV: prevKV}])
			}
		}
	}
}

func generatedPutSpecs() []generatedPutSpec {
	specs := make([]generatedPutSpec, 0, 32)
	for mode := 0; mode < 8; mode++ {
		for _, exists := range []bool{false, true} {
			for _, prevKV := range []bool{false, true} {
				specs = append(specs, generatedPutSpec{Mode: mode, Exists: exists, PrevKV: prevKV})
			}
		}
	}
	rng := rand.New(rand.NewSource(3720))
	rng.Shuffle(len(specs), func(left, right int) { specs[left], specs[right] = specs[right], specs[left] })
	return specs
}

func runGeneratedPutScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedPutSpec,
) []generatedPutOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root := fmt.Sprintf("/a3720/generated-put/%s/%d/", instance, time.Now().UnixNano())
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Revoke(cleanupCtx, leaseA.ID)
		_, _ = client.Revoke(cleanupCtx, leaseB.ID)
		_, _ = client.Delete(cleanupCtx, root, clientv3.WithPrefix())
	})
	leaseOrdinal := map[int64]int{int64(leaseA.ID): 1, int64(leaseB.ID): 2}
	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	outcomes := make([]generatedPutOutcome, 0, len(specs))
	for index, spec := range specs {
		prefix := fmt.Sprintf("%scase-%02d/", root, index)
		key, marker := []byte(prefix+"key"), []byte(prefix+"marker")
		base, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: marker, Value: []byte("base")})
		require.NoError(t, err)
		lastRevision := base.Header.Revision
		revisionOrdinal := map[int64]int{}
		if spec.Exists {
			seed, seedErr := kv.Put(ctx, &etcdserverpb.PutRequest{
				Key: key, Value: []byte("seed"), Lease: int64(leaseA.ID),
			})
			require.NoError(t, seedErr)
			lastRevision = seed.Header.Revision
			revisionOrdinal[lastRevision] = 1
		}

		request := generatedPutRequest(key, spec, int64(leaseB.ID))
		response, callErr := kv.Put(ctx, request)
		outcome := generatedPutOutcome{Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message()}
		if callErr == nil {
			require.NotNil(t, response.Header, "case %d", index)
			outcome.HeaderGap = response.Header.Revision - lastRevision
			revisionOrdinal[response.Header.Revision] = len(revisionOrdinal) + 1
			outcome.PrevKV = normalizeGeneratedPutKV(response.PrevKv, revisionOrdinal, leaseOrdinal)
		}
		final, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		require.NotNil(t, final.Header, "case %d", index)
		outcome.FinalHeaderGap = final.Header.Revision - lastRevision
		require.LessOrEqual(t, len(final.Kvs), 1)
		if len(final.Kvs) == 1 {
			outcome.Final = normalizeGeneratedPutKV(final.Kvs[0], revisionOrdinal, leaseOrdinal)
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func generatedPutRequest(key []byte, spec generatedPutSpec, leaseB int64) *etcdserverpb.PutRequest {
	request := &etcdserverpb.PutRequest{Key: key, PrevKv: spec.PrevKV}
	switch spec.Mode {
	case 0:
		request.Value = []byte("regular")
	case 1:
		request.Value, request.Lease = []byte("leased"), leaseB
	case 2:
		request.IgnoreValue = true
	case 3:
		request.IgnoreValue, request.Lease = true, leaseB
	case 4:
		request.Value, request.IgnoreLease = []byte("ignore-lease"), true
	case 5:
		request.Value, request.IgnoreValue = []byte("invalid-value"), true
	case 6:
		request.Value, request.Lease, request.IgnoreLease = []byte("invalid-lease"), leaseB, true
	case 7:
		request.Value, request.Lease = []byte("invalid-both"), leaseB
		request.IgnoreValue, request.IgnoreLease = true, true
	}
	return request
}

func normalizeGeneratedPutKV(
	item *mvccpb.KeyValue,
	revisionOrdinal map[int64]int,
	leaseOrdinal map[int64]int,
) *generatedPutKV {
	if item == nil {
		return nil
	}
	return &generatedPutKV{
		Value:          string(item.Value),
		CreateRevision: revisionOrdinal[item.CreateRevision], ModRevision: revisionOrdinal[item.ModRevision],
		Version: item.Version, Lease: leaseOrdinal[item.Lease],
	}
}
