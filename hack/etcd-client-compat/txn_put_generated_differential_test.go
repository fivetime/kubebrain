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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type generatedTxnPutSpec struct {
	Mode   int
	Exists bool
	PrevKV bool
	Nested bool
}

type generatedTxnPutOutcome struct {
	Code, Message   string
	Succeeded       bool
	Shape           string
	HeaderGap       int64
	PutHeaderGap    int64
	RangeHeaderGap  int64
	FinalHeaderGap  int64
	FinalCount      int
	PrevKV          *generatedPutKV
	Staged          *generatedPutKV
	Final           *generatedPutKV
	MarkerCommitted bool
}

func TestGeneratedTxnPutDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run generated Txn Put differential tests")
	}
	specs := generatedTxnPutSpecs()
	require.Len(t, specs, 40)
	referenceOutcome := runGeneratedTxnPutScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcome, runGeneratedTxnPutScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedTxnPutSeedCoversEveryCombination(t *testing.T) {
	combinations := map[generatedTxnPutSpec]bool{}
	for _, spec := range generatedTxnPutSpecs() {
		require.False(t, combinations[spec], "duplicate generated Txn Put combination: %+v", spec)
		combinations[spec] = true
	}
	for mode := 0; mode < 5; mode++ {
		for _, exists := range []bool{false, true} {
			for _, prevKV := range []bool{false, true} {
				for _, nested := range []bool{false, true} {
					require.True(t, combinations[generatedTxnPutSpec{
						Mode: mode, Exists: exists, PrevKV: prevKV, Nested: nested,
					}])
				}
			}
		}
	}
}

func generatedTxnPutSpecs() []generatedTxnPutSpec {
	specs := make([]generatedTxnPutSpec, 0, 40)
	for mode := 0; mode < 5; mode++ {
		for _, exists := range []bool{false, true} {
			for _, prevKV := range []bool{false, true} {
				for _, nested := range []bool{false, true} {
					specs = append(specs, generatedTxnPutSpec{
						Mode: mode, Exists: exists, PrevKV: prevKV, Nested: nested,
					})
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(3721))
	rng.Shuffle(len(specs), func(left, right int) { specs[left], specs[right] = specs[right], specs[left] })
	return specs
}

func runGeneratedTxnPutScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedTxnPutSpec,
) []generatedTxnPutOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root := fmt.Sprintf("/a3721/generated-txn-put/%s/%d/", instance, time.Now().UnixNano())
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
	outcomes := make([]generatedTxnPutOutcome, 0, len(specs))
	for index, spec := range specs {
		prefix := fmt.Sprintf("%scase-%02d/", root, index)
		key, marker := []byte(prefix+"key"), []byte(prefix+"marker")
		base, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "base"), Value: []byte("base")})
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

		put := generatedTxnPutRequest(key, spec, int64(leaseB.ID))
		request := generatedTxnRequest(marker, key, put, spec.Nested)
		response, callErr := kv.Txn(ctx, request)
		outcome := generatedTxnPutOutcome{
			Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		}
		if callErr == nil {
			require.NotNil(t, response.Header, "case %d", index)
			outcome.Succeeded = response.Succeeded
			outcome.HeaderGap = response.Header.Revision - lastRevision
			revisionOrdinal[response.Header.Revision] = len(revisionOrdinal) + 1
			putResponse, rangeResponse, shape := generatedTxnPutResponses(t, response, spec.Nested, index)
			outcome.Shape = shape
			require.NotNil(t, putResponse.Header, "case %d", index)
			require.NotNil(t, rangeResponse.Header, "case %d", index)
			outcome.PutHeaderGap = putResponse.Header.Revision - lastRevision
			outcome.RangeHeaderGap = rangeResponse.Header.Revision - lastRevision
			outcome.PrevKV = normalizeGeneratedPutKV(putResponse.PrevKv, revisionOrdinal, leaseOrdinal)
			require.LessOrEqual(t, len(rangeResponse.Kvs), 1, "case %d", index)
			if len(rangeResponse.Kvs) == 1 {
				outcome.Staged = normalizeGeneratedPutKV(rangeResponse.Kvs[0], revisionOrdinal, leaseOrdinal)
			}
		}
		final, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
		require.NoError(t, rangeErr)
		require.NotNil(t, final.Header, "case %d", index)
		outcome.FinalHeaderGap = final.Header.Revision - lastRevision
		outcome.FinalCount = len(final.Kvs)
		for _, item := range final.Kvs {
			switch string(item.Key) {
			case string(marker):
				outcome.MarkerCommitted = true
			case string(key):
				outcome.Final = normalizeGeneratedPutKV(item, revisionOrdinal, leaseOrdinal)
			}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func generatedTxnPutRequest(key []byte, spec generatedTxnPutSpec, leaseB int64) *etcdserverpb.PutRequest {
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
	}
	return request
}

func generatedTxnRequest(
	marker, key []byte,
	put *etcdserverpb.PutRequest,
	nested bool,
) *etcdserverpb.TxnRequest {
	markerOp := generatedTxnPutOp(&etcdserverpb.PutRequest{Key: marker, Value: []byte("marker")})
	putOp := generatedTxnPutOp(put)
	rangeOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: key},
	}}
	if !nested {
		return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{markerOp, putOp, rangeOp}}
	}
	return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		markerOp,
		{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{putOp, rangeOp},
		}}},
	}}
}

func generatedTxnPutOp(request *etcdserverpb.PutRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: request}}
}

func generatedTxnPutResponses(
	t *testing.T,
	response *etcdserverpb.TxnResponse,
	nested bool,
	caseIndex int,
) (*etcdserverpb.PutResponse, *etcdserverpb.RangeResponse, string) {
	t.Helper()
	if !nested {
		require.Len(t, response.Responses, 3, "case %d", caseIndex)
		putResponse, rangeResponse := response.Responses[1].GetResponsePut(), response.Responses[2].GetResponseRange()
		require.NotNil(t, putResponse, "case %d", caseIndex)
		require.NotNil(t, rangeResponse, "case %d", caseIndex)
		return putResponse, rangeResponse, "put,put,range"
	}
	require.Len(t, response.Responses, 2, "case %d", caseIndex)
	nestedResponse := response.Responses[1].GetResponseTxn()
	require.NotNil(t, nestedResponse, "case %d", caseIndex)
	require.Len(t, nestedResponse.Responses, 2, "case %d", caseIndex)
	putResponse := nestedResponse.Responses[0].GetResponsePut()
	rangeResponse := nestedResponse.Responses[1].GetResponseRange()
	require.NotNil(t, putResponse, "case %d", caseIndex)
	require.NotNil(t, rangeResponse, "case %d", caseIndex)
	return putResponse, rangeResponse, "put,txn(put,range)"
}
