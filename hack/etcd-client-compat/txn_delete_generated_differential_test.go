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

type generatedTxnDeleteSpec struct {
	Shape  int
	PrevKV bool
	Nested bool
}

type generatedTxnDeleteOutcome struct {
	Code, Message   string
	Succeeded       bool
	Shape           string
	HeaderGap       int64
	DeleteHeaderGap int64
	RangeHeaderGap  int64
	NestedHeader    int
	FinalHeaderGap  int64
	Deleted         int64
	NestedSucceeded bool
	Marker          *generatedDeleteKV
	PrevKVs         []generatedDeleteKV
	StagedRemaining []generatedDeleteKV
	FinalRemaining  []generatedDeleteKV
}

func TestGeneratedTxnDeleteDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run generated Txn DeleteRange differential tests")
	}
	specs := generatedTxnDeleteSpecs()
	require.Len(t, specs, 28)
	referenceOutcome := runGeneratedTxnDeleteScenario(t, reference, "reference", specs)
	require.Equal(t, referenceOutcome, runGeneratedTxnDeleteScenario(t, compatEndpoint(t), "kubebrain", specs))
}

func TestGeneratedTxnDeleteSeedCoversEveryCombination(t *testing.T) {
	combinations := map[generatedTxnDeleteSpec]bool{}
	for _, spec := range generatedTxnDeleteSpecs() {
		require.False(t, combinations[spec], "duplicate generated Txn DeleteRange combination: %+v", spec)
		combinations[spec] = true
	}
	for shape := 0; shape < 7; shape++ {
		for _, prevKV := range []bool{false, true} {
			for _, nested := range []bool{false, true} {
				require.True(t, combinations[generatedTxnDeleteSpec{
					Shape: shape, PrevKV: prevKV, Nested: nested,
				}])
			}
		}
	}
}

func generatedTxnDeleteSpecs() []generatedTxnDeleteSpec {
	specs := make([]generatedTxnDeleteSpec, 0, 28)
	for shape := 0; shape < 7; shape++ {
		for _, prevKV := range []bool{false, true} {
			for _, nested := range []bool{false, true} {
				specs = append(specs, generatedTxnDeleteSpec{
					Shape: shape, PrevKV: prevKV, Nested: nested,
				})
			}
		}
	}
	rng := rand.New(rand.NewSource(3722))
	rng.Shuffle(len(specs), func(left, right int) { specs[left], specs[right] = specs[right], specs[left] })
	return specs
}

func runGeneratedTxnDeleteScenario(
	t *testing.T,
	endpoint, instance string,
	specs []generatedTxnDeleteSpec,
) []generatedTxnDeleteOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root := fmt.Sprintf("/a3722/generated-txn-delete/%s/%d/", instance, time.Now().UnixNano())
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Revoke(cleanupCtx, lease.ID)
		_, _ = client.Delete(cleanupCtx, root, clientv3.WithPrefix())
	})

	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	outcomes := make([]generatedTxnDeleteOutcome, 0, len(specs))
	for index, spec := range specs {
		casePrefix := fmt.Sprintf("%scase-%02d/", root, index)
		dataPrefix, marker := casePrefix+"data/", []byte(casePrefix+"marker")
		revisions := make([]int64, 7)
		revisions[1] = generatedDeletePut(t, ctx, client, dataPrefix+"a", "va", 0)
		revisions[2] = generatedDeletePut(t, ctx, client, dataPrefix+"b", "vb1", 0)
		revisions[3] = generatedDeletePut(t, ctx, client, dataPrefix+"c", "vc", lease.ID)
		revisions[4] = generatedDeletePut(t, ctx, client, dataPrefix+"d", "vd", 0)
		revisions[5] = generatedDeletePut(t, ctx, client, dataPrefix+"b", "vb2", 0)
		revisionOrdinal := make(map[int64]int, len(revisions))
		for ordinal := 1; ordinal <= 5; ordinal++ {
			revisionOrdinal[revisions[ordinal]] = ordinal
		}

		deleteRequest := generatedDeleteRequest(dataPrefix, generatedDeleteSpec{
			Shape: spec.Shape, PrevKV: spec.PrevKV,
		})
		request := generatedTxnDeleteRequest(marker, dataPrefix, deleteRequest, spec.Nested)
		response, callErr := kv.Txn(ctx, request)
		outcome := generatedTxnDeleteOutcome{
			Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		}
		if callErr == nil {
			require.NotNil(t, response.Header, "case %d", index)
			outcome.Succeeded = response.Succeeded
			outcome.HeaderGap = response.Header.Revision - revisions[5]
			revisions[6] = response.Header.Revision
			revisionOrdinal[revisions[6]] = 6
			deleted, ranged, nestedResponse, shape := generatedTxnDeleteResponses(t, response, spec.Nested, index)
			outcome.Shape = shape
			require.NotNil(t, deleted.Header, "case %d", index)
			require.NotNil(t, ranged.Header, "case %d", index)
			outcome.DeleteHeaderGap = deleted.Header.Revision - revisions[5]
			outcome.RangeHeaderGap = ranged.Header.Revision - revisions[5]
			if nestedResponse != nil {
				require.NotNil(t, nestedResponse.Header, "case %d", index)
				switch nestedResponse.Header.Revision {
				case 0:
					outcome.NestedHeader = 0
				case response.Header.Revision:
					outcome.NestedHeader = 1
				default:
					outcome.NestedHeader = -1
				}
				outcome.NestedSucceeded = nestedResponse.Succeeded
			}
			outcome.Deleted = deleted.Deleted
			outcome.PrevKVs = normalizeGeneratedDeleteKVs(deleted.PrevKvs, dataPrefix, revisionOrdinal)
			outcome.StagedRemaining = normalizeGeneratedDeleteKVs(ranged.Kvs, dataPrefix, revisionOrdinal)
		}
		final, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(casePrefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(casePrefix)),
		})
		require.NoError(t, rangeErr)
		require.NotNil(t, final.Header, "case %d", index)
		outcome.FinalHeaderGap = final.Header.Revision - revisions[5]
		for _, item := range final.Kvs {
			if string(item.Key) == string(marker) {
				normalized := normalizeGeneratedDeleteKVs([]*mvccpb.KeyValue{item}, casePrefix, revisionOrdinal)
				outcome.Marker = &normalized[0]
				continue
			}
			outcome.FinalRemaining = append(outcome.FinalRemaining,
				normalizeGeneratedDeleteKVs([]*mvccpb.KeyValue{item}, dataPrefix, revisionOrdinal)[0])
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func generatedTxnDeleteRequest(
	marker []byte,
	dataPrefix string,
	deleteRequest *etcdserverpb.DeleteRangeRequest,
	nested bool,
) *etcdserverpb.TxnRequest {
	markerOp := generatedTxnPutOp(&etcdserverpb.PutRequest{Key: marker, Value: []byte("marker")})
	deleteOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: deleteRequest,
	}}
	rangeOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{
			Key: []byte(dataPrefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(dataPrefix)),
		},
	}}
	if !nested {
		return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{markerOp, deleteOp, rangeOp}}
	}
	return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		markerOp,
		{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{deleteOp, rangeOp},
		}}},
	}}
}

func generatedTxnDeleteResponses(
	t *testing.T,
	response *etcdserverpb.TxnResponse,
	nested bool,
	caseIndex int,
) (*etcdserverpb.DeleteRangeResponse, *etcdserverpb.RangeResponse, *etcdserverpb.TxnResponse, string) {
	t.Helper()
	if !nested {
		require.Len(t, response.Responses, 3, "case %d", caseIndex)
		deleted, ranged := response.Responses[1].GetResponseDeleteRange(), response.Responses[2].GetResponseRange()
		require.NotNil(t, deleted, "case %d", caseIndex)
		require.NotNil(t, ranged, "case %d", caseIndex)
		return deleted, ranged, nil, "put,delete,range"
	}
	require.Len(t, response.Responses, 2, "case %d", caseIndex)
	nestedResponse := response.Responses[1].GetResponseTxn()
	require.NotNil(t, nestedResponse, "case %d", caseIndex)
	require.Len(t, nestedResponse.Responses, 2, "case %d", caseIndex)
	deleted := nestedResponse.Responses[0].GetResponseDeleteRange()
	ranged := nestedResponse.Responses[1].GetResponseRange()
	require.NotNil(t, deleted, "case %d", caseIndex)
	require.NotNil(t, ranged, "case %d", caseIndex)
	return deleted, ranged, nestedResponse, "put,txn(delete,range)"
}
