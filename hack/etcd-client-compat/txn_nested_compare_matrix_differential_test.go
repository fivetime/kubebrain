package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type nestedCompareMatrixOutcome struct {
	Name            string
	NestedSucceeded bool
	Marker          string
	WantSucceeded   bool
}

func TestTxnNestedCompareMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runNestedCompareMatrixScenario(t, reference, "etcd")
	for _, outcome := range referenceOutcomes {
		require.Equal(t, outcome.WantSucceeded, outcome.NestedSucceeded,
			"official etcd nested outcome for %s", outcome.Name)
		require.Equal(t, map[bool]string{true: "success", false: "failure"}[outcome.WantSucceeded], outcome.Marker,
			"official etcd persisted branch for %s", outcome.Name)
	}
	require.Equal(t, referenceOutcomes, runNestedCompareMatrixScenario(t, compatEndpoint(t), "kubebrain"))
}

func runNestedCompareMatrixScenario(t *testing.T, endpoint, instance string) []nestedCompareMatrixOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-nested-compare-matrix/%s/%d/", instance, time.Now().UnixNano())
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix), RangeEnd: rangeEnd})
	})

	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	})

	type compareCase struct {
		name    string
		target  etcdserverpb.Compare_CompareTarget
		result  etcdserverpb.Compare_CompareResult
		succeed bool
	}
	cases := make([]compareCase, 0, 20)
	for _, target := range []etcdserverpb.Compare_CompareTarget{
		etcdserverpb.Compare_VALUE,
		etcdserverpb.Compare_VERSION,
		etcdserverpb.Compare_CREATE,
		etcdserverpb.Compare_MOD,
		etcdserverpb.Compare_LEASE,
	} {
		for index, result := range []etcdserverpb.Compare_CompareResult{
			etcdserverpb.Compare_EQUAL,
			etcdserverpb.Compare_NOT_EQUAL,
			etcdserverpb.Compare_LESS,
			etcdserverpb.Compare_GREATER,
		} {
			cases = append(cases, compareCase{
				name:   fmt.Sprintf("%s-%s", strings.ToLower(target.String()), strings.ToLower(result.String())),
				target: target, result: result, succeed: index%2 == 0,
			})
		}
	}

	outcomes := make([]nestedCompareMatrixOutcome, 0, len(cases))
	for index, test := range cases {
		key := []byte(fmt.Sprintf("%s%02d-key", prefix, index))
		marker := []byte(fmt.Sprintf("%s%02d-marker", prefix, index))
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("m"), Lease: grant.ID})
		require.NoError(t, putErr, test.name)

		var compare *etcdserverpb.Compare
		if test.target == etcdserverpb.Compare_VALUE {
			expected := "z"
			if test.result == etcdserverpb.Compare_EQUAL || test.result == etcdserverpb.Compare_NOT_EQUAL {
				expected = "m"
			}
			compare = valueCompare(key, nil, test.result, expected)
		} else {
			actual := int64(1)
			switch test.target {
			case etcdserverpb.Compare_CREATE, etcdserverpb.Compare_MOD:
				actual = put.Header.Revision
			case etcdserverpb.Compare_LEASE:
				actual = grant.ID
			}
			expected := actual
			if test.result == etcdserverpb.Compare_LESS || test.result == etcdserverpb.Compare_GREATER {
				expected++
			}
			compare = intCompare(key, nil, test.target, test.result, expected)
		}

		nested := &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{compare},
			Success: []*etcdserverpb.RequestOp{putRequestOp(marker, "success")},
			Failure: []*etcdserverpb.RequestOp{putRequestOp(marker, "failure")},
		}
		response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: nested}}},
		})
		require.NoError(t, txnErr, test.name)
		require.True(t, response.Succeeded, test.name)
		require.Len(t, response.Responses, 1, test.name)
		nestedResponse := response.Responses[0].GetResponseTxn()
		require.NotNil(t, nestedResponse, test.name)
		require.Len(t, nestedResponse.Responses, 1, test.name)
		require.NotNil(t, nestedResponse.Responses[0].GetResponsePut(), test.name)

		markerResponse, getErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: marker})
		require.NoError(t, getErr, test.name)
		require.Len(t, markerResponse.Kvs, 1, test.name)
		outcomes = append(outcomes, nestedCompareMatrixOutcome{
			Name: test.name, NestedSucceeded: nestedResponse.Succeeded, Marker: string(markerResponse.Kvs[0].Value),
			WantSucceeded: test.succeed,
		})
	}
	return outcomes
}
