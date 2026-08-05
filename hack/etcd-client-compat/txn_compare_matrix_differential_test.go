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

type compareMatrixOutcome struct {
	Name          string
	Succeeded     bool
	WantSucceeded bool
}

func TestTxnCompareMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runCompareMatrixScenario(t, reference, "etcd")
	for _, outcome := range referenceOutcomes {
		require.Equal(t, outcome.WantSucceeded, outcome.Succeeded, "official etcd outcome for %s", outcome.Name)
	}
	require.Equal(t, referenceOutcomes, runCompareMatrixScenario(t, compatEndpoint(t), "kubebrain"))
}

func runCompareMatrixScenario(t *testing.T, endpoint, instance string) []compareMatrixOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-compare-matrix/%s/%d/", instance, time.Now().UnixNano())
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
	})

	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	})

	keyA := []byte(prefix + "a")
	keyB := []byte(prefix + "b")
	keyLease := []byte(prefix + "leased")
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("same")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyB, Value: []byte("different")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyLease, Value: []byte("leased"), Lease: grant.ID})
	require.NoError(t, err)
	updateA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("same")})
	require.NoError(t, err)

	absent := []byte(prefix + "absent")
	emptyPrefix := prefix + "empty/"
	emptyKey := []byte(emptyPrefix)
	emptyEnd := []byte(clientv3.GetPrefixRangeEnd(emptyPrefix))
	// A from-key compare is unbounded. Start at the greatest one-byte key so the
	// empty-range cases remain isolated even when the endpoint contains data
	// outside this test's prefix (as a shared DBaaS endpoint normally does).
	emptyFromKey := []byte{0xff}
	tests := []struct {
		name          string
		compare       *etcdserverpb.Compare
		wantSucceeded bool
	}{
		{"value-equal", valueCompare(keyA, nil, etcdserverpb.Compare_EQUAL, "same"), true},
		{"value-not-equal", valueCompare(keyA, nil, etcdserverpb.Compare_NOT_EQUAL, "same"), false},
		{"value-less", valueCompare(keyA, nil, etcdserverpb.Compare_LESS, "z"), true},
		{"value-greater", valueCompare(keyA, nil, etcdserverpb.Compare_GREATER, "z"), false},
		{"version-equal", intCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 2), true},
		{"version-not-equal", intCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_NOT_EQUAL, 2), false},
		{"version-less", intCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_LESS, 3), true},
		{"version-greater", intCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 3), false},
		{"create-equal", intCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, putA.Header.Revision), true},
		{"create-not-equal", intCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_NOT_EQUAL, putA.Header.Revision), false},
		{"create-less", intCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_LESS, putA.Header.Revision+1), true},
		{"create-greater", intCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_GREATER, putA.Header.Revision+1), false},
		{"mod-equal", intCompare(keyA, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_EQUAL, updateA.Header.Revision), true},
		{"mod-not-equal", intCompare(keyA, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_NOT_EQUAL, updateA.Header.Revision), false},
		{"mod-less", intCompare(keyA, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_LESS, updateA.Header.Revision+1), true},
		{"mod-greater", intCompare(keyA, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_GREATER, updateA.Header.Revision+1), false},
		{"lease-equal", intCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID), true},
		{"lease-not-equal", intCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_NOT_EQUAL, grant.ID), false},
		{"lease-less", intCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_LESS, grant.ID+1), true},
		{"lease-greater", intCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_GREATER, grant.ID+1), false},
		{"absent-version-equal-zero", intCompare(absent, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0), true},
		{"absent-version-not-equal-zero", intCompare(absent, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_NOT_EQUAL, 0), false},
		{"absent-mod-less-one", intCompare(absent, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_LESS, 1), true},
		{"absent-create-greater-zero", intCompare(absent, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_GREATER, 0), false},
		{"absent-lease-equal-zero", intCompare(absent, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, 0), true},
		{"absent-value-equal-empty", valueCompare(absent, nil, etcdserverpb.Compare_EQUAL, ""), false},
		{"absent-value-not-equal", valueCompare(absent, nil, etcdserverpb.Compare_NOT_EQUAL, "value"), false},
		{"empty-range-version-equal-zero", intCompare(emptyKey, emptyEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0), true},
		{"empty-range-version-greater-zero", intCompare(emptyKey, emptyEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0), false},
		{"empty-range-value-not-equal", valueCompare(emptyKey, emptyEnd, etcdserverpb.Compare_NOT_EQUAL, "value"), false},
		{"multi-version-greater-zero", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0), true},
		{"multi-version-equal-one", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1), false},
		{"multi-create-equal-first", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, putA.Header.Revision), false},
		{"multi-create-less-after-update", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_LESS, updateA.Header.Revision+1), true},
		{"multi-value-not-equal-missing", valueCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_NOT_EQUAL, "missing"), true},
		{"multi-value-equal-same", valueCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_EQUAL, "same"), false},
		{"multi-lease-equal-grant", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID), false},
		{"multi-lease-not-equal-zero", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_NOT_EQUAL, 0), false},
		{"from-key-version-greater-zero", intCompare(keyB, []byte{0}, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0), true},
		{"from-key-value-equal-different", valueCompare(keyB, []byte{0}, etcdserverpb.Compare_EQUAL, "different"), false},
		{"from-key-empty-version-equal-zero", intCompare(emptyFromKey, []byte{0}, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0), true},
		{"from-key-empty-value-not-equal", valueCompare(emptyFromKey, []byte{0}, etcdserverpb.Compare_NOT_EQUAL, "anything"), false},
	}
	const pointMatrixSize = 5 * 4
	require.GreaterOrEqual(t, len(tests), pointMatrixSize)
	pointCoverage := make(map[[2]int32]bool, pointMatrixSize)
	for _, test := range tests[:pointMatrixSize] {
		pointCoverage[[2]int32{int32(test.compare.Target), int32(test.compare.Result)}] = true
	}
	for _, target := range []etcdserverpb.Compare_CompareTarget{
		etcdserverpb.Compare_VALUE,
		etcdserverpb.Compare_VERSION,
		etcdserverpb.Compare_CREATE,
		etcdserverpb.Compare_MOD,
		etcdserverpb.Compare_LEASE,
	} {
		for _, result := range []etcdserverpb.Compare_CompareResult{
			etcdserverpb.Compare_EQUAL,
			etcdserverpb.Compare_NOT_EQUAL,
			etcdserverpb.Compare_LESS,
			etcdserverpb.Compare_GREATER,
		} {
			require.True(t, pointCoverage[[2]int32{int32(target), int32(result)}],
				"point compare matrix must cover target=%s result=%s", target, result)
		}
	}

	outcomes := make([]compareMatrixOutcome, 0, len(tests))
	for _, test := range tests {
		resp, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{test.compare}})
		require.NoError(t, txnErr, test.name)
		outcomes = append(outcomes, compareMatrixOutcome{
			Name: test.name, Succeeded: resp.Succeeded, WantSucceeded: test.wantSucceeded,
		})
	}
	return outcomes
}

func valueCompare(key, rangeEnd []byte, result etcdserverpb.Compare_CompareResult, value string) *etcdserverpb.Compare {
	return &etcdserverpb.Compare{
		Key: key, RangeEnd: rangeEnd, Result: result, Target: etcdserverpb.Compare_VALUE,
		TargetUnion: &etcdserverpb.Compare_Value{Value: []byte(value)},
	}
}

func intCompare(
	key, rangeEnd []byte,
	target etcdserverpb.Compare_CompareTarget,
	result etcdserverpb.Compare_CompareResult,
	value int64,
) *etcdserverpb.Compare {
	compare := &etcdserverpb.Compare{Key: key, RangeEnd: rangeEnd, Result: result, Target: target}
	switch target {
	case etcdserverpb.Compare_VERSION:
		compare.TargetUnion = &etcdserverpb.Compare_Version{Version: value}
	case etcdserverpb.Compare_CREATE:
		compare.TargetUnion = &etcdserverpb.Compare_CreateRevision{CreateRevision: value}
	case etcdserverpb.Compare_MOD:
		compare.TargetUnion = &etcdserverpb.Compare_ModRevision{ModRevision: value}
	case etcdserverpb.Compare_LEASE:
		compare.TargetUnion = &etcdserverpb.Compare_Lease{Lease: value}
	}
	return compare
}
