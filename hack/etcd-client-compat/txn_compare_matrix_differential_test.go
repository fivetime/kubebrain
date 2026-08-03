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
	Name      string
	Succeeded bool
}

func TestTxnCompareMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runCompareMatrixScenario(t, reference, "etcd"),
		runCompareMatrixScenario(t, compatEndpoint(t), "kubebrain"),
	)
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
	emptyFromKey := []byte(prefix + "z")
	tests := []struct {
		name    string
		compare *etcdserverpb.Compare
	}{
		{"value-equal", valueCompare(keyA, nil, etcdserverpb.Compare_EQUAL, "same")},
		{"value-not-equal", valueCompare(keyA, nil, etcdserverpb.Compare_NOT_EQUAL, "other")},
		{"value-less", valueCompare(keyA, nil, etcdserverpb.Compare_LESS, "z")},
		{"value-greater", valueCompare(keyA, nil, etcdserverpb.Compare_GREATER, "a")},
		{"version-equal", intCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 2)},
		{"version-not-equal", intCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_NOT_EQUAL, 1)},
		{"create-equal", intCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, putA.Header.Revision)},
		{"mod-equal", intCompare(keyA, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_EQUAL, updateA.Header.Revision)},
		{"lease-equal", intCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID)},
		{"lease-not-equal-zero", intCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_NOT_EQUAL, 0)},
		{"absent-version-equal-zero", intCompare(absent, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0)},
		{"absent-version-not-equal-zero", intCompare(absent, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_NOT_EQUAL, 0)},
		{"absent-mod-less-one", intCompare(absent, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_LESS, 1)},
		{"absent-create-greater-zero", intCompare(absent, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_GREATER, 0)},
		{"absent-lease-equal-zero", intCompare(absent, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, 0)},
		{"absent-value-equal-empty", valueCompare(absent, nil, etcdserverpb.Compare_EQUAL, "")},
		{"absent-value-not-equal", valueCompare(absent, nil, etcdserverpb.Compare_NOT_EQUAL, "value")},
		{"empty-range-version-equal-zero", intCompare(emptyKey, emptyEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0)},
		{"empty-range-version-greater-zero", intCompare(emptyKey, emptyEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0)},
		{"empty-range-value-not-equal", valueCompare(emptyKey, emptyEnd, etcdserverpb.Compare_NOT_EQUAL, "value")},
		{"multi-version-greater-zero", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0)},
		{"multi-version-equal-one", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1)},
		{"multi-create-equal-first", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, putA.Header.Revision)},
		{"multi-create-less-after-update", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_LESS, updateA.Header.Revision+1)},
		{"multi-value-not-equal-missing", valueCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_NOT_EQUAL, "missing")},
		{"multi-value-equal-same", valueCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_EQUAL, "same")},
		{"multi-lease-equal-grant", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID)},
		{"multi-lease-not-equal-zero", intCompare([]byte(prefix), rangeEnd, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_NOT_EQUAL, 0)},
		{"from-key-version-greater-zero", intCompare(keyB, []byte{0}, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0)},
		{"from-key-value-equal-different", valueCompare(keyB, []byte{0}, etcdserverpb.Compare_EQUAL, "different")},
		{"from-key-empty-version-equal-zero", intCompare(emptyFromKey, []byte{0}, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0)},
		{"from-key-empty-value-not-equal", valueCompare(emptyFromKey, []byte{0}, etcdserverpb.Compare_NOT_EQUAL, "anything")},
	}

	outcomes := make([]compareMatrixOutcome, 0, len(tests))
	for _, test := range tests {
		resp, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{test.compare}})
		require.NoError(t, txnErr, test.name)
		outcomes = append(outcomes, compareMatrixOutcome{Name: test.name, Succeeded: resp.Succeeded})
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
