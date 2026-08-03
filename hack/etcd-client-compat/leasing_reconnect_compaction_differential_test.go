package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingReconnectCompactionOutcome struct {
	InitialMissing    bool
	TrafficDiscarded  bool
	CompactionApplied bool
	RecoveredValue    string
	DirectValue       string
}

func TestLeasingReconnectCompactionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing reconnect compaction differential tests")
	}

	require.Equal(t,
		runLeasingReconnectCompactionScenario(t, reference, "etcd"),
		runLeasingReconnectCompactionScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingReconnectCompactionScenario(t *testing.T, endpoint, instance string) leasingReconnectCompactionOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	first, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-reconnect-compact/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "data"
	firstKV, closeFirst, err := leasing.NewKV(first, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeSecond)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = second.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	initial, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, initial.Kvs)

	bridge.Blackhole()
	_, err = second.Put(ctx, prefix+"advance", "one")
	require.NoError(t, err)
	advanced, err := second.Put(ctx, prefix+"advance", "two")
	require.NoError(t, err)
	_, err = second.Compact(ctx, advanced.Header.Revision)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > 0
	}, 5*time.Second, 10*time.Millisecond)
	trafficDiscarded := bridge.DroppedBytes() > 0
	bridge.Unblackhole()

	_, err = secondKV.Put(ctx, key, "recovered")
	require.NoError(t, err)
	var recovered *clientv3.GetResponse
	require.Eventually(t, func() bool {
		response, getErr := firstKV.Get(ctx, key)
		if getErr != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != "recovered" {
			return false
		}
		recovered = response
		return true
	}, 10*time.Second, 20*time.Millisecond)
	direct, err := second.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, direct.Kvs, 1)

	return leasingReconnectCompactionOutcome{
		InitialMissing:    len(initial.Kvs) == 0,
		TrafficDiscarded:  trafficDiscarded,
		CompactionApplied: advanced.Header.Revision > 0,
		RecoveredValue:    string(recovered.Kvs[0].Value),
		DirectValue:       string(direct.Kvs[0].Value),
	}
}
