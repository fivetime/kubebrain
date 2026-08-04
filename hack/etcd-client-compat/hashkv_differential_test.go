package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type hashKVDifferentialResult struct {
	NegativeHash           uint32
	NegativeHashRevision   int64
	NegativeHeaderDelta    int64
	CurrentHashRevisionGap int64
	CurrentHeaderDelta     int64
	NegativeDiffersCurrent bool
}

func TestHashKVDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if kubebrain == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for differential tests")
	}
	require.Equal(t,
		runHashKVDifferentialScenario(t, reference, "etcd"),
		runHashKVDifferentialScenario(t, kubebrain, "kubebrain"),
	)
}

func runHashKVDifferentialScenario(t *testing.T, endpoint, instance string) hashKVDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-hashkv-differential/%s/%d", instance, time.Now().UnixNano())
	registerPrefixCleanup(t, cli, key)

	put, err := cli.Put(ctx, key, "value")
	require.NoError(t, err)
	negative, err := cli.HashKV(ctx, endpoint, -1)
	require.NoError(t, err)
	current, err := cli.HashKV(ctx, endpoint, 0)
	require.NoError(t, err)

	return hashKVDifferentialResult{
		NegativeHash:           negative.Hash,
		NegativeHashRevision:   negative.HashRevision,
		NegativeHeaderDelta:    negative.Header.Revision - put.Header.Revision,
		CurrentHashRevisionGap: current.Header.Revision - current.HashRevision,
		CurrentHeaderDelta:     current.Header.Revision - put.Header.Revision,
		NegativeDiffersCurrent: negative.Hash != current.Hash,
	}
}
