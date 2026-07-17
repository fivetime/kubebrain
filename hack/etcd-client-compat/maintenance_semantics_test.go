package compat

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultEtcdBackendQuota int64 = 2 * 1024 * 1024 * 1024

func TestMaintenanceStatusMetadataMatchesReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if kubebrain == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for differential tests")
	}

	referenceStatus := maintenanceStatus(t, reference)
	kubebrainStatus := maintenanceStatus(t, kubebrain)
	require.Equal(t, referenceStatus.DbSizeQuota, kubebrainStatus.DbSizeQuota)
	require.Equal(t, defaultEtcdBackendQuota, kubebrainStatus.DbSizeQuota)
	require.Positive(t, referenceStatus.RaftTerm)
	require.Positive(t, kubebrainStatus.RaftTerm)
	require.Equal(t, referenceStatus.RaftTerm, referenceStatus.Header.RaftTerm)
	require.Equal(t, kubebrainStatus.RaftTerm, kubebrainStatus.Header.RaftTerm)
	for _, term := range responseHeaderRaftTerms(t, reference) {
		require.Positive(t, term)
	}
	for _, term := range responseHeaderRaftTerms(t, kubebrain) {
		require.Positive(t, term)
	}
	referenceDefragHeaderNil := defragmentHeaderIsNil(t, reference)
	kubebrainDefragHeaderNil := defragmentHeaderIsNil(t, kubebrain)
	require.Equal(t, referenceDefragHeaderNil, kubebrainDefragHeaderNil)
	require.True(t, kubebrainDefragHeaderNil)
}

func defragmentHeaderIsNil(t *testing.T, endpoint string) bool {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.Defragment(ctx, endpoint)
	require.NoError(t, err)
	return resp.Header == nil
}

func responseHeaderRaftTerms(t *testing.T, endpoint string) []uint64 {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, "/dbaas-maintenance/header-term")
	require.NoError(t, err)
	key := "/dbaas-maintenance/header-term-watch"
	watchCh := cli.Watch(ctx, key, clientv3.WithCreatedNotify())
	created := <-watchCh
	require.NoError(t, created.Err())
	require.True(t, created.Created)
	_, err = cli.Put(ctx, key, "1")
	require.NoError(t, err)
	event := <-watchCh
	require.NoError(t, event.Err())
	require.Len(t, event.Events, 1)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})
	return []uint64{resp.Header.RaftTerm, created.Header.RaftTerm, event.Header.RaftTerm}
}

func maintenanceStatus(t *testing.T, endpoint string) *clientv3.StatusResponse {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.Status(ctx, endpoint)
	require.NoError(t, err)
	return resp
}

// TestMaintenanceHashKVSemantics exercises KubeBrain through etcd's official
// client. Hash values are backend-layout-specific, so the compatibility
// contract is stability and data sensitivity rather than numeric equality with
// etcd's bbolt hash.
func TestMaintenanceHashKVSemantics(t *testing.T) {
	endpoint := compatEndpoint()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testPrefix(t) + "/hash-key"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})

	put1, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	rev1 := put1.Header.Revision

	hash1, err := cli.HashKV(ctx, endpoint, rev1)
	require.NoError(t, err)
	require.Equal(t, rev1, hash1.HashRevision)
	hash1Again, err := cli.HashKV(ctx, endpoint, rev1)
	require.NoError(t, err)
	require.Equal(t, hash1.Hash, hash1Again.Hash)
	require.Equal(t, rev1, hash1Again.HashRevision)

	_, err = cli.Put(ctx, key, "v2")
	require.NoError(t, err)
	current, err := cli.HashKV(ctx, endpoint, 0)
	require.NoError(t, err)
	require.NotEqual(t, hash1.Hash, current.Hash)
	require.Equal(t, current.Header.Revision, current.HashRevision)

	negative, err := cli.HashKV(ctx, endpoint, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), negative.HashRevision)
	require.NotEqual(t, current.Hash, negative.Hash)

	historical, err := cli.HashKV(ctx, endpoint, rev1)
	require.NoError(t, err)
	require.Equal(t, hash1.Hash, historical.Hash)
	require.Equal(t, rev1, historical.HashRevision)
}

func TestMaintenanceHashKVMatchesAcrossMembers(t *testing.T) {
	endpoint := compatEndpoint()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	members, err := cli.MemberList(ctx)
	require.NoError(t, err)
	endpoints := make([]string, 0, len(members.Members))
	for _, member := range members.Members {
		endpoints = append(endpoints, member.ClientURLs...)
	}
	sort.Strings(endpoints)
	require.NotEmpty(t, endpoints)

	key := testPrefix(t) + "/member-hash"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})
	put, err := cli.Put(ctx, key, "member-consistent")
	require.NoError(t, err)

	var wantHash, wantCluster uint64
	memberIDs := map[uint64]struct{}{}
	for i, ep := range endpoints {
		statusResp, err := cli.Status(ctx, ep)
		require.NoError(t, err, "status %s", ep)
		hashResp, err := cli.HashKV(ctx, ep, put.Header.Revision)
		require.NoError(t, err, "hashkv %s", ep)
		if i == 0 {
			wantHash = uint64(hashResp.Hash)
			wantCluster = statusResp.Header.ClusterId
		} else {
			require.Equal(t, wantHash, uint64(hashResp.Hash), "hash mismatch at %s", ep)
			require.Equal(t, wantCluster, statusResp.Header.ClusterId, "cluster ID mismatch at %s", ep)
		}
		memberIDs[statusResp.Header.MemberId] = struct{}{}
	}
	require.Len(t, memberIDs, len(endpoints), "each configured endpoint must identify its serving member")
}
