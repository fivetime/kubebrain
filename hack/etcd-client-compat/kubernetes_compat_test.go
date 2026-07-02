package compat

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
)

func compatEndpoint() string {
	if endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	if endpoint := os.Getenv("ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return "127.0.0.1:3379"
}

func newKubernetesClient(t *testing.T) *kubernetes.Client {
	t.Helper()
	cli, err := kubernetes.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	return cli
}

func testPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("/registry/etcd-client-compat/%s/%d", t.Name(), time.Now().UnixNano())
}

func cleanupPrefix(t *testing.T, cli *kubernetes.Client, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	})
}

func TestKubernetesClientObjectLifecycle(t *testing.T) {
	cli := newKubernetesClient(t)
	prefix := testPrefix(t)
	cleanupPrefix(t, cli, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := prefix + "/pod-a"

	count, err := cli.Kubernetes.Count(ctx, prefix, kubernetes.CountOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	createResp, err := cli.Kubernetes.OptimisticPut(ctx, key, []byte("v1"), 0, kubernetes.PutOptions{})
	require.NoError(t, err)
	require.True(t, createResp.Succeeded)
	require.Greater(t, createResp.Revision, int64(0))

	getResp, err := cli.Kubernetes.Get(ctx, key, kubernetes.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, getResp.KV)
	require.Equal(t, "v1", string(getResp.KV.Value))
	require.Greater(t, getResp.KV.ModRevision, int64(0))

	conflictResp, err := cli.Kubernetes.OptimisticPut(ctx, key, []byte("conflict"), 0, kubernetes.PutOptions{GetOnFailure: true})
	require.NoError(t, err)
	require.False(t, conflictResp.Succeeded)
	require.NotNil(t, conflictResp.KV)
	require.Equal(t, "v1", string(conflictResp.KV.Value))

	updateResp, err := cli.Kubernetes.OptimisticPut(ctx, key, []byte("v2"), getResp.KV.ModRevision, kubernetes.PutOptions{GetOnFailure: true})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)
	require.Greater(t, updateResp.Revision, getResp.KV.ModRevision)

	updatedResp, err := cli.Kubernetes.Get(ctx, key, kubernetes.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, updatedResp.KV)
	require.Equal(t, "v2", string(updatedResp.KV.Value))

	staleDeleteResp, err := cli.Kubernetes.OptimisticDelete(ctx, key, getResp.KV.ModRevision, kubernetes.DeleteOptions{GetOnFailure: true})
	require.NoError(t, err)
	require.False(t, staleDeleteResp.Succeeded)
	require.NotNil(t, staleDeleteResp.KV)
	require.Equal(t, "v2", string(staleDeleteResp.KV.Value))

	deleteResp, err := cli.Kubernetes.OptimisticDelete(ctx, key, updatedResp.KV.ModRevision, kubernetes.DeleteOptions{})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)

	finalResp, err := cli.Kubernetes.Get(ctx, key, kubernetes.GetOptions{})
	require.NoError(t, err)
	require.Nil(t, finalResp.KV)
}

func TestKubernetesClientListPaginationAndHistoricalRead(t *testing.T) {
	cli := newKubernetesClient(t)
	prefix := testPrefix(t)
	cleanupPrefix(t, cli, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keys := []string{
		prefix + "/a",
		prefix + "/b",
		prefix + "/c",
		prefix + "/d",
		prefix + "/e",
	}
	var createdRevision int64
	var firstKeyModRevision int64
	for i, key := range keys {
		resp, err := cli.Kubernetes.OptimisticPut(ctx, key, []byte(fmt.Sprintf("value-%d", i)), 0, kubernetes.PutOptions{})
		require.NoError(t, err)
		require.True(t, resp.Succeeded)
		createdRevision = resp.Revision
		if i == 0 {
			created, err := cli.Kubernetes.Get(ctx, key, kubernetes.GetOptions{})
			require.NoError(t, err)
			require.NotNil(t, created.KV)
			firstKeyModRevision = created.KV.ModRevision
		}
	}

	count, err := cli.Kubernetes.Count(ctx, prefix, kubernetes.CountOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(keys)), count)

	var listed []string
	var cont string
	for {
		resp, err := cli.Kubernetes.List(ctx, prefix, kubernetes.ListOptions{Limit: 2, Continue: cont})
		require.NoError(t, err)
		require.Equal(t, int64(len(keys)-len(listed)), resp.Count)
		for _, kv := range resp.Kvs {
			listed = append(listed, string(kv.Key))
		}
		if len(resp.Kvs) < 2 {
			break
		}
		cont = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	require.Equal(t, keys, listed)

	_, err = cli.Kubernetes.OptimisticPut(ctx, keys[0], []byte("updated-after-list-revision"), firstKeyModRevision, kubernetes.PutOptions{})
	require.NoError(t, err)

	historicalResp, err := cli.Kubernetes.Get(ctx, keys[0], kubernetes.GetOptions{Revision: createdRevision})
	require.NoError(t, err)
	require.NotNil(t, historicalResp.KV)
	require.Equal(t, "value-0", string(historicalResp.KV.Value))
}

func TestKubernetesClientWatchAndLease(t *testing.T) {
	cli := newKubernetesClient(t)
	prefix := testPrefix(t)
	cleanupPrefix(t, cli, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	leaseResp, err := cli.Grant(ctx, 30)
	require.NoError(t, err)

	leasedKey := prefix + "/leased"
	putResp, err := cli.Kubernetes.OptimisticPut(ctx, leasedKey, []byte("leased-value"), 0, kubernetes.PutOptions{LeaseID: leaseResp.ID})
	require.NoError(t, err)
	require.True(t, putResp.Succeeded)

	ttlResp, err := cli.TimeToLive(ctx, leaseResp.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Contains(t, byteSlicesToStrings(ttlResp.Keys), leasedKey)

	keepAliveResp, err := cli.KeepAliveOnce(ctx, leaseResp.ID)
	require.NoError(t, err)
	require.Equal(t, leaseResp.ID, keepAliveResp.ID)
	require.Greater(t, keepAliveResp.TTL, int64(0))

	watchCtx, stopWatch := context.WithTimeout(ctx, 15*time.Second)
	defer stopWatch()
	watchCh := cli.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(putResp.Revision+1))

	watchedKey := prefix + "/watched"
	watchedResp, err := cli.Kubernetes.OptimisticPut(ctx, watchedKey, []byte("watched-value"), 0, kubernetes.PutOptions{})
	require.NoError(t, err)
	require.True(t, watchedResp.Succeeded)

	var events []string
	for resp := range watchCh {
		require.NoError(t, resp.Err())
		for _, event := range resp.Events {
			events = append(events, fmt.Sprintf("%s=%s", event.Kv.Key, event.Kv.Value))
		}
		if len(events) > 0 {
			break
		}
	}
	require.Contains(t, events, watchedKey+"=watched-value")

	_, err = cli.Revoke(ctx, leaseResp.ID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := cli.Kubernetes.Get(ctx, leasedKey, kubernetes.GetOptions{})
		return err == nil && resp.KV == nil
	}, 10*time.Second, 200*time.Millisecond)
}

func TestEtcdKeyMetadataAndCompare(t *testing.T) {
	cli := newKubernetesClient(t)
	prefix := testPrefix(t)
	cleanupPrefix(t, cli, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := prefix + "/metadata"
	createResp, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)

	firstGet, err := cli.Client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, firstGet.Kvs, 1)
	require.Equal(t, createResp.Header.Revision, firstGet.Kvs[0].CreateRevision)
	require.Equal(t, createResp.Header.Revision, firstGet.Kvs[0].ModRevision)
	require.Equal(t, int64(1), firstGet.Kvs[0].Version)

	_, err = cli.Put(ctx, key, "v2")
	require.NoError(t, err)

	updatedGet, err := cli.Client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, updatedGet.Kvs, 1)
	require.Equal(t, firstGet.Kvs[0].CreateRevision, updatedGet.Kvs[0].CreateRevision)
	require.Greater(t, updatedGet.Kvs[0].ModRevision, updatedGet.Kvs[0].CreateRevision)
	require.Equal(t, int64(2), updatedGet.Kvs[0].Version)

	createFiltered, err := cli.Client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithMinCreateRev(updatedGet.Kvs[0].CreateRevision), clientv3.WithMaxCreateRev(updatedGet.Kvs[0].CreateRevision))
	require.NoError(t, err)
	require.Equal(t, int64(1), createFiltered.Count)
	require.Equal(t, key, string(createFiltered.Kvs[0].Key))

	txnResp, err := cli.Txn(ctx).
		If(
			clientv3.Compare(clientv3.CreateRevision(key), "=", updatedGet.Kvs[0].CreateRevision),
			clientv3.Compare(clientv3.Version(key), "=", int64(2)),
		).
		Then(clientv3.OpPut(key, "v3")).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)

	mismatchResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", updatedGet.Kvs[0].CreateRevision+1)).
		Then(clientv3.OpPut(key, "should-not-write")).
		Else(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.False(t, mismatchResp.Succeeded)
	require.Equal(t, "v3", string(mismatchResp.Responses[0].GetResponseRange().Kvs[0].Value))
}

func byteSlicesToStrings(values [][]byte) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	sort.Strings(out)
	return out
}
