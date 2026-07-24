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

type serializableReadResult struct {
	currentValue       string
	historicalValue    string
	txnValue           string
	txnSucceeded       bool
	currentHeaderDelta int64
	historyHeaderDelta int64
}

func TestSerializableRangeSurvivesKubeBrainLeaderDeletion(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_SERIALIZABLE_FOLLOWER_ENDPOINT")
	leaderPod := os.Getenv("KUBEBRAIN_SERIALIZABLE_LEADER_POD")
	if endpoint == "" || leaderPod == "" {
		t.Skip("set KUBEBRAIN_SERIALIZABLE_FOLLOWER_ENDPOINT and KUBEBRAIN_SERIALIZABLE_LEADER_POD")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	key := fmt.Sprintf("/dbaas-serializable/failover/%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	_, err = cli.Put(ctx, key, "v2")
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	}()
	require.Eventually(t, func() bool {
		resp, err := cli.Get(ctx, key, clientv3.WithSerializable())
		if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v2" {
			return false
		}
		historical, err := cli.Get(ctx, key, clientv3.WithRev(first.Header.Revision), clientv3.WithSerializable())
		if err != nil || len(historical.Kvs) != 1 || string(historical.Kvs[0].Value) != "v1" {
			return false
		}
		txn, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(key), "=", "v2")).
			Then(clientv3.OpGet(key, clientv3.WithSerializable())).
			Else(clientv3.OpGet(key, clientv3.WithSerializable())).
			Commit()
		return err == nil && txn.Succeeded && len(txn.Responses) == 1 &&
			len(txn.Responses[0].GetResponseRange().Kvs) == 1
	}, 5*time.Second, 20*time.Millisecond)

	output, err := runCompatKubectlContext(t, ctx, "-n", namespace, "delete", "pod", leaderPod, "--wait=false")
	require.NoErrorf(t, err, "delete leader: %s", output)
	for i := 0; i < 30; i++ {
		opCtx, opCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		get, getErr := cli.Get(opCtx, key, clientv3.WithSerializable())
		historical, historicalErr := cli.Get(opCtx, key, clientv3.WithRev(first.Header.Revision), clientv3.WithSerializable())
		txn, txnErr := cli.Txn(opCtx).
			If(clientv3.Compare(clientv3.Value(key), "=", "v2")).
			Then(clientv3.OpGet(key, clientv3.WithSerializable())).
			Else(clientv3.OpGet(key, clientv3.WithSerializable())).
			Commit()
		opCancel()
		require.NoError(t, getErr, "serializable Range iteration %d", i)
		require.Len(t, get.Kvs, 1)
		require.Equal(t, "v2", string(get.Kvs[0].Value))
		require.NoError(t, historicalErr, "historical Range iteration %d", i)
		require.Len(t, historical.Kvs, 1)
		require.Equal(t, "v1", string(historical.Kvs[0].Value))
		require.NoError(t, txnErr, "serializable Txn iteration %d", i)
		require.True(t, txn.Succeeded)
		require.Len(t, txn.Responses[0].GetResponseRange().Kvs, 1)
		require.Equal(t, "v2", string(txn.Responses[0].GetResponseRange().Kvs[0].Value))
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSerializableReadDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT to run serializable read differential")
	}

	referenceResult := runSerializableReadScenario(t, reference, "etcd")
	kubebrainResult := runSerializableReadScenario(t, kubebrain, "kubebrain")
	require.Equal(t, referenceResult, kubebrainResult)
}

func runSerializableReadScenario(t *testing.T, endpoint, instance string) serializableReadResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()

	key := fmt.Sprintf("/dbaas-serializable/%s/%d", instance, time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	}()
	first, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	firstRevision := first.Header.Revision
	second, err := cli.Put(ctx, key, "v2")
	require.NoError(t, err)

	current, err := cli.Get(ctx, key, clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	historical, err := cli.Get(ctx, key, clientv3.WithRev(firstRevision), clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", 0)).
		Then(clientv3.OpGet(key, clientv3.WithSerializable())).
		Else(clientv3.OpGet(key, clientv3.WithSerializable())).
		Commit()
	require.NoError(t, err)
	require.Len(t, txn.Responses, 1)
	require.Len(t, txn.Responses[0].GetResponseRange().Kvs, 1)

	return serializableReadResult{
		currentValue:       string(current.Kvs[0].Value),
		historicalValue:    string(historical.Kvs[0].Value),
		txnValue:           string(txn.Responses[0].GetResponseRange().Kvs[0].Value),
		txnSucceeded:       txn.Succeeded,
		currentHeaderDelta: current.Header.Revision - second.Header.Revision,
		historyHeaderDelta: historical.Header.Revision - second.Header.Revision,
	}
}
