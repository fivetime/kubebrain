package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc/status"
)

type stmDifferentialResult struct {
	NewValue            string
	NewVersion          int64
	AbortCode           string
	AbortKeyExists      bool
	DeleteRetryAttempts int
	DeleteRetryValue    string
	SnapshotAttempts    int
	SnapshotVersion     int64
	SerializableReads   int
}

type crossKeyTxnResult struct {
	CreateSucceeded  bool
	CreateValue      string
	CreateVersion    int64
	UpdateSucceeded  bool
	UpdateValue      string
	DeleteSucceeded  bool
	DeleteCount      int64
	DeleteKeyExists  bool
	StaleSucceeded   bool
	StaleTargetExist bool
}

func TestSTMDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run STM differential compatibility tests")
	}
	require.Equal(t,
		runSTMDeterministicScenario(t, reference, "etcd"),
		runSTMDeterministicScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func TestTxnCrossKeyFastShapeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run cross-key Txn differential tests")
	}
	require.Equal(t,
		runCrossKeyTxnScenario(t, reference, "etcd"),
		runCrossKeyTxnScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runCrossKeyTxnScenario(t *testing.T, endpoint, instance string) crossKeyTxnResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-cross-key/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	createGuard := prefix + "create-guard"
	createTarget := prefix + "create-target"
	_, err = cli.Put(ctx, createTarget, "old")
	require.NoError(t, err)
	create, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(createGuard), "=", 0)).
		Then(clientv3.OpPut(createTarget, "updated")).
		Commit()
	require.NoError(t, err)
	createCurrent, err := cli.Get(ctx, createTarget)
	require.NoError(t, err)
	require.Len(t, createCurrent.Kvs, 1)

	updateGuard := prefix + "update-guard"
	updateTarget := prefix + "update-target"
	updateSeed, err := cli.Put(ctx, updateGuard, "guard")
	require.NoError(t, err)
	update, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(updateGuard), "=", updateSeed.Header.Revision)).
		Then(clientv3.OpPut(updateTarget, "created")).
		Commit()
	require.NoError(t, err)
	updateCurrent, err := cli.Get(ctx, updateTarget)
	require.NoError(t, err)
	require.Len(t, updateCurrent.Kvs, 1)

	deleteGuard := prefix + "delete-guard"
	deleteTarget := prefix + "delete-target"
	deleteSeed, err := cli.Put(ctx, deleteGuard, "guard")
	require.NoError(t, err)
	_, err = cli.Put(ctx, deleteTarget, "target")
	require.NoError(t, err)
	deleted, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(deleteGuard), "=", deleteSeed.Header.Revision)).
		Then(clientv3.OpDelete(deleteTarget)).
		Commit()
	require.NoError(t, err)
	deleteCurrent, err := cli.Get(ctx, deleteTarget)
	require.NoError(t, err)

	staleGuard := prefix + "stale-guard"
	staleTarget := prefix + "stale-target"
	staleSeed, err := cli.Put(ctx, staleGuard, "guard")
	require.NoError(t, err)
	_, err = cli.Delete(ctx, staleGuard)
	require.NoError(t, err)
	stale, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(staleGuard), "=", staleSeed.Header.Revision)).
		Then(clientv3.OpPut(staleTarget, "must-not-commit")).
		Commit()
	require.NoError(t, err)
	staleCurrent, err := cli.Get(ctx, staleTarget)
	require.NoError(t, err)

	return crossKeyTxnResult{
		CreateSucceeded:  create.Succeeded,
		CreateValue:      string(createCurrent.Kvs[0].Value),
		CreateVersion:    createCurrent.Kvs[0].Version,
		UpdateSucceeded:  update.Succeeded,
		UpdateValue:      string(updateCurrent.Kvs[0].Value),
		DeleteSucceeded:  deleted.Succeeded,
		DeleteCount:      deleted.Responses[0].GetResponseDeleteRange().Deleted,
		DeleteKeyExists:  len(deleteCurrent.Kvs) != 0,
		StaleSucceeded:   stale.Succeeded,
		StaleTargetExist: len(staleCurrent.Kvs) != 0,
	}
}

func runSTMDeterministicScenario(t *testing.T, endpoint, instance string) stmDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-stm/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	newKey := prefix + "new"
	_, err = concurrency.NewSTM(cli, func(stm concurrency.STM) error {
		stm.Put(newKey, "new-value")
		return nil
	}, concurrency.WithIsolation(concurrency.RepeatableReads))
	require.NoError(t, err)
	newResponse, err := cli.Get(ctx, newKey)
	require.NoError(t, err)
	require.Len(t, newResponse.Kvs, 1)

	abortKey := prefix + "abort"
	abortCtx, abortCancel := context.WithCancel(ctx)
	_, abortErr := concurrency.NewSTM(cli, func(stm concurrency.STM) error {
		stm.Put(abortKey, "must-not-commit")
		abortCancel()
		stm.Put(abortKey, "still-must-not-commit")
		return nil
	}, concurrency.WithIsolation(concurrency.RepeatableReads), concurrency.WithAbortContext(abortCtx))
	require.Error(t, abortErr)
	abortResponse, err := cli.Get(ctx, abortKey)
	require.NoError(t, err)

	sourceKey := prefix + "delete-source"
	resultKey := prefix + "delete-result"
	_, err = cli.Put(ctx, sourceKey, "source")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, getErr := cli.Get(ctx, sourceKey, clientv3.WithSerializable())
		return getErr == nil && len(response.Kvs) == 1 &&
			string(response.Kvs[0].Value) == "source"
	}, 5*time.Second, 20*time.Millisecond)
	deleted := make(chan struct{})
	releaseDelete := make(chan struct{})
	deleteErrors := make(chan error, 1)
	go func() {
		defer close(deleted)
		<-releaseDelete
		_, deleteErr := cli.Delete(ctx, sourceKey)
		deleteErrors <- deleteErr
	}()
	attempts := 0
	firstRead := ""
	_, err = concurrency.NewSTM(cli, func(stm concurrency.STM) error {
		attempts++
		value := stm.Get(sourceKey)
		if attempts == 1 {
			firstRead = value
			close(releaseDelete)
			<-deleted
		}
		stm.Put(resultKey, value+"-committed")
		return nil
	}, concurrency.WithIsolation(concurrency.RepeatableReads))
	require.NoError(t, err)
	require.NoError(t, <-deleteErrors)
	require.Equal(t, "source", firstRead)
	retryResponse, err := cli.Get(ctx, resultKey)
	require.NoError(t, err)
	require.Len(t, retryResponse.Kvs, 1)

	snapshotReadKey := prefix + "snapshot-read"
	snapshotWriteKey := prefix + "snapshot-write"
	_, err = cli.Put(ctx, snapshotReadKey, "stable")
	require.NoError(t, err)
	snapshotAttempts := 0
	applySnapshot := func(stm concurrency.STM) error {
		snapshotAttempts++
		stm.Get(snapshotReadKey)
		stm.Put(snapshotWriteKey, "value")
		return nil
	}
	_, err = concurrency.NewSTM(
		cli, applySnapshot, concurrency.WithIsolation(concurrency.SerializableSnapshot),
	)
	require.NoError(t, err)
	_, err = concurrency.NewSTM(
		cli, applySnapshot, concurrency.WithIsolation(concurrency.SerializableSnapshot),
	)
	require.NoError(t, err)
	snapshotResponse, err := cli.Get(ctx, snapshotWriteKey)
	require.NoError(t, err)
	require.Len(t, snapshotResponse.Kvs, 1)

	serializePrefix := prefix + "serialize/"
	serializeKeys := make([]string, 5)
	for index := range serializeKeys {
		serializeKeys[index] = fmt.Sprintf("%s%d", serializePrefix, index)
		_, err = cli.Put(ctx, serializeKeys[index], "0")
		require.NoError(t, err)
	}
	updates := make(chan struct{})
	updateErrs := make(chan error, 1)
	go func() {
		defer close(updates)
		for generation := 1; generation <= 5; generation++ {
			ops := make([]clientv3.Op, 0, len(serializeKeys))
			for _, key := range serializeKeys {
				ops = append(ops, clientv3.OpPut(key, fmt.Sprint(generation)))
			}
			if _, txnErr := cli.Txn(ctx).Then(ops...).Commit(); txnErr != nil {
				updateErrs <- txnErr
				return
			}
			updates <- struct{}{}
		}
		updateErrs <- nil
	}()
	serializableReads := 0
	for range updates {
		_, err = concurrency.NewSTM(
			cli,
			func(stm concurrency.STM) error {
				first := stm.Get(serializeKeys[0])
				for _, key := range serializeKeys[1:] {
					if value := stm.Get(key); value != first {
						return fmt.Errorf("serializable STM observed split batch: first=%q key=%q value=%q", first, key, value)
					}
				}
				return nil
			},
			concurrency.WithIsolation(concurrency.Serializable),
		)
		require.NoError(t, err)
		serializableReads++
	}
	require.NoError(t, <-updateErrs)

	return stmDifferentialResult{
		NewValue:            string(newResponse.Kvs[0].Value),
		NewVersion:          newResponse.Kvs[0].Version,
		AbortCode:           status.Code(abortErr).String(),
		AbortKeyExists:      len(abortResponse.Kvs) != 0,
		DeleteRetryAttempts: attempts,
		DeleteRetryValue:    string(retryResponse.Kvs[0].Value),
		SnapshotAttempts:    snapshotAttempts,
		SnapshotVersion:     snapshotResponse.Kvs[0].Version,
		SerializableReads:   serializableReads,
	}
}

func TestSTMConcurrentTransfersPreserveInvariant(t *testing.T) {
	cli := newConcurrencyClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-stm/contention/%d/", time.Now().UnixNano())
	const accounts = 5
	const initial = 100
	const workers = 10
	for index := 0; index < accounts; index++ {
		_, err := cli.Put(ctx, fmt.Sprintf("%s%d", prefix, index), fmt.Sprint(initial))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	var callbacks atomic.Int64
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			_, err := concurrency.NewSTM(cli, func(stm concurrency.STM) error {
				callbacks.Add(1)
				from := worker % accounts
				to := (worker + 1) % accounts
				fromKey := fmt.Sprintf("%s%d", prefix, from)
				toKey := fmt.Sprintf("%s%d", prefix, to)
				var fromValue, toValue int
				_, _ = fmt.Sscan(stm.Get(fromKey), &fromValue)
				_, _ = fmt.Sscan(stm.Get(toKey), &toValue)
				transfer := fromValue / 3
				stm.Put(fromKey, fmt.Sprint(fromValue-transfer))
				stm.Put(toKey, fmt.Sprint(toValue+transfer))
				return nil
			}, concurrency.WithIsolation(concurrency.RepeatableReads))
			errs <- err
		}(worker)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	response, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, response.Kvs, accounts)
	sum := 0
	for _, kv := range response.Kvs {
		var value int
		_, err = fmt.Sscan(string(kv.Value), &value)
		require.NoError(t, err)
		require.GreaterOrEqual(t, value, 0)
		sum += value
	}
	require.Equal(t, accounts*initial, sum)
	require.GreaterOrEqual(t, callbacks.Load(), int64(workers))
}
