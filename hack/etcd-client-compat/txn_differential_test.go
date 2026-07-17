package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type normalizedKV struct {
	Key       string
	Value     string
	CreateRev int64
	ModRev    int64
	Version   int64
	HasLease  bool
}

type txnDifferentialResult struct {
	TxnSucceeded  bool
	TxnRevision   int64
	FirstRangeRev int64
	FirstRange    []normalizedKV
	PutRevision   int64
	PutPrev       *normalizedKV
	CountRangeRev int64
	Count         int64
	CountKVs      int
	FinalRangeRev int64
	Final         []normalizedKV
	ErrorCode     string
	ErrorMessage  string
}

func TestTxnDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	kubebrain := runTxnDifferentialScenario(t, compatEndpoint(), "kubebrain")
	etcd := runTxnDifferentialScenario(t, reference, "etcd")
	require.Equal(t, etcd, kubebrain)
}

func TestTxnConcurrentCreateDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	kubebrain := runConcurrentCreateScenario(t, compatEndpoint(), "kubebrain")
	etcd := runConcurrentCreateScenario(t, reference, "etcd")
	require.Equal(t, etcd, kubebrain)
}

type txnLeaseResult struct {
	CreateSucceeded bool
	CreateRevision  int64
	KeysAfterCreate int
	DeleteSucceeded bool
	DeleteRevision  int64
	DeletePrev      *normalizedKV
	KeysAfterDelete int
	DuplicateError  authErrorOutcome
	DuplicateAbsent bool
}

func TestTxnLeaseAttachmentDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	require.Equal(t,
		runTxnLeaseScenario(t, reference, "etcd"),
		runTxnLeaseScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runTxnLeaseScenario(t *testing.T, endpoint, instance string) txnLeaseResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-lease/%s/%d/", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
	})
	key := prefix + "fast"
	created, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "value", clientv3.WithLease(lease.ID))).
		Else(clientv3.OpGet(key)).Commit()
	require.NoError(t, err)
	afterCreate, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	current, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	deleted, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", current.Kvs[0].ModRevision)).
		Then(clientv3.OpDelete(key, clientv3.WithPrevKV())).Commit()
	require.NoError(t, err)
	afterDelete, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)

	duplicateKey := prefix + "duplicate"
	_, duplicateErr := cli.Txn(ctx).Then(
		clientv3.OpPut(duplicateKey, "temporary", clientv3.WithLease(lease.ID)),
		clientv3.OpDelete(duplicateKey),
	).Commit()
	require.Error(t, duplicateErr)
	duplicateCurrent, err := cli.Get(ctx, duplicateKey)
	require.NoError(t, err)

	result := txnLeaseResult{
		CreateSucceeded: created.Succeeded,
		CreateRevision:  created.Header.Revision - baseRev,
		KeysAfterCreate: len(afterCreate.Keys),
		DeleteSucceeded: deleted.Succeeded,
		DeleteRevision:  deleted.Header.Revision - baseRev,
		KeysAfterDelete: len(afterDelete.Keys),
		DuplicateError:  authError(duplicateErr),
		DuplicateAbsent: len(duplicateCurrent.Kvs) == 0,
	}
	if previous := deleted.Responses[0].GetResponseDeleteRange().PrevKvs; len(previous) == 1 {
		normalized := normalizeKV(previous[0], prefix, baseRev)
		result.DeletePrev = &normalized
	}
	return result
}

type concurrentCreateResult struct {
	Succeeded int
	Failed    int
	FinalKVs  int
}

func runConcurrentCreateScenario(t *testing.T, endpoint, instance string) concurrentCreateResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	key := fmt.Sprintf("/dbaas-differential/%s/concurrent-create/%d", instance, time.Now().UnixNano())
	const contenders = 32
	start := make(chan struct{})
	results := make(chan bool, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := cli.Txn(ctx).
				If(clientv3.Compare(clientv3.Version(key), "=", 0)).
				Then(clientv3.OpPut(key, fmt.Sprintf("winner-%d", i))).
				Else(clientv3.OpGet(key)).
				Commit()
			if err != nil {
				errs <- err
				return
			}
			results <- resp.Succeeded
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	result := concurrentCreateResult{}
	for succeeded := range results {
		if succeeded {
			result.Succeeded++
		} else {
			result.Failed++
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	final, err := cli.Get(ctx, key)
	require.NoError(t, err)
	result.FinalKVs = len(final.Kvs)
	require.Equal(t, concurrentCreateResult{Succeeded: 1, Failed: contenders - 1, FinalKVs: 1}, result)
	_, _ = cli.Delete(ctx, key)
	return result
}

func runTxnDifferentialScenario(t *testing.T, endpoint, instance string) txnDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-differential/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	a, b := prefix+"a", prefix+"b"
	seed, err := cli.Put(ctx, a, "old")
	require.NoError(t, err)
	baseRev := seed.Header.Revision

	txn, err := cli.Txn(ctx).Then(
		clientv3.OpPut(b, "new-b"),
		clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
		clientv3.OpPut(a, "new-a", clientv3.WithIgnoreLease(), clientv3.WithPrevKV()),
		clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithMinModRev(baseRev+1), clientv3.WithCountOnly()),
	).Commit()
	require.NoError(t, err)
	require.Len(t, txn.Responses, 4)

	firstRange := txn.Responses[1].GetResponseRange()
	put := txn.Responses[2].GetResponsePut()
	countRange := txn.Responses[3].GetResponseRange()
	final, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)

	_, missingErr := cli.Put(ctx, prefix+"missing", "ignored", clientv3.WithIgnoreLease())
	require.Error(t, missingErr)
	missingStatus := status.Convert(missingErr)

	result := txnDifferentialResult{
		TxnSucceeded:  txn.Succeeded,
		TxnRevision:   txn.Header.Revision - baseRev,
		FirstRangeRev: firstRange.Header.Revision - baseRev,
		FirstRange:    normalizeKVs(firstRange.Kvs, prefix, baseRev),
		PutRevision:   put.Header.Revision - baseRev,
		CountRangeRev: countRange.Header.Revision - baseRev,
		Count:         countRange.Count,
		CountKVs:      len(countRange.Kvs),
		FinalRangeRev: final.Header.Revision - baseRev,
		Final:         normalizeKVs(final.Kvs, prefix, baseRev),
		ErrorCode:     missingStatus.Code().String(),
		ErrorMessage:  missingStatus.Message(),
	}
	if put.PrevKv != nil {
		prev := normalizeKV(put.PrevKv, prefix, baseRev)
		result.PutPrev = &prev
	}
	return result
}

func normalizeKVs(kvs []*mvccpb.KeyValue, prefix string, baseRev int64) []normalizedKV {
	result := make([]normalizedKV, 0, len(kvs))
	for _, kv := range kvs {
		result = append(result, normalizeKV(kv, prefix, baseRev))
	}
	return result
}

func normalizeKV(kv *mvccpb.KeyValue, prefix string, baseRev int64) normalizedKV {
	key := string(kv.Key)
	if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
		key = key[len(prefix):]
	}
	return normalizedKV{
		Key:       key,
		Value:     string(kv.Value),
		CreateRev: kv.CreateRevision - baseRev,
		ModRev:    kv.ModRevision - baseRev,
		Version:   kv.Version,
		HasLease:  kv.Lease != 0,
	}
}
