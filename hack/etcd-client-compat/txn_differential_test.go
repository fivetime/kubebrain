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
	CountMore     bool
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

	etcd := runTxnDifferentialScenario(t, reference, "etcd")
	want := txnDifferentialResult{
		TxnSucceeded:  true,
		TxnRevision:   1,
		FirstRangeRev: 1,
		FirstRange: []normalizedKV{
			{Key: "a", Value: "old", Version: 1},
			{Key: "b", Value: "new-b", CreateRev: 1, ModRev: 1, Version: 1},
		},
		PutRevision:   1,
		PutPrev:       &normalizedKV{Key: "a", Value: "old", Version: 1},
		CountRangeRev: 1,
		Count:         2,
		FinalRangeRev: 1,
		Final: []normalizedKV{
			{Key: "a", Value: "new-a", ModRev: 1, Version: 2},
			{Key: "b", Value: "new-b", CreateRev: 1, ModRev: 1, Version: 1},
		},
		ErrorCode:    "Unknown",
		ErrorMessage: "etcdserver: key not found",
	}
	require.Equal(t, want, etcd)
	require.Equal(t, etcd, runTxnDifferentialScenario(t, compatEndpoint(), "kubebrain"))
}

type unconditionalTxnResult struct {
	EmptySucceeded   bool
	EmptyResponses   int
	EmptyRevision    int64
	WriteSucceeded   bool
	WriteResponses   int
	WriteRevision    int64
	WrittenValue     string
	FailureKeyExists bool
}

func TestTxnUnconditionalFailureBranchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runUnconditionalTxnScenario(t, reference, "etcd"),
		runUnconditionalTxnScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runUnconditionalTxnScenario(t *testing.T, endpoint, instance string) unconditionalTxnResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-unconditional/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	failureKey := prefix + "failure"
	empty, err := cli.Txn(ctx).Else(clientv3.OpGet(failureKey)).Commit()
	require.NoError(t, err)

	writtenKey := prefix + "written"
	written, err := cli.Txn(ctx).
		Then(clientv3.OpPut(writtenKey, "value")).
		Else(clientv3.OpPut(failureKey, "must-not-write")).
		Commit()
	require.NoError(t, err)
	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	result := unconditionalTxnResult{
		EmptySucceeded: empty.Succeeded,
		EmptyResponses: len(empty.Responses),
		EmptyRevision:  empty.Header.Revision - base.Header.Revision,
		WriteSucceeded: written.Succeeded,
		WriteResponses: len(written.Responses),
		WriteRevision:  written.Header.Revision - base.Header.Revision,
	}
	for _, kv := range current.Kvs {
		switch string(kv.Key) {
		case writtenKey:
			result.WrittenValue = string(kv.Value)
		case failureKey:
			result.FailureKeyExists = true
		}
	}
	return result
}

type historicalLeaseResult struct {
	UnleasedRevision int64
	LeasedRevision   int64
	PlainAgainRev    int64
	OldPlainLease    int64
	OldLeased        bool
	CurrentLease     int64
	CurrentValue     string
}

func TestHistoricalLeaseDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runHistoricalLeaseScenario(t, reference, "etcd"),
		runHistoricalLeaseScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runHistoricalLeaseScenario(t *testing.T, endpoint, instance string) historicalLeaseResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-txn-historical-lease/%s/%d", instance, time.Now().UnixNano())

	base, err := cli.Get(ctx, key)
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
		_, _ = cli.Delete(cleanupCtx, key)
	})

	unleased, err := cli.Put(ctx, key, "plain")
	require.NoError(t, err)
	leased, err := cli.Put(ctx, key, "leased", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	oldPlain, err := cli.Get(ctx, key, clientv3.WithRev(unleased.Header.Revision))
	require.NoError(t, err)
	require.Len(t, oldPlain.Kvs, 1)

	plainAgain, err := cli.Put(ctx, key, "plain-again")
	require.NoError(t, err)
	oldLeased, err := cli.Get(ctx, key, clientv3.WithRev(leased.Header.Revision))
	require.NoError(t, err)
	require.Len(t, oldLeased.Kvs, 1)
	current, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)

	return historicalLeaseResult{
		UnleasedRevision: unleased.Header.Revision - base.Header.Revision,
		LeasedRevision:   leased.Header.Revision - base.Header.Revision,
		PlainAgainRev:    plainAgain.Header.Revision - base.Header.Revision,
		OldPlainLease:    oldPlain.Kvs[0].Lease,
		OldLeased:        oldLeased.Kvs[0].Lease == int64(lease.ID),
		CurrentLease:     current.Kvs[0].Lease,
		CurrentValue:     string(current.Kvs[0].Value),
	}
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
		clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithMinModRev(baseRev+1),
			clientv3.WithCountOnly(), clientv3.WithLimit(1)),
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
		CountMore:     countRange.More,
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
