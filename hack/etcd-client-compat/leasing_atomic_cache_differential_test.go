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
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingAtomicCacheOutcome struct {
	Keys                  int
	WriterTransactions    int64
	ReaderProgress        bool
	MixedRevisionReads    int64
	FinalSingleRevision   bool
	FinalValuesConsistent bool
}

func TestLeasingAtomicCacheDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing atomic cache differential tests")
	}

	require.Equal(t,
		runLeasingAtomicCacheScenario(t, reference, "etcd"),
		runLeasingAtomicCacheScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingAtomicCacheScenario(t *testing.T, endpoint, instance string) leasingAtomicCacheOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-atomic/%s/%d/", instance, time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	leased, closeLeased, err := leasing.NewKV(client, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	const (
		keyCount    = 8
		writerCount = 4
		readerCount = 4
		iterations  = 8
	)
	keys := make([]string, keyCount)
	initialPuts := make([]clientv3.Op, keyCount)
	gets := make([]clientv3.Op, keyCount)
	for index := range keys {
		keys[index] = fmt.Sprintf("%s%02d", dataPrefix, index)
		initialPuts[index] = clientv3.OpPut(keys[index], "generation-0")
		gets[index] = clientv3.OpGet(keys[index])
	}
	_, err = client.Txn(ctx).Then(initialPuts...).Commit()
	require.NoError(t, err)
	for _, get := range gets {
		_, err = leased.Do(ctx, get)
		require.NoError(t, err)
	}

	var writerTransactions atomic.Int64
	var readerTransactions atomic.Int64
	var mixedRevisionReads atomic.Int64
	start := make(chan struct{})
	writersDone := make(chan struct{})
	errs := make(chan error, writerCount+readerCount)
	var writers sync.WaitGroup
	var readers sync.WaitGroup

	for writer := 0; writer < writerCount; writer++ {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				generation := fmt.Sprintf("writer-%d-generation-%d", writer, iteration)
				puts := make([]clientv3.Op, keyCount)
				for index := range keys {
					puts[index] = clientv3.OpPut(keys[index], generation)
				}
				if _, commitErr := leased.Txn(ctx).Then(puts...).Commit(); commitErr != nil {
					errs <- commitErr
					return
				}
				writerTransactions.Add(1)
			}
		}(writer)
	}
	for range readerCount {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for {
				response, commitErr := leased.Txn(ctx).Then(gets...).Commit()
				if commitErr != nil {
					errs <- commitErr
					return
				}
				readerTransactions.Add(1)
				if !leasingTxnResponseHasSingleRevision(response, keyCount) {
					mixedRevisionReads.Add(1)
				}
				select {
				case <-writersDone:
					return
				default:
				}
			}
		}()
	}
	close(start)
	writers.Wait()
	close(writersDone)
	readers.Wait()
	close(errs)
	for runErr := range errs {
		require.NoError(t, runErr)
	}
	require.Equal(t, int64(writerCount*iterations), writerTransactions.Load())
	require.Positive(t, readerTransactions.Load())
	require.Zero(t, mixedRevisionReads.Load())

	final, err := leased.Txn(ctx).Then(gets...).Commit()
	require.NoError(t, err)
	finalSingleRevision := leasingTxnResponseHasSingleRevision(final, keyCount)
	finalValuesConsistent := true
	var finalValue string
	for index, response := range final.Responses {
		kvs := response.GetResponseRange().Kvs
		require.Len(t, kvs, 1)
		if index == 0 {
			finalValue = string(kvs[0].Value)
		} else if string(kvs[0].Value) != finalValue {
			finalValuesConsistent = false
		}
	}

	return leasingAtomicCacheOutcome{
		Keys:                  keyCount,
		WriterTransactions:    writerTransactions.Load(),
		ReaderProgress:        readerTransactions.Load() > 0,
		MixedRevisionReads:    mixedRevisionReads.Load(),
		FinalSingleRevision:   finalSingleRevision,
		FinalValuesConsistent: finalValuesConsistent,
	}
}

func leasingTxnResponseHasSingleRevision(response *clientv3.TxnResponse, expected int) bool {
	if len(response.Responses) != expected {
		return false
	}
	var revision int64
	for index, operation := range response.Responses {
		kvs := operation.GetResponseRange().Kvs
		if len(kvs) != 1 {
			return false
		}
		if index == 0 {
			revision = kvs[0].ModRevision
		} else if kvs[0].ModRevision != revision {
			return false
		}
	}
	return revision > 0
}
