package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestTxnCompareHeaderRevisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Zero(t, runTxnCompareHeaderRevisionScenario(t, reference, "etcd"))
	require.Zero(t, runTxnCompareHeaderRevisionScenario(t, compatEndpoint(t), "kubebrain"))
}

type txnRevisionOutcome struct {
	ReadOnlyDelta    int64
	EmptyDeleteDelta int64
	WriteDelta       int64
}

func TestTxnRevisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := txnRevisionOutcome{ReadOnlyDelta: 0, EmptyDeleteDelta: 0, WriteDelta: 1}
	referenceOutcome := runTxnRevisionScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnRevisionScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnRevisionScenario(t *testing.T, endpoint, instance string) txnRevisionOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-txn-revision/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	seed, err := client.Put(ctx, key, "one")
	require.NoError(t, err)

	readOnly, err := client.Txn(ctx).Then(clientv3.OpGet(key)).Commit()
	require.NoError(t, err)
	emptyDelete, err := client.Txn(ctx).Then(clientv3.OpDelete(prefix + "missing")).Commit()
	require.NoError(t, err)
	write, err := client.Txn(ctx).Then(clientv3.OpPut(key, "two")).Commit()
	require.NoError(t, err)

	return txnRevisionOutcome{
		ReadOnlyDelta:    readOnly.Header.Revision - seed.Header.Revision,
		EmptyDeleteDelta: emptyDelete.Header.Revision - seed.Header.Revision,
		WriteDelta:       write.Header.Revision - seed.Header.Revision,
	}
}

func runTxnCompareHeaderRevisionScenario(t *testing.T, endpoint, instance string) int {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	prefix := fmt.Sprintf("/dbaas-txn-compare-header/%s/%d/", instance, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	const rounds = 500
	violations := 0
	for i := 0; i < rounds; i++ {
		key := fmt.Sprintf("%s%04d", prefix, i)
		start := make(chan struct{})
		var putRevision int64
		var putErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := client.Put(ctx, key, "value")
			putErr = err
			if err == nil {
				putRevision = resp.Header.Revision
			}
		}()

		close(start)
		txn, txnErr := client.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpGet(key)).
			Commit()
		wg.Wait()
		require.NoError(t, putErr)
		require.NoError(t, txnErr)

		if putRevision > txn.Header.Revision && !txn.Succeeded {
			violations++
		}
		if txn.Header.Revision >= putRevision && txn.Succeeded {
			violations++
		}
	}
	return violations
}
