package compat

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestConcurrentPutDeleteNeverReportsSpuriousMiss keeps the key present before
// each round, then races one unconditional Delete with Put-only writers. The
// Delete can linearize before or after any Put, but the key exists at every
// possible pre-Delete point, so etcd must report Deleted=1.
func TestConcurrentPutDeleteNeverReportsSpuriousMiss(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	const rounds, writers = 30, 8
	for round := 0; round < rounds; round++ {
		key := fmt.Sprintf("%s%02d", prefix, round)
		_, err = cli.Put(ctx, key, "seed")
		require.NoError(t, err)

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for writer := 0; writer < writers; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				_, putErr := cli.Put(ctx, key, fmt.Sprintf("writer-%d", writer))
				errs <- putErr
			}(writer)
		}
		close(start)
		deleted, deleteErr := cli.Delete(ctx, key)
		wg.Wait()
		close(errs)
		require.NoError(t, deleteErr)
		for putErr := range errs {
			require.NoError(t, putErr)
		}
		require.EqualValues(t, 1, deleted.Deleted, "round %d reported a spurious missing key", round)
	}
}

// TestConcurrentUnconditionalTxnSameKeyNeverFails covers the atomic TxnApply
// path. With no compares, etcd always selects Success; overlapping writes may
// serialize in any order but must not expose backend guard conflicts.
func TestConcurrentUnconditionalTxnSameKeyNeverFails(t *testing.T) {
	key := testPrefix(t) + "/txn-hot"
	const writers, perWriter = 8, 20

	var wg sync.WaitGroup
	errCh := make(chan error, writers*perWriter)
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{
				Endpoints: []string{compatEndpoint()}, DialTimeout: 5 * time.Second,
			})
			if err != nil {
				errCh <- err
				return
			}
			defer cli.Close()
			for i := 0; i < perWriter; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				resp, err := cli.Txn(ctx).Then(clientv3.OpPut(key, fmt.Sprintf("writer-%d-%d", writer, i))).Commit()
				cancel()
				if err != nil {
					errCh <- err
					return
				}
				if !resp.Succeeded {
					errCh <- fmt.Errorf("unconditional txn unexpectedly selected failure")
					return
				}
			}
		}(writer)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint()}, DialTimeout: 5 * time.Second})
		if err != nil {
			return
		}
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = cli.Delete(ctx, key)
	})
}
