package etcd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

type mutationLockProbeBackend struct {
	backend.Backend
	deleteCalled   bool
	txnApplyCalled bool
}

func (b *mutationLockProbeBackend) Delete(context.Context, *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	b.deleteCalled = true
	return &proto.DeleteResponse{Header: &proto.ResponseHeader{}}, nil
}

func (b *mutationLockProbeBackend) TxnApply(context.Context, []backend.TxnWriteOp, []backend.TxnGuard) ([]backend.TxnWriteResult, uint64, error) {
	b.txnApplyCalled = true
	return []backend.TxnWriteResult{{}}, 1, nil
}

func TestMutationKeyLockSerializesSameKey(t *testing.T) {
	shim := &backendShim{}
	for i := range shim.mutationLocks {
		shim.mutationLocks[i] = make(chan struct{}, 1)
	}

	const workers = 32
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		inCritical int
		maxActive  int
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, _ := shim.lockMutationKeys(context.Background(), []byte("/hot/key"))
			mu.Lock()
			inCritical++
			maxActive = max(maxActive, inCritical)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inCritical--
			mu.Unlock()
			unlock()
		}()
	}
	wg.Wait()
	require.Equal(t, 1, maxActive)
}

func TestMutationKeyLockAllowsDistinctStripes(t *testing.T) {
	shim := &backendShim{}
	for i := range shim.mutationLocks {
		shim.mutationLocks[i] = make(chan struct{}, 1)
	}
	first := []byte("/key/first")
	var second []byte
	for i := 0; ; i++ {
		candidate := []byte(fmt.Sprintf("/key/%d", i))
		if mutationLockStripe(candidate) != mutationLockStripe(first) {
			second = candidate
			break
		}
	}

	unlockFirst, err := shim.lockMutationKeys(context.Background(), first)
	require.NoError(t, err)
	defer unlockFirst()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unlockSecond, err := shim.lockMutationKeys(ctx, second)
	require.NoError(t, err)
	unlockSecond()
}

func TestMutationKeyLockHonorsCanceledWaiter(t *testing.T) {
	shim := &backendShim{}
	for i := range shim.mutationLocks {
		shim.mutationLocks[i] = make(chan struct{}, 1)
	}
	key := []byte("/hot/key")
	unlock, err := shim.lockMutationKeys(context.Background(), key)
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = shim.lockMutationKeys(ctx, key)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPointDeleteAndTxnApplyUseMutationKeyLock(t *testing.T) {
	probe := &mutationLockProbeBackend{}
	shim := &backendShim{backend: probe}
	for i := range shim.mutationLocks {
		shim.mutationLocks[i] = make(chan struct{}, 1)
	}
	key := []byte("/hot/key")
	unlock, err := shim.lockMutationKeys(context.Background(), key)
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = shim.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	require.ErrorIs(t, err, context.Canceled)
	_, _, _, err = shim.TxnApply(ctx, []backend.TxnWriteOp{{Key: key}}, nil, []bool{false})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, probe.deleteCalled)
	require.False(t, probe.txnApplyCalled)
}

func TestBeginMutationCarriesOwnershipIntoTxnApply(t *testing.T) {
	probe := &mutationLockProbeBackend{}
	shim := &backendShim{backend: probe}
	for i := range shim.mutationLocks {
		shim.mutationLocks[i] = make(chan struct{}, 1)
	}
	key := []byte("/hot/key")
	ctx, unlock, err := shim.BeginMutation(context.Background(), key)
	require.NoError(t, err)
	defer unlock()

	done := make(chan error, 1)
	go func() {
		_, _, _, applyErr := shim.TxnApply(ctx,
			[]backend.TxnWriteOp{{Key: key}}, nil, []bool{false})
		done <- applyErr
	}()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("TxnApply reacquired a mutation stripe already owned by its context")
	}
	require.True(t, probe.txnApplyCalled)
}
