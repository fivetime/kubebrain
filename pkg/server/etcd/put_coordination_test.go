package etcd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPutKeyLockSerializesSameKey(t *testing.T) {
	shim := &backendShim{}
	for i := range shim.putLocks {
		shim.putLocks[i] = make(chan struct{}, 1)
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
			unlock, _ := shim.lockPutKey(context.Background(), []byte("/hot/key"))
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

func TestPutKeyLockAllowsDistinctStripes(t *testing.T) {
	shim := &backendShim{}
	for i := range shim.putLocks {
		shim.putLocks[i] = make(chan struct{}, 1)
	}
	first := []byte("/key/first")
	var second []byte
	for i := 0; ; i++ {
		candidate := []byte(fmt.Sprintf("/key/%d", i))
		if putLockStripe(candidate) != putLockStripe(first) {
			second = candidate
			break
		}
	}

	unlockFirst, err := shim.lockPutKey(context.Background(), first)
	require.NoError(t, err)
	defer unlockFirst()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unlockSecond, err := shim.lockPutKey(ctx, second)
	require.NoError(t, err)
	unlockSecond()
}

func TestPutKeyLockHonorsCanceledWaiter(t *testing.T) {
	shim := &backendShim{}
	for i := range shim.putLocks {
		shim.putLocks[i] = make(chan struct{}, 1)
	}
	key := []byte("/hot/key")
	unlock, err := shim.lockPutKey(context.Background(), key)
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = shim.lockPutKey(ctx, key)
	require.ErrorIs(t, err, context.Canceled)
}
