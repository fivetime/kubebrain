package tikv

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv"
)

func TestCreateTxnClientsStartsPoolConcurrently(t *testing.T) {
	const count = 16
	var started atomic.Int32
	release := make(chan struct{})
	type result struct {
		clients []*txnkv.Client
		err     error
	}
	done := make(chan result, 1)
	go func() {
		clients, err := createTxnClients(count, func(_ int) (*txnkv.Client, error) {
			started.Add(1)
			<-release
			return &txnkv.Client{}, nil
		})
		done <- result{clients: clients, err: err}
	}()
	require.Eventually(t, func() bool { return started.Load() == count }, time.Second, time.Millisecond)
	close(release)
	got := <-done
	require.NoError(t, got.err)
	require.Len(t, got.clients, count)
}

func TestCreateTxnClientsReturnsLowestSlotError(t *testing.T) {
	want := errors.New("slot failed")
	clients, err := createTxnClients(4, func(index int) (*txnkv.Client, error) {
		return nil, fmt.Errorf("%w %d", want, index)
	})
	require.Nil(t, clients)
	require.ErrorIs(t, err, want)
	require.ErrorContains(t, err, "txn client 0")
}
