package tikv

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv"
)

func TestNewKvStorageStartupHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	store, err := NewKvStorageWithContext(ctx, []string{"http://127.0.0.1:1"}, 1, Security{})
	require.Nil(t, store)
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
}

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

func TestCreateTxnClientRotatesPastUnavailablePreferredEndpoint(t *testing.T) {
	var attempts [][]string
	want := &txnkv.Client{}
	client, err := createTxnClientWithEndpointRotation([]string{"pd-0", "pd-1", "pd-2"}, 0, func(addrs []string) (*txnkv.Client, error) {
		attempts = append(attempts, append([]string(nil), addrs...))
		if addrs[0] == "pd-0" {
			return nil, errors.New("preferred endpoint unavailable")
		}
		return want, nil
	})
	require.NoError(t, err)
	require.Same(t, want, client)
	require.Equal(t, [][]string{{"pd-0", "pd-1", "pd-2"}, {"pd-1", "pd-2", "pd-0"}}, attempts)
}

func TestCreateTxnClientReportsAllEndpointRotationsFailed(t *testing.T) {
	var attempts [][]string
	client, err := createTxnClientWithEndpointRotation([]string{"pd-0", "pd-1", "pd-2"}, 1, func(addrs []string) (*txnkv.Client, error) {
		attempts = append(attempts, append([]string(nil), addrs...))
		return nil, errors.New("unavailable")
	})
	require.Nil(t, client)
	require.ErrorContains(t, err, "all 3 PD endpoint rotations failed")
	require.Equal(t, [][]string{{"pd-1", "pd-2", "pd-0"}, {"pd-2", "pd-0", "pd-1"}, {"pd-0", "pd-1", "pd-2"}}, attempts)
}

func TestCreateTxnClientRejectsEmptyEndpointSet(t *testing.T) {
	client, err := createTxnClientWithEndpointRotation(nil, 0, func([]string) (*txnkv.Client, error) {
		t.Fatal("factory must not run without PD endpoints")
		return nil, nil
	})
	require.Nil(t, client)
	require.ErrorContains(t, err, "no PD endpoints configured")
}
