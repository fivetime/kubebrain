package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestRPCServerCleanupJoinsOwnedWorkers(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	raw := server.backend.(*backendShim).backend
	// Also clean up on the regression's failure path, so the test itself does
	// not leave behind the very workers it is checking.
	defer func() {
		require.NoError(t, server.Close())
		require.NoError(t, raw.(interface{ Close() error }).Close())
	}()
	entered, stopped := make(chan struct{}), make(chan struct{})
	require.True(t, server.leaseManager.startWorker(func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
		close(stopped)
	}))
	<-entered
	closeFn()
	select {
	case <-stopped:
	default:
		t.Fatal("test server cleanup returned without joining its owned worker")
	}
	require.False(t, server.leaseManager.startWorker(func(context.Context) {
		t.Error("closed test server admitted new background work")
	}))
}
