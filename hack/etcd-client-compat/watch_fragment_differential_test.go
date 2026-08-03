package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWatchFragmentDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	require.Equal(t,
		runWatchFragmentClientScenario(t, reference, "etcd"),
		runWatchFragmentClientScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runWatchFragmentClientScenario(t *testing.T, endpoint, instance string) int {
	t.Helper()
	const (
		eventCount    = 10
		valueBytes    = 1024 * 1024
		clientMaxRecv = 1536 * 1024
	)
	writer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	watcher, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
		MaxCallRecvMsgSize: clientMaxRecv,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, watcher.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-watch-fragment/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = writer.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	base, err := writer.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	for index := 0; index < eventCount; index++ {
		_, err = writer.Put(ctx, fmt.Sprintf("%s%d", prefix, index), strings.Repeat("x", valueBytes))
		require.NoError(t, err)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer watchCancel()
	responses := watcher.Watch(
		watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(base.Header.Revision+1),
		clientv3.WithFragment(),
	)
	events := 0
	for events < eventCount {
		select {
		case response, ok := <-responses:
			require.True(t, ok)
			require.NoError(t, response.Err())
			events += len(response.Events)
		case <-watchCtx.Done():
			require.NoError(t, watchCtx.Err())
		}
	}
	return events
}
