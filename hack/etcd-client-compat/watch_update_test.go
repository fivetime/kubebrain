package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestWatchUpdateReportsUpdateNotCreate pins #52 end-to-end: the watch event for
// an update must report IsCreate()==false, carry the correct create_revision
// (the key's creation revision, below its mod_revision), and include PrevKv.
func TestWatchUpdateReportsUpdateNotCreate(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint()}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := testPrefix(t) + "/obj"
	cleanupPrefix(t, newKubernetesClient(t), key)

	createResp, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	createRev := createResp.Header.Revision

	wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
	defer wcancel()
	wch := cli.Watch(wctx, key, clientv3.WithRev(createRev+1), clientv3.WithPrevKV())

	_, err = cli.Put(ctx, key, "v2")
	require.NoError(t, err)

	select {
	case wr := <-wch:
		require.NoError(t, wr.Err())
		require.NotEmpty(t, wr.Events)
		ev := wr.Events[0]
		require.Equal(t, clientv3.EventTypePut, ev.Type)
		require.False(t, ev.IsCreate(), "an update must not be reported as a create")
		require.Less(t, ev.Kv.CreateRevision, ev.Kv.ModRevision,
			"create_revision must be below mod_revision for an update")
		require.Equal(t, createRev, ev.Kv.CreateRevision, "create_revision must be the key's creation revision")
		require.NotNil(t, ev.PrevKv, "update event must carry prev-kv")
		require.Equal(t, "v1", string(ev.PrevKv.Value))
	case <-wctx.Done():
		t.Fatal("timed out waiting for the update watch event")
	}
}
