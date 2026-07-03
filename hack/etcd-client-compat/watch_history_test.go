package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestWatchHistoryFallbackCorrectness drives a watch from a revision that
// predates the writes with the real etcd client and asserts the replayed
// CREATE/PUT/DELETE stream is correct, including that each DELETE carries its
// prev-kv. Depending on the warm-cache window the request is served either from
// the in-memory cache or the storage-scan history fallback (#30); both paths
// must produce the identical stream. The deterministic assertion that the
// fallback does exactly one scan (no per-tombstone point reads) lives in the
// backend unit test TestHistoryWatchEventsNoPerTombstoneReads.
func TestWatchHistoryFallbackCorrectness(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	prefix := testPrefix(t) + "/"
	cleanupPrefix(t, newKubernetesClient(t), prefix)

	// startRev is captured before any write under prefix; watching from here
	// forces the history path to replay everything we do below.
	seed, err := cli.Get(ctx, "/registry", clientv3.WithLimit(1))
	require.NoError(t, err)
	startRev := seed.Header.Revision

	kept := prefix + "kept"
	gone := prefix + "gone"
	// kept: create then update (ends PUT v2). gone: create then delete (ends DELETE).
	_, err = cli.Put(ctx, kept, "k1")
	require.NoError(t, err)
	_, err = cli.Put(ctx, kept, "k2")
	require.NoError(t, err)
	_, err = cli.Put(ctx, gone, "g1")
	require.NoError(t, err)
	delResp, err := cli.Delete(ctx, gone)
	require.NoError(t, err)
	require.EqualValues(t, 1, delResp.Deleted)

	// Watch from startRev+1 with prev-kv; collect until we have the DELETE.
	wctx, wcancel := context.WithTimeout(ctx, 15*time.Second)
	defer wcancel()
	wch := cli.Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(startRev+1), clientv3.WithPrevKV())

	type ev struct {
		typ     string
		val     string
		prevVal string
	}
	got := map[string][]ev{}
	sawDelete := false
	for !sawDelete {
		select {
		case wr := <-wch:
			require.NoError(t, wr.Err())
			for _, e := range wr.Events {
				rec := ev{typ: e.Type.String(), val: string(e.Kv.Value)}
				if e.PrevKv != nil {
					rec.prevVal = string(e.PrevKv.Value)
				}
				got[string(e.Kv.Key)] = append(got[string(e.Kv.Key)], rec)
				if e.Type == clientv3.EventTypeDelete {
					sawDelete = true
				}
			}
		case <-wctx.Done():
			t.Fatalf("timed out before observing the DELETE; got=%v", got)
		}
	}

	// kept: last event is PUT "k2".
	keptEvs := got[kept]
	require.NotEmpty(t, keptEvs)
	last := keptEvs[len(keptEvs)-1]
	require.Equal(t, "PUT", last.typ)
	require.Equal(t, "k2", last.val)

	// gone: ends with DELETE whose prev-kv value is the last live value "g1".
	goneEvs := got[gone]
	require.NotEmpty(t, goneEvs)
	del := goneEvs[len(goneEvs)-1]
	require.Equal(t, "DELETE", del.typ)
	require.Equal(t, "g1", del.prevVal, "DELETE must carry prev-kv recovered from the history scan")
}
