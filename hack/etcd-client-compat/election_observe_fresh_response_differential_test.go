package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// TestElectionObserveFreshResponsesOnProclaim pins upstream etcd 1570c5c85.
// Election.Observe returns *GetResponse values and must allocate a fresh
// response wrapper for every observed leader proposal, so later watch events
// cannot mutate previously delivered observations.
func TestElectionObserveFreshResponsesOnProclaim(t *testing.T) {
	endpoint := compatEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/a3761-election-observe/%d/", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	session, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	election := concurrency.NewElection(session, prefix+"election")
	require.NoError(t, election.Campaign(ctx, "abc"))

	observeCtx, observeCancel := context.WithCancel(ctx)
	defer observeCancel()
	observeCh := election.Observe(observeCtx)

	mustObserve := func(want string) *clientv3.GetResponse {
		t.Helper()
		select {
		case resp, ok := <-observeCh:
			require.True(t, ok)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Header)
			require.Len(t, resp.Kvs, 1)
			require.Equal(t, want, string(resp.Kvs[0].Value))
			return resp
		case <-ctx.Done():
			t.Fatalf("timed out waiting for observed election value %q: %v", want, ctx.Err())
			return nil
		}
	}

	first := mustObserve("abc")

	require.NoError(t, election.Proclaim(ctx, "def"))
	second := mustObserve("def")

	require.NoError(t, election.Proclaim(ctx, "ghi"))
	third := mustObserve("ghi")

	require.NotSame(t, first, second)
	require.NotSame(t, first, third)
	require.NotSame(t, second, third)

	require.Equal(t, "abc", string(first.Kvs[0].Value))
	require.Equal(t, "def", string(second.Kvs[0].Value))
	require.Equal(t, "ghi", string(third.Kvs[0].Value))
	require.Equal(t, int64(1), first.Kvs[0].Version)
	require.Equal(t, int64(2), second.Kvs[0].Version)
	require.Equal(t, int64(3), third.Kvs[0].Version)
	require.Less(t, first.Header.Revision, second.Header.Revision)
	require.Less(t, second.Header.Revision, third.Header.Revision)
}
