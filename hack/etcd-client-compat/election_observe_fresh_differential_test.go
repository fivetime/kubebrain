package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// TestElectionObserveFreshResponsesDifferentialAgainstReferenceEtcd pins
// upstream etcd 1570c5c85. Each Election.Observe update must return a fresh
// *clientv3.GetResponse wrapper so later Proclaim updates do not mutate earlier
// observations.
func TestElectionObserveFreshResponsesDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := runElectionObserveFreshScenario(t, reference, "reference")
	require.Equal(t, want, runElectionObserveFreshScenario(t, compatEndpoint(t), "kubebrain"))
}

type electionObserveFreshOutcome struct {
	Values          []string
	Versions        []int64
	HeaderAscending bool
	FreshWrappers   bool
	FinalLeader     string
}

func runElectionObserveFreshScenario(t *testing.T, endpoint, instance string) electionObserveFreshOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	session, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	require.NoError(t, err)
	t.Cleanup(session.Orphan)

	prefix := fmt.Sprintf("/a3752/election-observe-fresh/%s/%d/", instance, time.Now().UnixNano())
	election := concurrency.NewElection(session, prefix)
	require.NoError(t, election.Campaign(ctx, "abc"))
	observeCtx, observeCancel := context.WithCancel(ctx)
	defer observeCancel()
	observe := election.Observe(observeCtx)
	mustObserve := func(want string) *clientv3.GetResponse {
		select {
		case response, ok := <-observe:
			require.True(t, ok)
			require.NotNil(t, response)
			require.NotNil(t, response.Header)
			require.Len(t, response.Kvs, 1)
			require.Equal(t, want, string(response.Kvs[0].Value))
			return response
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return nil
		}
	}

	first := mustObserve("abc")
	require.NoError(t, election.Proclaim(ctx, "def"))
	second := mustObserve("def")
	require.NoError(t, election.Proclaim(ctx, "ghi"))
	third := mustObserve("ghi")

	leader, err := election.Leader(ctx)
	require.NoError(t, err)
	require.NoError(t, election.Resign(ctx))
	return electionObserveFreshOutcome{
		Values: []string{
			string(first.Kvs[0].Value),
			string(second.Kvs[0].Value),
			string(third.Kvs[0].Value),
		},
		Versions: []int64{
			first.Kvs[0].Version,
			second.Kvs[0].Version,
			third.Kvs[0].Version,
		},
		HeaderAscending: first.Header.Revision < second.Header.Revision &&
			second.Header.Revision < third.Header.Revision,
		FreshWrappers: first != second && first != third && second != third,
		FinalLeader:   string(leader.Kvs[0].Value),
	}
}
