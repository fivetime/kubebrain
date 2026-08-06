package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseRevokeMixedPrevKVWatchCase struct {
	Name                           string
	CreatedHeaderAfterBase         int64
	FrameHeaderAfterBase           int64
	EventCount                     int
	Keys                           []string
	CurrentDeleteMetadataCanonical bool
	PrevPresent                    bool
	PrevValues                     []string
	PrevModAfterBase               []int64
	PrevCreateAfterBase            []int64
	PrevVersions                   []int64
	PrevLeasesAttached             []bool
}

type leaseRevokeMixedPrevKVWatchOutcome struct {
	FirstPutAfterBase     int64
	SecondPutAfterBase    int64
	RevokeHeaderAfterBase int64
	Cases                 []leaseRevokeMixedPrevKVWatchCase
}

func TestLeaseRevokeMixedPrevKVWatchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseRevokeMixedPrevKVWatchOutcome{
		FirstPutAfterBase:     1,
		SecondPutAfterBase:    2,
		RevokeHeaderAfterBase: 3,
		Cases: []leaseRevokeMixedPrevKVWatchCase{
			leaseRevokeMixedPrevKVWant("live-prev", 2, true),
			leaseRevokeMixedPrevKVWant("live-no-prev", 2, false),
			leaseRevokeMixedPrevKVWant("catchup-prev", 3, true),
			leaseRevokeMixedPrevKVWant("catchup-no-prev", 3, false),
		},
	}
	referenceOutcome := runLeaseRevokeMixedPrevKVWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseRevokeMixedPrevKVWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func leaseRevokeMixedPrevKVWant(name string, createdHeaderAfterBase int64, withPrev bool) leaseRevokeMixedPrevKVWatchCase {
	want := leaseRevokeMixedPrevKVWatchCase{
		Name:                           name,
		CreatedHeaderAfterBase:         createdHeaderAfterBase,
		FrameHeaderAfterBase:           3,
		EventCount:                     2,
		Keys:                           []string{"a", "b"},
		CurrentDeleteMetadataCanonical: true,
		PrevPresent:                    withPrev,
	}
	if withPrev {
		want.PrevValues = []string{"value-a", "value-b"}
		want.PrevModAfterBase = []int64{2, 1}
		want.PrevCreateAfterBase = []int64{2, 1}
		want.PrevVersions = []int64{1, 1}
		want.PrevLeasesAttached = []bool{true, true}
	}
	return want
}

func runLeaseRevokeMixedPrevKVWatchScenario(t *testing.T, endpoint, instance string) leaseRevokeMixedPrevKVWatchOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-lease-revoke-mixed-prevkv-watch/%s/%d/", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	grant, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	revoked := false
	t.Cleanup(func() {
		if revoked {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, grant.ID)
	})
	putB, err := cli.Put(ctx, prefix+"b", "value-b", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	putA, err := cli.Put(ctx, prefix+"a", "value-a", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	type activeWatch struct {
		name     string
		withPrev bool
		cancel   context.CancelFunc
		channel  clientv3.WatchChan
		created  clientv3.WatchResponse
	}
	newWatch := func(name string, startRevision int64, withPrev bool) activeWatch {
		watchCtx, watchCancel := context.WithCancel(ctx)
		options := []clientv3.OpOption{
			clientv3.WithPrefix(), clientv3.WithRev(startRevision), clientv3.WithCreatedNotify(),
		}
		if withPrev {
			options = append(options, clientv3.WithPrevKV())
		}
		channel := cli.Watch(watchCtx, prefix, options...)
		created := receiveTxnLeaseLiveWatchResponse(t, ctx, channel)
		require.True(t, created.Created)
		require.Empty(t, created.Events)
		return activeWatch{name: name, withPrev: withPrev, cancel: watchCancel, channel: channel, created: created}
	}

	live := []activeWatch{
		newWatch("live-prev", putA.Header.Revision+1, true),
		newWatch("live-no-prev", putA.Header.Revision+1, false),
	}
	for _, watch := range live {
		defer watch.cancel()
	}
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	revoked = true

	outcome := leaseRevokeMixedPrevKVWatchOutcome{
		FirstPutAfterBase:     putB.Header.Revision - base.Header.Revision,
		SecondPutAfterBase:    putA.Header.Revision - base.Header.Revision,
		RevokeHeaderAfterBase: revoke.Header.Revision - base.Header.Revision,
		Cases:                 make([]leaseRevokeMixedPrevKVWatchCase, 0, 4),
	}
	for _, watch := range live {
		frame := receiveTxnLeaseLiveWatchResponse(t, ctx, watch.channel)
		outcome.Cases = append(outcome.Cases, normalizeLeaseRevokeMixedPrevKVWatchCase(
			t, watch.name, watch.withPrev, watch.created, frame, prefix, base.Header.Revision, revoke.Header.Revision,
		))
		watch.cancel()
	}

	catchup := []activeWatch{
		newWatch("catchup-prev", revoke.Header.Revision, true),
		newWatch("catchup-no-prev", revoke.Header.Revision, false),
	}
	for _, watch := range catchup {
		defer watch.cancel()
		frame := receiveTxnLeaseLiveWatchResponse(t, ctx, watch.channel)
		outcome.Cases = append(outcome.Cases, normalizeLeaseRevokeMixedPrevKVWatchCase(
			t, watch.name, watch.withPrev, watch.created, frame, prefix, base.Header.Revision, revoke.Header.Revision,
		))
		watch.cancel()
	}
	return outcome
}

func normalizeLeaseRevokeMixedPrevKVWatchCase(
	t *testing.T,
	name string,
	withPrev bool,
	created clientv3.WatchResponse,
	frame clientv3.WatchResponse,
	prefix string,
	baseRevision int64,
	revokeRevision int64,
) leaseRevokeMixedPrevKVWatchCase {
	t.Helper()
	require.Len(t, frame.Events, 2)
	outcome := leaseRevokeMixedPrevKVWatchCase{
		Name:                           name,
		CreatedHeaderAfterBase:         created.Header.Revision - baseRevision,
		FrameHeaderAfterBase:           frame.Header.Revision - baseRevision,
		EventCount:                     len(frame.Events),
		Keys:                           make([]string, 0, 2),
		CurrentDeleteMetadataCanonical: true,
		PrevPresent:                    withPrev,
	}
	for _, event := range frame.Events {
		require.Equal(t, mvccpb.DELETE, event.Type)
		outcome.Keys = append(outcome.Keys, string(event.Kv.Key[len(prefix):]))
		outcome.CurrentDeleteMetadataCanonical = outcome.CurrentDeleteMetadataCanonical &&
			event.Kv.CreateRevision == 0 && event.Kv.ModRevision == revokeRevision &&
			event.Kv.Version == 0 && event.Kv.Lease == 0 && len(event.Kv.Value) == 0
		if withPrev {
			require.NotNil(t, event.PrevKv)
			outcome.PrevValues = append(outcome.PrevValues, string(event.PrevKv.Value))
			outcome.PrevModAfterBase = append(outcome.PrevModAfterBase, event.PrevKv.ModRevision-baseRevision)
			outcome.PrevCreateAfterBase = append(outcome.PrevCreateAfterBase, event.PrevKv.CreateRevision-baseRevision)
			outcome.PrevVersions = append(outcome.PrevVersions, event.PrevKv.Version)
			outcome.PrevLeasesAttached = append(outcome.PrevLeasesAttached, event.PrevKv.Lease != 0)
		} else {
			require.Nil(t, event.PrevKv)
		}
	}
	return outcome
}
