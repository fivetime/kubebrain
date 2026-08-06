package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type txnLeaseLiveWatchOutcome struct {
	CreatedCanonical  bool
	CreatedHeaderGap  int64
	TxnRevisionGap    int64
	RevokeRevisionGap int64
	FrameEventCounts  []int
	FrameHeaderGaps   []int64
	Events            []txnLeaseRevokeWatchEvent
	FinalValue        string
	FinalLeaseIsB     bool
	LeaseAExpired     bool
	LeaseBKeys        []string
}

func TestTxnLeaseLiveWatchFramingDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := txnLeaseLiveWatchOutcome{
		CreatedCanonical:  true,
		CreatedHeaderGap:  0,
		TxnRevisionGap:    1,
		RevokeRevisionGap: 2,
		FrameEventCounts:  []int{2, 1},
		FrameHeaderGaps:   []int64{1, 2},
		Events: []txnLeaseRevokeWatchEvent{
			{RevisionGap: 1, Type: "PUT", Key: "x", Value: "new-x", Lease: "B", PrevValue: "old-x", PrevLease: "A"},
			{RevisionGap: 1, Type: "PUT", Key: "w", Value: "new-w", Lease: "A"},
			{RevisionGap: 2, Type: "DELETE", Key: "w", PrevValue: "new-w", PrevLease: "A"},
		},
		FinalValue:    "new-x",
		FinalLeaseIsB: true,
		LeaseAExpired: true,
		LeaseBKeys:    []string{"x"},
	}
	referenceOutcome := runTxnLeaseLiveWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnLeaseLiveWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnLeaseLiveWatchScenario(t *testing.T, endpoint, instance string) txnLeaseLiveWatchOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-lease-live-watch/%s/%d/", instance, time.Now().UnixNano())
	leaseA, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	leaseARevoked := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !leaseARevoked {
			_, _ = cli.Revoke(cleanupCtx, leaseA.ID)
		}
		_, _ = cli.Revoke(cleanupCtx, leaseB.ID)
	})
	seed, err := cli.Put(ctx, prefix+"x", "old-x", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	baseRevision := seed.Header.Revision

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(baseRevision+1), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	created := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	require.Empty(t, created.Events)

	txn, err := cli.Txn(ctx).Then(
		clientv3.OpPut(prefix+"x", "new-x", clientv3.WithLease(leaseB.ID)),
		clientv3.OpPut(prefix+"w", "new-w", clientv3.WithLease(leaseA.ID)),
	).Commit()
	require.NoError(t, err)
	txnFrame := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.Len(t, txnFrame.Events, 2)
	revoked, err := cli.Revoke(ctx, leaseA.ID)
	require.NoError(t, err)
	leaseARevoked = true
	revokeFrame := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.Len(t, revokeFrame.Events, 1)
	watchCancel()

	events := make([]txnLeaseRevokeWatchEvent, 0, 3)
	for _, frame := range []clientv3.WatchResponse{txnFrame, revokeFrame} {
		for _, event := range frame.Events {
			observed := txnLeaseRevokeWatchEvent{
				RevisionGap: event.Kv.ModRevision - baseRevision,
				Type:        event.Type.String(),
				Key:         string(event.Kv.Key[len(prefix):]),
				Value:       string(event.Kv.Value),
				Lease:       raceLeaseLabel(event.Kv.Lease, leaseA.ID, leaseB.ID),
			}
			if event.PrevKv != nil {
				observed.PrevValue = string(event.PrevKv.Value)
				observed.PrevLease = raceLeaseLabel(event.PrevKv.Lease, leaseA.ID, leaseB.ID)
			}
			events = append(events, observed)
		}
	}
	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	ttlA, err := cli.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	ttlB, err := cli.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)

	return txnLeaseLiveWatchOutcome{
		CreatedCanonical:  created.Created && !created.Canceled && created.CompactRevision == 0 && len(created.Events) == 0,
		CreatedHeaderGap:  created.Header.Revision - baseRevision,
		TxnRevisionGap:    txn.Header.Revision - baseRevision,
		RevokeRevisionGap: revoked.Header.Revision - baseRevision,
		FrameEventCounts:  []int{len(txnFrame.Events), len(revokeFrame.Events)},
		FrameHeaderGaps:   []int64{txnFrame.Header.Revision - baseRevision, revokeFrame.Header.Revision - baseRevision},
		Events:            events,
		FinalValue:        string(current.Kvs[0].Value),
		FinalLeaseIsB:     current.Kvs[0].Lease == int64(leaseB.ID),
		LeaseAExpired:     ttlA.TTL == -1 && len(ttlA.Keys) == 0,
		LeaseBKeys:        relativeLeaseKeys(ttlB.Keys, prefix),
	}
}

func receiveTxnLeaseLiveWatchResponse(t *testing.T, ctx context.Context, watch clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok)
		require.NoError(t, response.Err())
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for live watch response: %v", ctx.Err())
		return clientv3.WatchResponse{}
	}
}
