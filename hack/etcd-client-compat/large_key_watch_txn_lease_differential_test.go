package compat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type largeKeyMutationEvent struct {
	Type       mvccpb.Event_EventType
	KeyHash    [sha256.Size]byte
	Value      string
	PrevValue  string
	Version    int64
	CreateGap  int64
	ModGap     int64
	LeaseBound bool
}

type largeKeyMutationProjection struct {
	TxnSucceeded []bool
	RevisionGaps []int64
	Events       []largeKeyMutationEvent
	FinalCount   int64
}

func TestLargeKeyWatchTxnLeaseDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run large-key watch/txn/lease differential tests")
	}
	key := append([]byte("$"+testPrefix(t)+"/large-key-watch-txn-lease/"), bytes.Repeat([]byte{'k'}, 600<<10)...)
	want := runLargeKeyWatchTxnLeaseScenario(t, reference, key)
	require.Equal(t, want, runLargeKeyWatchTxnLeaseScenario(t, compatEndpoint(t), key))
}

func runLargeKeyWatchTxnLeaseScenario(t *testing.T, endpoint string, key []byte) largeKeyMutationProjection {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, cleanupErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
		require.NoError(t, cleanupErr)
	}
	cleanup()
	t.Cleanup(cleanup)

	leaseID := int64(0x46070001)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 60})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	})

	watch, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, PrevKv: true},
	}}))
	created, err := watch.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	baseRevision := created.Header.Revision

	first, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value-1"), Lease: leaseID},
		}}},
	})
	require.NoError(t, err)
	require.True(t, first.Succeeded)
	firstEvent := receiveLargeKeyMutationEvent(t, watch, key, baseRevision)

	second, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VALUE, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("value-1")},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value-2"), Lease: leaseID},
		}}},
	})
	require.NoError(t, err)
	require.True(t, second.Succeeded)
	secondEvent := receiveLargeKeyMutationEvent(t, watch, key, baseRevision)

	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	deleteEvent := receiveLargeKeyMutationEvent(t, watch, key, baseRevision)

	final, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, final.Kvs)
	return largeKeyMutationProjection{
		TxnSucceeded: []bool{first.Succeeded, second.Succeeded},
		RevisionGaps: []int64{
			first.Header.Revision - baseRevision,
			second.Header.Revision - first.Header.Revision,
			final.Header.Revision - second.Header.Revision,
		},
		Events:     []largeKeyMutationEvent{firstEvent, secondEvent, deleteEvent},
		FinalCount: final.Count,
	}
}

func receiveLargeKeyMutationEvent(
	t *testing.T, watch etcdserverpb.Watch_WatchClient, key []byte, baseRevision int64,
) largeKeyMutationEvent {
	t.Helper()
	for {
		response, err := watch.Recv()
		require.NoError(t, err)
		if len(response.Events) == 0 {
			continue
		}
		require.Len(t, response.Events, 1)
		event := response.Events[0]
		require.Equal(t, key, event.Kv.Key)
		var createGap int64
		if event.Kv.CreateRevision != 0 {
			createGap = event.Kv.CreateRevision - baseRevision
		}
		projection := largeKeyMutationEvent{
			Type:       event.Type,
			KeyHash:    sha256.Sum256(event.Kv.Key),
			Value:      string(event.Kv.Value),
			Version:    event.Kv.Version,
			CreateGap:  createGap,
			ModGap:     event.Kv.ModRevision - baseRevision,
			LeaseBound: event.Kv.Lease != 0,
		}
		if event.PrevKv != nil {
			projection.PrevValue = string(event.PrevKv.Value)
		}
		return projection
	}
}
