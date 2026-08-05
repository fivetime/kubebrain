package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type watchSingleOversizedEventOutcome struct {
	CreatedEnvelope       watchControlOutcome
	ResponseEnvelope      watchControlOutcome
	PutRevisionGap        int64
	ResponseHeaderAtPut   bool
	ResponseOverTwoMiB    bool
	Type                  mvccpb.Event_EventType
	KeyMatches            bool
	ValueMatches          bool
	CreateRevisionGap     int64
	ModRevisionGap        int64
	Version               int64
	Lease                 int64
	PrevPresent           bool
	PrevKeyMatches        bool
	PrevValueMatches      bool
	PrevCreateRevisionGap int64
	PrevModRevisionGap    int64
	PrevVersion           int64
	PrevLease             int64
	KVObserved            bool
}

func TestWatchSingleOversizedEventDoesNotFragmentDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := watchSingleOversizedEventOutcome{
		CreatedEnvelope:       expectedFragmentControlEnvelope(true, true, 0),
		ResponseEnvelope:      expectedFragmentControlEnvelope(false, false, 1),
		PutRevisionGap:        1,
		ResponseHeaderAtPut:   true,
		ResponseOverTwoMiB:    true,
		Type:                  mvccpb.PUT,
		KeyMatches:            true,
		ValueMatches:          true,
		CreateRevisionGap:     0,
		ModRevisionGap:        1,
		Version:               2,
		Lease:                 0,
		PrevPresent:           true,
		PrevKeyMatches:        true,
		PrevValueMatches:      true,
		PrevCreateRevisionGap: 0,
		PrevModRevisionGap:    0,
		PrevVersion:           1,
		PrevLease:             0,
		KVObserved:            true,
	}
	referenceOutcome := runWatchSingleOversizedEventScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome,
		runWatchSingleOversizedEventScenario(t, compatEndpoint(t), "kubebrain"))
}

func runWatchSingleOversizedEventScenario(t *testing.T, endpoint, instance string) watchSingleOversizedEventOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	key := []byte(fmt.Sprintf("/dbaas-watch-single-oversized/%s/%d", instance, time.Now().UnixNano()))
	oldValue := []byte(strings.Repeat("o", 1024*1024))
	newValue := []byte(strings.Repeat("n", 1024*1024))
	kv := etcdserverpb.NewKVClient(conn)
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: oldValue})
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, StartRevision: seed.Header.Revision + 1, PrevKv: true, Fragment: true,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.NotNil(t, created.Header)

	updated, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: newValue})
	require.NoError(t, err)
	require.NotNil(t, updated.Header)
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, response.Events, 1)
	event := response.Events[0]

	outcome := watchSingleOversizedEventOutcome{
		CreatedEnvelope:     observeWatchControlResponse(created, seed.Header),
		ResponseEnvelope:    observeWatchControlResponse(response, seed.Header),
		PutRevisionGap:      updated.Header.Revision - seed.Header.Revision,
		ResponseHeaderAtPut: response.Header != nil && response.Header.Revision == updated.Header.Revision,
		ResponseOverTwoMiB:  proto.Size(response) > 2*1024*1024,
		Type:                event.Type,
		KVObserved:          event.Kv != nil,
		PrevPresent:         event.PrevKv != nil,
	}
	if event.Kv != nil {
		outcome.KeyMatches = bytes.Equal(event.Kv.Key, key)
		outcome.ValueMatches = bytes.Equal(event.Kv.Value, newValue)
		outcome.CreateRevisionGap = event.Kv.CreateRevision - seed.Header.Revision
		outcome.ModRevisionGap = event.Kv.ModRevision - seed.Header.Revision
		outcome.Version = event.Kv.Version
		outcome.Lease = event.Kv.Lease
	}
	if event.PrevKv != nil {
		outcome.PrevKeyMatches = bytes.Equal(event.PrevKv.Key, key)
		outcome.PrevValueMatches = bytes.Equal(event.PrevKv.Value, oldValue)
		outcome.PrevCreateRevisionGap = event.PrevKv.CreateRevision - seed.Header.Revision
		outcome.PrevModRevisionGap = event.PrevKv.ModRevision - seed.Header.Revision
		outcome.PrevVersion = event.PrevKv.Version
		outcome.PrevLease = event.PrevKv.Lease
	}
	require.NoError(t, stream.CloseSend())
	return outcome
}
