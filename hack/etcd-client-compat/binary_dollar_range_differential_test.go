package compat

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type dollarKeyRangeOutcome struct {
	LowerValue      string
	CurrentCount    int64
	HistoricalValue string
	PrefixCount     int64
	StreamCount     int64
}

type dollarRevisionCollisionOutcome struct {
	UpdatedCount           int64
	UpdatedShortValue      string
	UpdatedForeignValue    string
	UpdatedStreamCount     int64
	DeletedCurrentCount    int64
	DeletedForeignValue    string
	HistoricalCount        int64
	HistoricalShortValue   string
	HistoricalForeignValue string
}

// TestDollarRevisionCollisionDifferentialAgainstReferenceEtcd constructs a
// real legacy physical collision through client/v3 alone. A probe write yields
// a revision strictly between the shorter key's create and later update/delete;
// embedding that revision as big-endian bytes after '$' makes the extension key
// sort between the shorter key's physical versions in KubeBrain.
func TestDollarRevisionCollisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run dollar revision-collision differential tests")
	}
	referenceOutcome := runDollarRevisionCollisionScenario(t, reference, "reference")
	require.Equal(t, referenceOutcome, runDollarRevisionCollisionScenario(t, compatEndpoint(t), "kubebrain"))
}

func runDollarRevisionCollisionScenario(t *testing.T, endpoint, instance string) dollarRevisionCollisionOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	base := []byte(testPrefix(t) + "/" + instance + "/collision/")
	shortKey := append(append([]byte(nil), base...), 'a')
	probeKey := append(append([]byte(nil), base...), []byte("probe")...)
	rangeEnd := append(append([]byte(nil), shortKey...), 0xff)
	cleanupEnd := append(append([]byte(nil), base...), 0xff)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: base, RangeEnd: cleanupEnd})
	})

	created, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: shortKey, Value: []byte("short-v1")})
	require.NoError(t, err)
	probe, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: probeKey, Value: []byte("probe")})
	require.NoError(t, err)
	require.Greater(t, probe.Header.Revision, created.Header.Revision)
	boundary := make([]byte, 8)
	binary.BigEndian.PutUint64(boundary, uint64(probe.Header.Revision))
	foreignKey := append(append(append([]byte(nil), shortKey...), '$'), boundary...)
	foreignKey = append(foreignKey, 'x')
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: foreignKey, Value: []byte("foreign")})
	require.NoError(t, err)
	updated, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: shortKey, Value: []byte("short-v2")})
	require.NoError(t, err)
	require.Greater(t, updated.Header.Revision, probe.Header.Revision)

	current, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: shortKey, RangeEnd: rangeEnd})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 2)
	stream, err := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{Key: shortKey, RangeEnd: rangeEnd})
	require.NoError(t, err)
	var streamCount int64
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			require.ErrorIs(t, recvErr, io.EOF)
			break
		}
		streamCount += int64(len(response.GetRangeResponse().GetKvs()))
	}

	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: shortKey})
	require.NoError(t, err)
	deleted, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: shortKey, RangeEnd: rangeEnd})
	require.NoError(t, err)
	require.Len(t, deleted.Kvs, 1)
	historical, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: shortKey, RangeEnd: rangeEnd, Revision: updated.Header.Revision,
	})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 2)

	return dollarRevisionCollisionOutcome{
		UpdatedCount: current.Count, UpdatedShortValue: string(current.Kvs[0].Value),
		UpdatedForeignValue: string(current.Kvs[1].Value), UpdatedStreamCount: streamCount,
		DeletedCurrentCount: deleted.Count, DeletedForeignValue: string(deleted.Kvs[0].Value),
		HistoricalCount: historical.Count, HistoricalShortValue: string(historical.Kvs[0].Value),
		HistoricalForeignValue: string(historical.Kvs[1].Value),
	}
}

// TestDollarKeyNarrowRangeDifferentialAgainstReferenceEtcd pins arbitrary etcd
// keys containing KubeBrain's legacy '$' object-key delimiter. The scanner unit
// test forces the TiKV split; this black-box layer fixes the client-visible
// exact/history/prefix/RangeStream contract against upstream etcd.
func TestDollarKeyNarrowRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run dollar-key range differential tests")
	}
	referenceOutcome := runDollarKeyNarrowRangeScenario(t, reference, "reference")
	require.Equal(t, referenceOutcome, runDollarKeyNarrowRangeScenario(t, compatEndpoint(t), "kubebrain"))
}

func runDollarKeyNarrowRangeScenario(t *testing.T, endpoint, instance string) dollarKeyRangeOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	prefix := []byte(testPrefix(t) + "/" + instance + "/a")
	lower := append([]byte(nil), prefix...)
	target := append(append([]byte(nil), prefix...), []byte("$target")...)
	rangeEnd := append(append([]byte(nil), target...), 0xff)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: lower, RangeEnd: rangeEnd})
	})

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: lower, Value: []byte("lower")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: target, Value: []byte("v1")})
	require.NoError(t, err)
	updated, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: target, Value: []byte("v2")})
	require.NoError(t, err)
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: target})
	require.NoError(t, err)

	lowerResponse, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: lower})
	require.NoError(t, err)
	require.Len(t, lowerResponse.Kvs, 1)
	current, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: target})
	require.NoError(t, err)
	historical, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: target, Revision: updated.Header.Revision})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	prefixResponse, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: target, RangeEnd: rangeEnd})
	require.NoError(t, err)
	stream, err := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{Key: target, RangeEnd: rangeEnd})
	require.NoError(t, err)
	var streamCount int64
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			require.ErrorIs(t, recvErr, io.EOF)
			break
		}
		streamCount += int64(len(response.GetRangeResponse().GetKvs()))
	}
	return dollarKeyRangeOutcome{
		LowerValue: string(lowerResponse.Kvs[0].Value), CurrentCount: current.Count,
		HistoricalValue: string(historical.Kvs[0].Value), PrefixCount: prefixResponse.Count,
		StreamCount: streamCount,
	}
}
