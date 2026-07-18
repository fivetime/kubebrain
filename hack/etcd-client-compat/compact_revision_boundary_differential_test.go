package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type compactRevisionBoundaryOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestCompactRevisionBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []compactRevisionBoundaryOutcome{
		{Name: "zero-logical", Code: "OutOfRange", Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "zero-physical", Code: "OutOfRange", Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "negative-physical", Code: "OutOfRange", Message: "etcdserver: mvcc: required revision has been compacted"},
		{Name: "max-logical", Code: "OutOfRange", Message: "etcdserver: mvcc: required revision is a future revision"},
		{Name: "max-physical", Code: "OutOfRange", Message: "etcdserver: mvcc: required revision is a future revision"},
	}
	referenceOutcomes := runCompactRevisionBoundaryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runCompactRevisionBoundaryScenario(t, compatEndpoint(), "kubebrain"))
}

func runCompactRevisionBoundaryScenario(
	t *testing.T,
	endpoint string,
	instance string,
) []compactRevisionBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-compact-revision/%s/%d", instance, time.Now().UnixNano())
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(key)})
	})
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put.Header.Revision})
	require.NoError(t, err)

	tests := []struct {
		name     string
		revision int64
		physical bool
	}{
		{name: "zero-logical"},
		{name: "zero-physical", physical: true},
		{name: "negative-physical", revision: -1, physical: true},
		{name: "max-logical", revision: math.MaxInt64},
		{name: "max-physical", revision: math.MaxInt64, physical: true},
	}
	outcomes := make([]compactRevisionBoundaryOutcome, 0, len(tests))
	for _, test := range tests {
		_, callErr := kv.Compact(ctx, &etcdserverpb.CompactionRequest{
			Revision: test.revision,
			Physical: test.physical,
		})
		require.Error(t, callErr)
		require.Equal(t, codes.OutOfRange, status.Code(callErr))
		outcomes = append(outcomes, compactRevisionBoundaryOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
