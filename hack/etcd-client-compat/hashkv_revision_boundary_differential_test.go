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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type hashKVRevisionOutcome struct {
	Name                       string
	Code                       string
	Message                    string
	HashRevisionMatchesRequest bool
	LatestHashAtOrAfterPut     bool
	CompactRevisionValid       bool
	HeaderAtCurrentRevision    bool
	NegativeRevisionHash       uint32
}

func TestHashKVRevisionBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runHashKVRevisionBoundaryScenario(t, reference, "etcd")
	require.Equal(t, []hashKVRevisionOutcome{
		{
			Name:                       "negative-one",
			Code:                       "OK",
			HashRevisionMatchesRequest: true,
			CompactRevisionValid:       true,
			HeaderAtCurrentRevision:    true,
			NegativeRevisionHash:       0x40a4756d,
		},
		{
			Name:                    "latest-zero",
			Code:                    "OK",
			LatestHashAtOrAfterPut:  true,
			CompactRevisionValid:    true,
			HeaderAtCurrentRevision: true,
		},
		{
			Name:                       "current",
			Code:                       "OK",
			HashRevisionMatchesRequest: true,
			CompactRevisionValid:       true,
			HeaderAtCurrentRevision:    true,
		},
		{
			Name:    "future",
			Code:    "OutOfRange",
			Message: "etcdserver: mvcc: required revision is a future revision",
		},
		{
			Name:    "max-int",
			Code:    "OutOfRange",
			Message: "etcdserver: mvcc: required revision is a future revision",
		},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runHashKVRevisionBoundaryScenario(t, compatEndpoint(t), "kubebrain"))
}

func runHashKVRevisionBoundaryScenario(t *testing.T, endpoint, instance string) []hashKVRevisionOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-hashkv-revision/%s/%d", instance, time.Now().UnixNano())
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(key)})
	})

	tests := []struct {
		name     string
		revision int64
	}{
		{name: "negative-one", revision: -1},
		{name: "latest-zero"},
		{name: "current", revision: put.Header.Revision},
		{name: "future", revision: put.Header.Revision + 100},
		{name: "max-int", revision: math.MaxInt64},
	}
	outcomes := make([]hashKVRevisionOutcome, 0, len(tests))
	for _, test := range tests {
		resp, callErr := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: test.revision})
		outcome := hashKVRevisionOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		if resp != nil {
			if test.revision != 0 {
				outcome.HashRevisionMatchesRequest = resp.HashRevision == test.revision
			} else {
				outcome.LatestHashAtOrAfterPut = resp.HashRevision >= put.Header.Revision
			}
			outcome.CompactRevisionValid = resp.CompactRevision == -1 || resp.CompactRevision > 0
			outcome.HeaderAtCurrentRevision = resp.Header.Revision >= put.Header.Revision
			if test.revision == -1 {
				outcome.NegativeRevisionHash = resp.Hash
			}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}
