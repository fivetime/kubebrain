package compat

import (
	"context"
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

type hashDifferentialOutcome struct {
	Name                    string
	Code                    string
	Message                 string
	HeaderAtCurrentRevision bool
	StableWithoutWrite      bool
	RevisionAdvanced        bool
	HashChangedAfterWrite   bool
}

func TestHashDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcome := runHashDifferentialScenario(t, reference)
	require.Equal(t, hashDifferentialOutcome{
		Name:                    "hash",
		Code:                    "OK",
		HeaderAtCurrentRevision: true,
		StableWithoutWrite:      true,
		RevisionAdvanced:        true,
		HashChangedAfterWrite:   true,
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, runHashDifferentialScenario(t, compatEndpoint()))
}

func runHashDifferentialScenario(t *testing.T, endpoint string) hashDifferentialOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := []byte(testPrefix(t) + "/hash")
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	first, callErr := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	outcome := hashDifferentialOutcome{
		Name:    "hash",
		Code:    status.Code(callErr).String(),
		Message: status.Convert(callErr).Message(),
	}
	if callErr != nil {
		return outcome
	}
	second, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	update, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after")})
	require.NoError(t, err)
	third, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)

	outcome.HeaderAtCurrentRevision = first.Header.Revision >= put.Header.Revision
	outcome.StableWithoutWrite = second.Header.Revision == first.Header.Revision && second.Hash == first.Hash
	outcome.RevisionAdvanced = third.Header.Revision >= update.Header.Revision &&
		third.Header.Revision > second.Header.Revision
	outcome.HashChangedAfterWrite = third.Hash != second.Hash
	return outcome
}
