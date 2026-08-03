package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type hashHeaderEnvelopeOutcome struct {
	HashHeaderPresent            bool
	HashKVHeaderPresent          bool
	ClusterIDPositive            bool
	MemberIDPositive             bool
	RaftTermPositive             bool
	SameClusterAcrossMethods     bool
	SameMemberAcrossMethods      bool
	SameTermAcrossMethods        bool
	HashRevisionAtOrAfterPut     bool
	HashKVHeaderAtRequestedRev   bool
	HashKVHashAtRequestedRev     bool
	HashKVCompactRevisionNotPast bool
}

func TestMaintenanceHashHeaderEnvelopeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := hashHeaderEnvelopeOutcome{
		HashHeaderPresent:            true,
		HashKVHeaderPresent:          true,
		ClusterIDPositive:            true,
		MemberIDPositive:             true,
		RaftTermPositive:             true,
		SameClusterAcrossMethods:     true,
		SameMemberAcrossMethods:      true,
		SameTermAcrossMethods:        true,
		HashRevisionAtOrAfterPut:     true,
		HashKVHeaderAtRequestedRev:   true,
		HashKVHashAtRequestedRev:     true,
		HashKVCompactRevisionNotPast: true,
	}
	referenceOutcome := readHashHeaderEnvelope(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, readHashHeaderEnvelope(t, compatEndpoint(t)))
}

func readHashHeaderEnvelope(t *testing.T, endpoint string) hashHeaderEnvelopeOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	key := []byte(testPrefix(t) + "/hash-header-envelope")
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("value"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: key,
		})
	})
	hash, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	hashKV, err := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: put.GetHeader().GetRevision()})
	require.NoError(t, err)
	hashHeader := hash.GetHeader()
	hashKVHeader := hashKV.GetHeader()
	return hashHeaderEnvelopeOutcome{
		HashHeaderPresent:            hashHeader != nil,
		HashKVHeaderPresent:          hashKVHeader != nil,
		ClusterIDPositive:            hashHeader.GetClusterId() > 0 && hashKVHeader.GetClusterId() > 0,
		MemberIDPositive:             hashHeader.GetMemberId() > 0 && hashKVHeader.GetMemberId() > 0,
		RaftTermPositive:             hashHeader.GetRaftTerm() > 0 && hashKVHeader.GetRaftTerm() > 0,
		SameClusterAcrossMethods:     hashHeader.GetClusterId() == hashKVHeader.GetClusterId(),
		SameMemberAcrossMethods:      hashHeader.GetMemberId() == hashKVHeader.GetMemberId(),
		SameTermAcrossMethods:        hashHeader.GetRaftTerm() == hashKVHeader.GetRaftTerm(),
		HashRevisionAtOrAfterPut:     hashHeader.GetRevision() >= put.GetHeader().GetRevision(),
		HashKVHeaderAtRequestedRev:   hashKVHeader.GetRevision() == put.GetHeader().GetRevision(),
		HashKVHashAtRequestedRev:     hashKV.GetHashRevision() == put.GetHeader().GetRevision(),
		HashKVCompactRevisionNotPast: hashKV.GetCompactRevision() <= hashKV.GetHashRevision(),
	}
}
