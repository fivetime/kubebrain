package compat

import (
	"context"
	"fmt"
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

type memberUpdatePeerURLConflictOutcome struct {
	Code    string
	Message string
}

func TestMemberUpdatePeerURLConflictDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := memberUpdatePeerURLConflictOutcomeForEndpoint(t, reference, true)
	require.Equal(t, memberUpdatePeerURLConflictOutcome{
		Code: "FailedPrecondition", Message: "etcdserver: Peer URLs already exists",
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, memberUpdatePeerURLConflictOutcomeForEndpoint(t, compatEndpoint(t), false))
}

func memberUpdatePeerURLConflictOutcomeForEndpoint(t *testing.T, endpoint string, allowFixtureMember bool) memberUpdatePeerURLConflictOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewClusterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listResponse, err := client.MemberList(ctx, &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	if len(listResponse.Members) < 2 && allowFixtureMember {
		addResponse, addErr := client.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{
			// etcd tombstones removed member IDs, which are derived from peer URLs.
			// A unique URL keeps repeated differential runs from reusing a removed ID.
			PeerURLs: []string{fmt.Sprintf("http://a3477-%d.invalid:2380", time.Now().UnixNano())}, IsLearner: true,
		})
		require.NoError(t, addErr)
		require.NotNil(t, addResponse.GetMember())
		addedID := addResponse.GetMember().GetID()
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, cleanupErr := client.MemberRemove(cleanupCtx, &etcdserverpb.MemberRemoveRequest{ID: addedID})
			require.NoError(t, cleanupErr)
		})
		listResponse, err = client.MemberList(ctx, &etcdserverpb.MemberListRequest{})
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, len(listResponse.Members), 2)
	target, conflicting := listResponse.Members[0], listResponse.Members[1]
	require.NotEmpty(t, conflicting.GetPeerURLs())

	_, callErr := client.MemberUpdate(ctx, &etcdserverpb.MemberUpdateRequest{
		ID: target.GetID(), PeerURLs: append([]string(nil), conflicting.GetPeerURLs()...),
	})
	return memberUpdatePeerURLConflictOutcome{
		Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
	}
}
