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

type memberAddPeerURLConflictOutcome struct {
	Code    string
	Message string
}

func TestMemberAddPeerURLConflictDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := memberAddPeerURLConflictOutcomeForEndpoint(t, reference)
	require.Equal(t, memberAddPeerURLConflictOutcome{
		Code: "FailedPrecondition", Message: "etcdserver: Peer URLs already exists",
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, memberAddPeerURLConflictOutcomeForEndpoint(t, compatEndpoint()))
}

func TestMemberAddWhitespaceNormalizedPeerURLConflictDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := memberAddPeerURLConflictOutcomeForEndpointWithTransform(t, reference, func(peerURL string) string {
		return " \t" + peerURL + "\n "
	})
	require.Equal(t, memberAddPeerURLConflictOutcome{
		Code: "FailedPrecondition", Message: "etcdserver: Peer URLs already exists",
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, memberAddPeerURLConflictOutcomeForEndpointWithTransform(t, compatEndpoint(), func(peerURL string) string {
		return " \t" + peerURL + "\n "
	}))
}

func memberAddPeerURLConflictOutcomeForEndpoint(t *testing.T, endpoint string) memberAddPeerURLConflictOutcome {
	return memberAddPeerURLConflictOutcomeForEndpointWithTransform(t, endpoint, func(peerURL string) string { return peerURL })
}

func memberAddPeerURLConflictOutcomeForEndpointWithTransform(t *testing.T, endpoint string, transform func(string) string) memberAddPeerURLConflictOutcome {
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
	require.NotEmpty(t, listResponse.Members)
	require.NotEmpty(t, listResponse.Members[0].GetPeerURLs())
	_, callErr := client.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{PeerURLs: []string{
		transform(listResponse.Members[0].GetPeerURLs()[0]),
		"http://a3478-new.invalid:2380",
	}})
	require.Error(t, callErr)
	return memberAddPeerURLConflictOutcome{
		Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
	}
}
