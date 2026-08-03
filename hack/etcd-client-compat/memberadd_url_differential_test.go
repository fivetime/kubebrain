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

type memberAddURLOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestMemberAddURLValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := memberAddURLOutcomes(t, reference)
	require.Equal(t, []memberAddURLOutcome{
		{Name: "empty-list", Code: "InvalidArgument", Message: "etcdserver: given member URLs are invalid"},
		{Name: "missing-scheme", Code: "InvalidArgument", Message: "etcdserver: given member URLs are invalid"},
		{Name: "unsupported-scheme", Code: "InvalidArgument", Message: "etcdserver: given member URLs are invalid"},
		{Name: "missing-host", Code: "InvalidArgument", Message: "etcdserver: given member URLs are invalid"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, memberAddURLOutcomes(t, compatEndpoint()))
}

func memberAddURLOutcomes(t *testing.T, endpoint string) []memberAddURLOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewClusterClient(conn)
	tests := []struct {
		name     string
		peerURLs []string
	}{
		{name: "empty-list"},
		{name: "missing-scheme", peerURLs: []string{"not-a-peer-url"}},
		{name: "unsupported-scheme", peerURLs: []string{"ftp://127.0.0.1:2380"}},
		{name: "missing-host", peerURLs: []string{"http://"}},
	}
	outcomes := make([]memberAddURLOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, callErr := client.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{PeerURLs: test.peerURLs})
		cancel()
		outcomes = append(outcomes, memberAddURLOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
