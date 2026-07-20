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
)

type authHeaderOutcome struct {
	StatusRevisionMatches  bool
	RoleAddRevisionMatches bool
	RoleGetRevisionMatches bool
	ClusterIDNonZero       bool
	MemberIDNonZero        bool
	RaftTermPositive       bool
}

func TestAuthHeaderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runAuthHeaderScenario(t, reference, "reference"),
		runAuthHeaderScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runAuthHeaderScenario(t *testing.T, endpoint, instance string) authHeaderOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	auth := etcdserverpb.NewAuthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	suffix := time.Now().UnixNano()
	key := fmt.Sprintf("/dbaas-auth-header/%s/%d", instance, suffix)
	role := fmt.Sprintf("dbaas-auth-header-%s-%d", instance, suffix)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(key)})
		_, _ = auth.RoleDelete(cleanupCtx, &etcdserverpb.AuthRoleDeleteRequest{Role: role})
	})

	statusResponse, err := auth.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	roleAddResponse, err := auth.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: role})
	require.NoError(t, err)
	roleGetResponse, err := auth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: role})
	require.NoError(t, err)

	header := statusResponse.Header
	require.NotNil(t, header)
	return authHeaderOutcome{
		StatusRevisionMatches:  header.Revision == put.Header.Revision,
		RoleAddRevisionMatches: roleAddResponse.Header.GetRevision() == put.Header.Revision,
		RoleGetRevisionMatches: roleGetResponse.Header.GetRevision() == put.Header.Revision,
		ClusterIDNonZero:       header.ClusterId != 0,
		MemberIDNonZero:        header.MemberId != 0,
		RaftTermPositive:       header.RaftTerm > 0,
	}
}
