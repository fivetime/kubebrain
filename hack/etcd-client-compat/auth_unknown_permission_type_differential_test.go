package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type unknownPermissionTypeOutcome struct {
	GrantCode       string
	StoredCount     int
	StoredType      int32
	ReplacementCode string
	ReplacementType int32
	RevokeCode      string
	RemainingCount  int
}

// TestAuthUnknownPermissionTypeDifferentialAgainstReferenceEtcd fixes an
// intentionally permissive protobuf-enum edge in etcd's public Auth API.
// Unknown permission types are persisted and returned verbatim; they must not
// be silently normalized to one of READ, WRITE, or READWRITE.
func TestAuthUnknownPermissionTypeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := unknownPermissionTypeOutcome{
		GrantCode:       "OK",
		StoredCount:     1,
		StoredType:      99,
		ReplacementCode: "OK",
		ReplacementType: 99,
		RevokeCode:      "OK",
		RemainingCount:  0,
	}
	referenceOutcome := runUnknownPermissionTypeScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runUnknownPermissionTypeScenario(t, compatEndpoint(t)))
}

func runUnknownPermissionTypeScenario(t *testing.T, endpoint string) unknownPermissionTypeOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	auth := etcdserverpb.NewAuthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	role := fmt.Sprintf("dbaas-unknown-permission-%d", time.Now().UnixNano())
	_, err = auth.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: role})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, cleanupErr := auth.RoleDelete(cleanupCtx, &etcdserverpb.AuthRoleDeleteRequest{Role: role})
		require.NoError(t, cleanupErr)
	})

	key := []byte("/dbaas-unknown-permission/key")
	rangeEnd := []byte("/dbaas-unknown-permission/kez")
	unknown := authpb.Permission_Type(99)
	_, grantErr := auth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: role,
		Perm: &authpb.Permission{PermType: unknown, Key: key, RangeEnd: rangeEnd},
	})
	stored, err := auth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: role})
	require.NoError(t, err)

	// Exercise etcd's same-range replacement path before restoring the unknown
	// value, so both insert and update preserve the enum exactly.
	_, err = auth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: role,
		Perm: &authpb.Permission{PermType: authpb.READ, Key: key, RangeEnd: rangeEnd},
	})
	require.NoError(t, err)
	_, replacementErr := auth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: role,
		Perm: &authpb.Permission{PermType: unknown, Key: key, RangeEnd: rangeEnd},
	})
	replaced, err := auth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: role})
	require.NoError(t, err)

	_, revokeErr := auth.RoleRevokePermission(ctx, &etcdserverpb.AuthRoleRevokePermissionRequest{
		Role: role, Key: key, RangeEnd: rangeEnd,
	})
	remaining, err := auth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: role})
	require.NoError(t, err)

	outcome := unknownPermissionTypeOutcome{
		GrantCode: status.Code(grantErr).String(), StoredCount: len(stored.Perm),
		ReplacementCode: status.Code(replacementErr).String(), RevokeCode: status.Code(revokeErr).String(),
		RemainingCount: len(remaining.Perm),
	}
	if len(stored.Perm) == 1 {
		outcome.StoredType = int32(stored.Perm[0].PermType)
	}
	if len(replaced.Perm) == 1 {
		outcome.ReplacementType = int32(replaced.Perm[0].PermType)
	}
	return outcome
}
