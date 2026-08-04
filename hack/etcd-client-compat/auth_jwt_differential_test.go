package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type jwtAuthOutcome struct {
	TokenSegments int
	InitialPut    bool
	OldToken      authErrorOutcome
	ReauthPut     bool
}

func TestJWTAuthDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_JWT_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_JWT_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_JWT_ETCD_ENDPOINT and KUBEBRAIN_JWT_ETCD_ENDPOINT")
	}
	referenceResult := runJWTAuthScenario(t, reference, "reference")
	kubebrainResult := runJWTAuthScenario(t, kubebrain, "kubebrain")
	require.Equal(t, referenceResult, kubebrainResult)
}

func runJWTAuthScenario(t *testing.T, endpoint, name string) jwtAuthOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")
	key := "/jwt-differential/" + name
	_, err := bootstrap.UserAdd(ctx, "root", "root-secret")
	if err != nil {
		require.Contains(t, err.Error(), "user name already exists")
	}
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		root, clientErr := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
			Username: "root", Password: "root-secret",
		})
		require.NoError(t, clientErr)
		_, _ = root.AuthDisable(cleanupCtx)
		if users, listErr := bootstrap.UserList(cleanupCtx); listErr == nil {
			for _, user := range users.Users {
				if detail, getErr := bootstrap.UserGet(cleanupCtx, user); getErr == nil {
					for _, role := range detail.Roles {
						_, _ = bootstrap.UserRevokeRole(cleanupCtx, user, role)
					}
				}
				_, _ = bootstrap.UserDelete(cleanupCtx, user)
			}
		}
		if roles, listErr := bootstrap.RoleList(cleanupCtx); listErr == nil {
			for _, role := range roles.Roles {
				_, _ = bootstrap.RoleDelete(cleanupCtx, role)
			}
		}
		_, _ = bootstrap.Delete(cleanupCtx, key)
		require.NoError(t, root.Close())
		users, listUsersErr := bootstrap.UserList(cleanupCtx)
		require.NoError(t, listUsersErr)
		require.Empty(t, users.Users)
		roles, listRolesErr := bootstrap.RoleList(cleanupCtx)
		require.NoError(t, listRolesErr)
		require.Empty(t, roles.Roles)
		remaining, getErr := bootstrap.Get(cleanupCtx, key)
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
	})
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	auth, err := bootstrap.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)
	oldClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Token: auth.Token,
	})
	require.NoError(t, err)
	defer oldClient.Close()
	_, initialPutErr := oldClient.Put(ctx, key, "initial")

	root := authClient(t, endpoint, "root", "root-secret")
	_, err = root.RoleAdd(ctx, fmt.Sprintf("jwt-revision-invalidator-%s-%d", name, time.Now().UnixNano()))
	require.NoError(t, err)
	_, oldTokenErr := oldClient.Get(ctx, key)
	reauthenticated := authClient(t, endpoint, "root", "root-secret")
	_, reauthPutErr := reauthenticated.Put(ctx, key, "reauthenticated")

	return jwtAuthOutcome{
		TokenSegments: len(strings.Split(auth.Token, ".")),
		InitialPut:    initialPutErr == nil,
		OldToken:      authError(oldTokenErr),
		ReauthPut:     reauthPutErr == nil,
	}
}
