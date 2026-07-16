package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

func bootstrapAuthForToken(t *testing.T, server *RPCServer) (*authManager, *authTokenManager) {
	t.Helper()
	ctx := context.Background()
	manager := newAuthManager(server.backend)
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, manager.roleAdd(ctx, "root"))
	require.NoError(t, manager.userGrantRole(ctx, "root", "root"))
	require.NoError(t, manager.enable(ctx))
	return manager, newAuthTokenManager(server.backend)
}

func TestAuthTokenUsesPerUserInvalidation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager, tokens := bootstrapAuthForToken(t, server)
	ctx := context.Background()

	_, err := tokens.authenticate(ctx, "root", "wrong")
	require.ErrorIs(t, err, rpctypes.ErrAuthFailed)
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "nopass", Options: &authpb.UserAddOptions{NoPassword: true},
	}))
	_, err = tokens.authenticate(ctx, "nopass", "password")
	require.ErrorIs(t, err, errNoPasswordUser)
	token, err := tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)
	claims, err := tokens.verify(ctx, token)
	require.NoError(t, err)
	require.Equal(t, "root", claims.Username)

	// etcd simple tokens survive unrelated auth mutations.
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	_, err = tokens.verify(ctx, token)
	require.NoError(t, err)
	// Password changes invalidate only this user's existing tokens.
	require.NoError(t, manager.userChangePassword(ctx, "root", "changed", ""))
	_, err = tokens.verify(ctx, token)
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
	newToken, err := tokens.authenticate(ctx, "root", "secret")
	require.ErrorIs(t, err, rpctypes.ErrAuthFailed)
	newToken, err = tokens.authenticate(ctx, "root", "changed")
	require.NoError(t, err)
	require.NotEqual(t, token, newToken)
	_, err = tokens.verify(ctx, newToken)
	require.NoError(t, err)

	parts := strings.Split(newToken, ".")
	require.Len(t, parts, 2)
	replacement := "A"
	if parts[1][0] == 'A' {
		replacement = "B"
	}
	tampered := parts[0] + "." + replacement + parts[1][1:]
	_, err = tokens.verify(ctx, tampered)
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
}

func TestAuthTokenVerificationFailsClosedWithoutSigningKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, tokens := bootstrapAuthForToken(t, server)
	ctx := context.Background()
	token, err := tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)

	key, err := server.backend.InternalGet(ctx, authTokenSigningKey)
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalCAS(ctx, []backend.InternalCASOp{{
		Key: authTokenSigningKey, Expected: key, ExpectedExists: true, Delete: true,
	}}))
	_, err = tokens.verify(ctx, token)
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
	_, err = server.backend.InternalGet(ctx, authTokenSigningKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound, "verification must not silently rotate a missing signing key")
}

func TestAuthTokenSigningKeySurvivesManagerRecreationAndExpires(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, tokens := bootstrapAuthForToken(t, server)
	ctx := context.Background()
	now := time.Unix(2_000_000_000, 0)
	tokens.now = func() time.Time { return now }
	token, err := tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)

	recreated := newAuthTokenManager(server.backend)
	recreated.now = func() time.Time { return now.Add(time.Minute) }
	_, err = recreated.verify(ctx, token)
	require.NoError(t, err, "persisted signing key must verify after process-local manager recreation")
	recreated.now = func() time.Time { return now.Add(authTokenTTL + time.Second) }
	_, err = recreated.verify(ctx, token)
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
}

func TestAuthTokenRejectsWhileDisabled(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	tokens := newAuthTokenManager(server.backend)
	_, err := tokens.authenticate(context.Background(), "root", "secret")
	require.ErrorIs(t, err, rpctypes.ErrAuthNotEnabled)
}

func TestAuthTokenLazilyMigratesLegacyUserGeneration(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	manager := newAuthManager(server.backend)
	password, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	require.NoError(t, err)
	initial, err := manager.repo.load(ctx)
	require.NoError(t, err)
	_, err = manager.repo.mutate(ctx, initial.Config, authMutation{
		Key:   authRecordKey(authUsersKey, "root"),
		Value: &authpb.User{Name: []byte("root"), Password: password, Roles: []string{"root"}},
	})
	require.NoError(t, err)
	require.NoError(t, manager.enable(ctx))
	before, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Nil(t, before.TokenGenerations["root"])

	tokens := newAuthTokenManager(server.backend)
	token, err := tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)
	after, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Config.Revision, after.Config.Revision)
	require.Len(t, after.TokenGenerations["root"].Password, authUserTokenGenerationBytes)
	_, err = tokens.verify(ctx, token)
	require.NoError(t, err)
}
