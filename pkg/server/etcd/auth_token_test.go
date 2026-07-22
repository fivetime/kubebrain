package etcd

import (
	"context"
	"encoding/base64"
	"strings"
	"sync"
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
	_, err = tokens.authenticate(ctx, "root", "secret")
	require.ErrorIs(t, err, rpctypes.ErrAuthFailed)
	newToken, err := tokens.authenticate(ctx, "root", "changed")
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

func TestAuthTokenAuthenticateRetriesAfterAuthRevisionChange(t *testing.T) {
	t.Run("password change rejects stale credentials", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		manager, tokens := bootstrapAuthForToken(t, server)
		ctx := context.Background()
		var once sync.Once
		tokens.afterPasswordCheck = func() {
			once.Do(func() {
				require.NoError(t, manager.userChangePassword(ctx, "root", "changed", ""))
			})
		}

		_, err := tokens.authenticate(ctx, "root", "secret")
		require.ErrorIs(t, err, rpctypes.ErrAuthFailed)
		token, err := tokens.authenticate(ctx, "root", "changed")
		require.NoError(t, err)
		_, err = tokens.verify(ctx, token)
		require.NoError(t, err)
	})

	t.Run("unrelated auth mutation signs latest revision", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		manager, tokens := bootstrapAuthForToken(t, server)
		ctx := context.Background()
		var once sync.Once
		tokens.afterPasswordCheck = func() {
			once.Do(func() {
				require.NoError(t, manager.roleAdd(ctx, "reader"))
			})
		}

		token, err := tokens.authenticate(ctx, "root", "secret")
		require.NoError(t, err)
		claims, err := tokens.verify(ctx, token)
		require.NoError(t, err)
		snapshot, err := tokens.snapshots.current(ctx)
		require.NoError(t, err)
		require.Equal(t, snapshot.Config.Revision, claims.Revision)
	})
}

func TestAuthTokenRejectsSignedUnknownOrTrailingClaims(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, tokens := bootstrapAuthForToken(t, server)
	ctx := context.Background()
	token, err := tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	require.Len(t, parts, 2)
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, byte('}'), payload[len(payload)-1])

	unknownPayload := append([]byte{}, payload[:len(payload)-1]...)
	unknownPayload = append(unknownPayload, []byte(`,"x":true}`)...)
	_, err = tokens.verify(ctx, signedAuthTokenForPayload(t, server, unknownPayload))
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)

	trailingPayload := append(append([]byte{}, payload...), []byte(`{"u":"root"}`)...)
	_, err = tokens.verify(ctx, signedAuthTokenForPayload(t, server, trailingPayload))
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
}

func signedAuthTokenForPayload(t *testing.T, server *RPCServer, payload []byte) string {
	t.Helper()
	key, err := server.backend.InternalGet(context.Background(), authTokenSigningKey)
	require.NoError(t, err)
	signature := signAuthToken(key, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature)
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

func TestAuthTokenUsesConfiguredTTL(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetAuthConfiguration("simple", uint(bcrypt.DefaultCost), 2)
	ctx := context.Background()
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.enable(ctx))
	now := time.Unix(2_000_000_000, 0)
	server.tokens.now = func() time.Time { return now }
	token, err := server.tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)

	server.tokens.now = func() time.Time { return now.Add(time.Second) }
	_, err = server.tokens.verify(ctx, token)
	require.NoError(t, err)
	server.tokens.now = func() time.Time { return now.Add(2 * time.Second) }
	_, err = server.tokens.verify(ctx, token)
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
