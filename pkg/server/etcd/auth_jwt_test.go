package etcd

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type jwtTestKeys struct {
	rsaPrivate, rsaPublic []byte
	ecPrivate, ecPublic   []byte
	edPrivate, edPublic   []byte
}

func generateJWTTestKeys(t *testing.T) jwtTestKeys {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ecDER, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)
	edDER, err := x509.MarshalPKCS8PrivateKey(edKey)
	require.NoError(t, err)
	publicPEM := func(key any) []byte {
		der, marshalErr := x509.MarshalPKIXPublicKey(key)
		require.NoError(t, marshalErr)
		return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	}
	return jwtTestKeys{
		rsaPrivate: pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rsaKey)}),
		rsaPublic:  publicPEM(&rsaKey.PublicKey),
		ecPrivate:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: ecDER}),
		ecPublic:   publicPEM(&ecKey.PublicKey),
		edPrivate:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: edDER}),
		edPublic:   publicPEM(edKey.Public()),
	}
}

func writeJWTKey(t *testing.T, name string, value []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, value, 0o600))
	return path
}

func TestJWTProviderAlgorithmsAndExpiry(t *testing.T) {
	keys := generateJWTTestKeys(t)
	tests := []struct {
		method  string
		private []byte
		public  []byte
	}{
		{method: "HS256", private: []byte("shared-secret")},
		{method: "RS256", private: keys.rsaPrivate, public: keys.rsaPublic},
		{method: "PS256", private: keys.rsaPrivate, public: keys.rsaPublic},
		{method: "ES256", private: keys.ecPrivate, public: keys.ecPublic},
		{method: "EdDSA", private: keys.edPrivate, public: keys.edPublic},
	}
	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			privatePath := writeJWTKey(t, "private.pem", tc.private)
			spec := fmt.Sprintf("jwt,sign-method=%s,priv-key=%s,ttl=2s", tc.method, privatePath)
			if tc.public != nil {
				spec += ",pub-key=" + writeJWTKey(t, "public.pem", tc.public)
			}
			provider, err := parseAuthTokenProvider(spec)
			require.NoError(t, err)
			now := time.Unix(2_000_000_000, 0)
			token, err := provider.issue("root", 17, now)
			require.NoError(t, err)
			claims, err := provider.verify(token, now.Add(time.Second))
			require.NoError(t, err)
			require.Equal(t, "root", claims.Username)
			require.Equal(t, uint64(17), claims.Revision)
			_, err = provider.verify(token, now.Add(2*time.Second))
			requireJWTError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
		})
	}
}

func TestJWTProviderPublicOnlyAndKeyMismatch(t *testing.T) {
	keys := generateJWTTestKeys(t)
	privatePath := writeJWTKey(t, "private.pem", keys.rsaPrivate)
	publicPath := writeJWTKey(t, "public.pem", keys.rsaPublic)
	signer, err := parseAuthTokenProvider("jwt,sign-method=RS256,priv-key=" + privatePath)
	require.NoError(t, err)
	verifier, err := parseAuthTokenProvider("jwt,sign-method=RS256,pub-key=" + publicPath)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	token, err := signer.issue("root", 7, now)
	require.NoError(t, err)
	_, err = verifier.verify(token, now)
	require.NoError(t, err)
	_, err = verifier.issue("root", 7, now)
	require.EqualError(t, err, "auth: JWT token provider is verify-only")

	other := generateJWTTestKeys(t)
	mismatch := "jwt,sign-method=RS256,priv-key=" + privatePath + ",pub-key=" + writeJWTKey(t, "other-public.pem", other.rsaPublic)
	require.EqualError(t, ValidateAuthTokenProvider(mismatch), "auth: public and private keys don't match")
}

func TestJWTProviderRevisionFloatCoercionMatchesEtcd(t *testing.T) {
	secretValue := []byte("shared-secret")
	secret := writeJWTKey(t, "secret", secretValue)
	provider, err := parseAuthTokenProvider("jwt,sign-method=HS256,priv-key=" + secret)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)

	const revision = uint64(1<<53 + 1)
	token, err := provider.issue("root", revision, now)
	require.NoError(t, err)
	claims, err := provider.verify(token, now)
	require.NoError(t, err)
	require.Equal(t, uint64(float64(revision)), claims.Revision)

	fractional := jwt.NewWithClaims(provider.method, jwt.MapClaims{
		"username": "root",
		"revision": 1.5,
		"exp":      now.Add(time.Minute).Unix(),
	})
	token, err = fractional.SignedString(secretValue)
	require.NoError(t, err)
	claims, err = provider.verify(token, now)
	require.NoError(t, err)
	require.Equal(t, uint64(1), claims.Revision)
}

func TestJWTProviderOptionSyntaxMatchesEtcd(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "jwt=keys")
	require.NoError(t, os.Mkdir(dir, 0o700))
	secret := filepath.Join(dir, "hmac=secret")
	require.NoError(t, os.WriteFile(secret, []byte("shared-secret"), 0o600))

	specWithEqualsPath := "jwt,sign-method=HS256,priv-key=" + secret
	require.EqualError(t,
		ValidateAuthTokenProvider(specWithEqualsPath),
		fmt.Sprintf("invalid auth token option %q", "priv-key="+secret),
	)

	plainSecret := writeJWTKey(t, "secret", []byte("shared-secret"))
	provider, err := parseAuthTokenProvider("jwt,=ignored,sign-method=HS256,priv-key=" + plainSecret)
	require.NoError(t, err)
	token, err := provider.issue("root", 1, time.Unix(2_000_000_000, 0))
	require.NoError(t, err)
	_, err = provider.verify(token, time.Unix(2_000_000_001, 0))
	require.NoError(t, err)
}

func TestAuthTokenProviderSimpleOptionsMatchEtcd(t *testing.T) {
	provider, err := parseAuthTokenProvider("simple,foo=bar,=ignored")
	require.NoError(t, err)
	require.Nil(t, provider)
	require.NoError(t, ValidateAuthTokenProvider("simple,foo=bar"))

	require.EqualError(t, ValidateAuthTokenProvider("simple,foo"), `invalid auth token option "foo"`)
	require.EqualError(t, ValidateAuthTokenProvider("simple,foo=bar=baz"), `invalid auth token option "foo=bar=baz"`)
	require.EqualError(t, ValidateAuthTokenProvider("simple,foo=bar,foo=baz"), `duplicate auth token option "foo"`)
}

func TestJWTProviderNonPositiveTTLMatchesEtcd(t *testing.T) {
	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	for _, ttl := range []string{"0s", "-1s"} {
		t.Run(ttl, func(t *testing.T) {
			provider, err := parseAuthTokenProvider("jwt,sign-method=HS256,priv-key=" + secret + ",ttl=" + ttl)
			require.NoError(t, err)
			want, err := time.ParseDuration(ttl)
			require.NoError(t, err)
			require.Equal(t, want, provider.ttl)
		})
	}

	require.ErrorContains(t,
		ValidateAuthTokenProvider("jwt,sign-method=HS256,priv-key="+secret+",ttl=forever"),
		"invalid JWT ttl",
	)
}

func TestJWTProviderRejectsOversizedKeyFiles(t *testing.T) {
	oversized := writeJWTKey(t, "oversized", make([]byte, maxJWTKeyBytes+1))
	require.ErrorContains(t,
		ValidateAuthTokenProvider("jwt,sign-method=HS256,priv-key="+oversized),
		"read JWT priv-key: key file exceeds",
	)
	require.ErrorContains(t,
		ValidateAuthTokenProvider("jwt,sign-method=RS256,pub-key="+oversized),
		"read JWT pub-key: key file exceeds",
	)
}

func TestJWTManagerUsesAuthRevisionAndRejectsOldToken(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager, _ := bootstrapAuthForToken(t, server)
	keys := generateJWTTestKeys(t)
	spec := "jwt,sign-method=RS256,priv-key=" + writeJWTKey(t, "private.pem", keys.rsaPrivate)
	require.NoError(t, server.tokens.configureProvider(spec))
	now := time.Unix(2_000_000_000, 0)
	server.tokens.now = func() time.Time { return now }
	token, err := server.tokens.authenticate(context.Background(), "root", "secret")
	require.NoError(t, err)
	require.Len(t, strings.Split(token, "."), 3)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))
	caller, err := server.authCallerFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, "root", caller.username)

	require.NoError(t, manager.roleAdd(context.Background(), "reader"))
	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("revision-check")})
	requireJWTError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown, "etcdserver: revision of auth store is old")
}

func TestJWTAuthRPCInvalidatesOldTokenAfterAuthMutation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	require.NoError(t, server.tokens.configureProvider("jwt,sign-method=HS256,priv-key="+secret))
	now := time.Unix(2_000_000_000, 0)
	server.tokens.now = func() time.Time { return now }
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "root", Password: "root-secret",
	}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.enable(ctx))

	authenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
		Name: "root", Password: "root-secret",
	})
	require.NoError(t, err)
	require.Len(t, strings.Split(authenticated.Token, "."), 3)
	oldCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, authenticated.Token,
	))
	key := []byte("/a974/jwt-auth/revision")
	_, err = server.Put(oldCtx, &etcdserverpb.PutRequest{Key: key, Value: []byte("initial")})
	require.NoError(t, err)

	_, err = server.RoleAdd(oldCtx, &etcdserverpb.AuthRoleAddRequest{Name: "jwt-revision-invalidator"})
	require.NoError(t, err)
	_, err = server.Range(oldCtx, &etcdserverpb.RangeRequest{Key: key})
	requireJWTError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown, "etcdserver: revision of auth store is old")

	reauthenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
		Name: "root", Password: "root-secret",
	})
	require.NoError(t, err)
	require.NotEqual(t, authenticated.Token, reauthenticated.Token)
	newCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, reauthenticated.Token,
	))
	_, err = server.Put(newCtx, &etcdserverpb.PutRequest{Key: key, Value: []byte("reauthenticated")})
	require.NoError(t, err)
	ranged, err := server.Range(newCtx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, ranged.Kvs, 1)
	require.Equal(t, []byte("reauthenticated"), ranged.Kvs[0].Value)
}

func TestJWTEmptyAndZeroClaimsMatchEtcdAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, _ = bootstrapAuthForToken(t, server)
	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	require.NoError(t, server.tokens.configureProvider("jwt,sign-method=HS256,priv-key="+secret))
	now := time.Unix(2_000_000_000, 0)

	current, err := server.tokens.snapshots.current(context.Background())
	require.NoError(t, err)
	emptyUser, err := server.tokens.jwt.issue("", current.Config.Revision, now)
	require.NoError(t, err)
	emptyCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, emptyUser))
	_, err = server.Range(emptyCtx, &etcdserverpb.RangeRequest{Key: []byte("empty-user")})
	requireJWTError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.AuthDisable(emptyCtx, &etcdserverpb.AuthDisableRequest{})
	requireJWTError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")

	rootZero, err := server.tokens.jwt.issue("root", 0, now)
	require.NoError(t, err)
	claims, err := server.tokens.jwt.verify(rootZero, now)
	require.NoError(t, err)
	require.Equal(t, uint64(0), claims.Revision)
	zeroCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootZero))
	_, err = server.Range(zeroCtx, &etcdserverpb.RangeRequest{Key: []byte("zero-revision")})
	requireJWTError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.AuthDisable(zeroCtx, &etcdserverpb.AuthDisableRequest{})
	require.NoError(t, err)
}

func TestJWTProviderRejectsMalformedOptionsAndWrongAlgorithm(t *testing.T) {
	require.EqualError(t, ValidateAuthTokenProvider("jwt,sign-method=RS256,broken"), `invalid auth token option "broken"`)
	require.EqualError(t, ValidateAuthTokenProvider("jwt,sign-method=RS256,sign-method=PS256"), `duplicate auth token option "sign-method"`)
	require.EqualError(t, ValidateAuthTokenProvider("jwt"), "auth: invalid auth signature method")
	require.EqualError(t, ValidateAuthTokenProvider("jwt,sign-method=none"), "unsupported JWT signing method *jwt.signingMethodNone")
	require.EqualError(t, ValidateAuthTokenProvider("bearer"), `auth token provider "bearer" is unsupported`)

	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	hs256, err := parseAuthTokenProvider("jwt,sign-method=HS256,priv-key=" + secret)
	require.NoError(t, err)
	hs512, err := parseAuthTokenProvider("jwt,sign-method=HS512,priv-key=" + secret)
	require.NoError(t, err)
	token, err := hs512.issue("root", 1, time.Now())
	require.NoError(t, err)
	_, err = hs256.verify(token, time.Now())
	requireJWTError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
}

func requireJWTError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
