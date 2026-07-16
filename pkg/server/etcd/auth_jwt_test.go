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

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
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
			require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
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
	require.ErrorContains(t, err, "verify-only")

	other := generateJWTTestKeys(t)
	mismatch := "jwt,sign-method=RS256,priv-key=" + privatePath + ",pub-key=" + writeJWTKey(t, "other-public.pem", other.rsaPublic)
	require.ErrorContains(t, ValidateAuthTokenProvider(mismatch), "don't match")
}

func TestJWTProviderPreservesUint64Revision(t *testing.T) {
	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	provider, err := parseAuthTokenProvider("jwt,sign-method=HS256,priv-key=" + secret)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	const revision = uint64(1<<53 + 1)
	token, err := provider.issue("root", revision, now)
	require.NoError(t, err)
	claims, err := provider.verify(token, now)
	require.NoError(t, err)
	require.Equal(t, revision, claims.Revision)
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
	_, err = server.authCallerFromContext(ctx)
	require.ErrorIs(t, err, rpctypes.ErrAuthOldRevision)
}

func TestJWTProviderRejectsMalformedOptionsAndWrongAlgorithm(t *testing.T) {
	require.Error(t, ValidateAuthTokenProvider("jwt,sign-method=RS256,broken"))
	require.Error(t, ValidateAuthTokenProvider("jwt,sign-method=RS256,sign-method=PS256"))
	require.Error(t, ValidateAuthTokenProvider("jwt,sign-method=none"))

	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	hs256, err := parseAuthTokenProvider("jwt,sign-method=HS256,priv-key=" + secret)
	require.NoError(t, err)
	hs512, err := parseAuthTokenProvider("jwt,sign-method=HS512,priv-key=" + secret)
	require.NoError(t, err)
	token, err := hs512.issue("root", 1, time.Now())
	require.NoError(t, err)
	_, err = hs256.verify(token, time.Now())
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
}
