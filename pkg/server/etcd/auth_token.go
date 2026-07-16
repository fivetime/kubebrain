package etcd

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var authTokenSigningKey = []byte("auth/tokenkeys/1")

const (
	authTokenKeyBytes  = 32
	authTokenTTL       = 5 * time.Minute
	authTokenClockSkew = 30 * time.Second
)

type authTokenClaims struct {
	Username string `json:"u"`
	Revision uint64 `json:"r"`
	IssuedAt int64  `json:"i"`
	Expires  int64  `json:"e"`
	Nonce    string `json:"n"`
}

type authTokenManager struct {
	repo      *authRepository
	snapshots *authSnapshotCache
	now       func() time.Time
}

func newAuthTokenManager(backend BackendShim) *authTokenManager {
	return &authTokenManager{
		repo: newAuthRepository(backend), snapshots: newAuthSnapshotCache(backend), now: time.Now,
	}
}

func (m *authTokenManager) loadSigningKey(ctx context.Context) ([]byte, error) {
	key, err := m.repo.backend.InternalGet(ctx, authTokenSigningKey)
	if err != nil {
		return nil, err
	}
	if len(key) != authTokenKeyBytes {
		return nil, errors.New("invalid auth token signing key")
	}
	return key, nil
}

func (m *authTokenManager) ensureSigningKey(ctx context.Context) ([]byte, error) {
	key, err := m.loadSigningKey(ctx)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		return nil, err
	}
	key = make([]byte, authTokenKeyBytes)
	if _, err = rand.Read(key); err != nil {
		return nil, err
	}
	err = m.repo.backend.InternalCAS(ctx, []backend.InternalCASOp{{Key: authTokenSigningKey, Value: key}})
	if errors.Is(err, storage.ErrCASFailed) {
		return m.loadSigningKey(ctx)
	}
	return key, err
}

func (m *authTokenManager) authenticate(ctx context.Context, username, password string) (string, error) {
	snapshot, err := m.snapshots.current(ctx)
	if err != nil {
		return "", err
	}
	if !snapshot.Config.Enabled {
		return "", rpctypes.ErrAuthNotEnabled
	}
	user := snapshot.Users[username]
	if user == nil || (user.Options != nil && user.Options.NoPassword) || bcrypt.CompareHashAndPassword(user.Password, []byte(password)) != nil {
		return "", rpctypes.ErrAuthFailed
	}
	key, err := m.ensureSigningKey(ctx)
	if err != nil {
		return "", err
	}
	now := m.now()
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	claims := authTokenClaims{
		Username: username, Revision: snapshot.Config.Revision,
		IssuedAt: now.Unix(), Expires: now.Add(authTokenTTL).Unix(),
		Nonce: base64.RawURLEncoding.EncodeToString(nonce),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signature := signAuthToken(key, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func signAuthToken(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func (m *authTokenManager) verify(ctx context.Context, token string) (authTokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	key, err := m.loadSigningKey(ctx)
	if err != nil {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	if !hmac.Equal(signature, signAuthToken(key, payload)) {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	var claims authTokenClaims
	now := m.now()
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Username == "" || claims.Revision == 0 ||
		claims.IssuedAt > now.Add(authTokenClockSkew).Unix() || claims.Expires <= now.Unix() || claims.Expires <= claims.IssuedAt {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	snapshot, err := m.snapshots.current(ctx)
	if err != nil {
		return authTokenClaims{}, err
	}
	if !snapshot.Config.Enabled || claims.Revision != snapshot.Config.Revision || snapshot.Users[claims.Username] == nil {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	return claims, nil
}
