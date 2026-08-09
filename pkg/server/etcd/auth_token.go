package etcd

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
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
	Username    string `json:"u"`
	Revision    uint64 `json:"r"`
	Generation  string `json:"g,omitempty"`
	Certificate bool   `json:"c,omitempty"`
	IssuedAt    int64  `json:"i"`
	Expires     int64  `json:"e"`
	Nonce       string `json:"n"`
}

type authTokenManager struct {
	repo      *authRepository
	snapshots *authSnapshotCache
	now       func() time.Time
	ttl       time.Duration
	jwt       *jwtTokenProvider
	nop       bool

	afterPasswordCheck func()
}

func (m *authTokenManager) configureProvider(spec string) error {
	if spec == "" {
		m.jwt = nil
		m.nop = true
		return nil
	}
	provider, err := parseAuthTokenProvider(spec)
	if err != nil {
		return err
	}
	m.jwt = provider
	m.nop = false
	return nil
}

func (m *authTokenManager) ensureUserGeneration(ctx context.Context, username string) (*authpb.User, error) {
	generation, err := newUserTokenGeneration(username)
	if err != nil {
		return nil, err
	}
	value, err := marshalAuthRecord(generation)
	if err != nil {
		return nil, err
	}
	err = m.repo.backend.InternalCAS(ctx, []backend.InternalCASOp{{
		Key: authRecordKey(authTokenGenerationsKey, username), Value: value,
	}})
	if err != nil && !errors.Is(err, storage.ErrCASFailed) {
		return nil, err
	}
	m.snapshots.invalidate()
	snapshot, err := m.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	generation = snapshot.TokenGenerations[username]
	if generation == nil || len(generation.Password) != authUserTokenGenerationBytes {
		return nil, errors.New("missing auth user token generation")
	}
	return generation, nil
}

func newAuthTokenManager(backend BackendShim) *authTokenManager {
	return &authTokenManager{
		repo: newAuthRepository(backend), snapshots: newAuthSnapshotCache(backend), now: time.Now, ttl: authTokenTTL,
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
	return m.authenticateWithIssue(ctx, username, password, nil)
}

func (m *authTokenManager) authenticateWithIssue(
	ctx context.Context,
	username, password string,
	observeIssue func(func() (string, error)) (string, error),
) (string, error) {
	for {
		snapshot, err := m.snapshots.current(ctx)
		if err != nil {
			return "", err
		}
		if !snapshot.Config.Enabled {
			return "", rpctypes.ErrAuthNotEnabled
		}
		user := snapshot.Users[username]
		if user == nil {
			return "", rpctypes.ErrAuthFailed
		}
		if user.Options != nil && user.Options.NoPassword {
			return "", errNoPasswordUser
		}
		if bcrypt.CompareHashAndPassword(user.Password, []byte(password)) != nil {
			return "", rpctypes.ErrAuthFailed
		}
		if m.nop {
			return "", rpctypes.ErrAuthFailed
		}
		if m.afterPasswordCheck != nil {
			m.afterPasswordCheck()
		}
		latest, err := m.snapshots.current(ctx)
		if err != nil {
			return "", err
		}
		if latest.Config != snapshot.Config {
			continue
		}
		if observeIssue != nil {
			return observeIssue(func() (string, error) { return m.issue(ctx, latest, username) })
		}
		return m.issue(ctx, latest, username)
	}
}

// issue creates a normal signed user token after authenticate has checked the
// password.
func (m *authTokenManager) issue(ctx context.Context, snapshot *authSnapshot, username string) (string, error) {
	if !snapshot.Config.Enabled || snapshot.Users[username] == nil {
		return "", rpctypes.ErrAuthFailed
	}
	if m.nop {
		return "", rpctypes.ErrAuthFailed
	}
	if m.jwt != nil {
		return m.jwt.issue(username, snapshot.Config.Revision, m.now())
	}
	generation := snapshot.TokenGenerations[username]
	var err error
	if generation == nil {
		generation, err = m.ensureUserGeneration(ctx, username)
		if err != nil {
			return "", err
		}
	}
	if len(generation.Password) != authUserTokenGenerationBytes {
		return "", errors.New("invalid auth user token generation")
	}
	return m.issueClaims(ctx, authTokenClaims{
		Username: username, Revision: snapshot.Config.Revision,
		Generation: base64.RawURLEncoding.EncodeToString(generation.Password),
	})
}

func (m *authTokenManager) issueCertificate(ctx context.Context, snapshot *authSnapshot, username string) (string, error) {
	if !snapshot.Config.Enabled || username == "" {
		return "", rpctypes.ErrUserEmpty
	}
	if m.nop {
		return "", rpctypes.ErrAuthFailed
	}
	if m.jwt != nil {
		return m.jwt.issue(username, snapshot.Config.Revision, m.now())
	}
	return m.issueClaims(ctx, authTokenClaims{
		Username: username, Revision: snapshot.Config.Revision, Certificate: true,
	})
}

func (m *authTokenManager) issueClaims(ctx context.Context, claims authTokenClaims) (string, error) {
	key, err := m.ensureSigningKey(ctx)
	if err != nil {
		return "", err
	}
	now := m.now()
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	claims.IssuedAt = now.Unix()
	claims.Expires = now.Add(m.ttl).Unix()
	claims.Nonce = base64.RawURLEncoding.EncodeToString(nonce)
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
	if m.nop {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	if m.jwt != nil {
		claims, err := m.jwt.verify(token, m.now())
		if err != nil {
			return authTokenClaims{}, err
		}
		return claims, nil
	}
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
	now := m.now()
	claims, err := decodeAuthTokenClaims(payload)
	if err != nil || claims.Username == "" || claims.Revision == 0 ||
		claims.IssuedAt > now.Add(authTokenClockSkew).Unix() || claims.Expires <= now.Unix() || claims.Expires <= claims.IssuedAt {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	snapshot, err := m.snapshots.current(ctx)
	if err != nil {
		return authTokenClaims{}, err
	}
	if !snapshot.Config.Enabled {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	if claims.Certificate {
		if claims.Generation != "" {
			return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
		}
		return claims, nil
	}
	if snapshot.Users[claims.Username] == nil {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	if claims.Generation == "" {
		// Tokens issued before per-user generations were introduced remain valid
		// during a rolling upgrade, but retain their legacy global-revision rule.
		if claims.Revision != snapshot.Config.Revision {
			return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
		}
		return claims, nil
	}
	generation, err := base64.RawURLEncoding.DecodeString(claims.Generation)
	currentGeneration := snapshot.TokenGenerations[claims.Username]
	if err != nil || currentGeneration == nil || !hmac.Equal(generation, currentGeneration.Password) {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	return claims, nil
}

func decodeAuthTokenClaims(payload []byte) (authTokenClaims, error) {
	var claims authTokenClaims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return authTokenClaims{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return authTokenClaims{}, errors.New("auth token claims contain trailing JSON")
	}
	return claims, nil
}
