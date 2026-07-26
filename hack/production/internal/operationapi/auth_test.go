package operationapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

type oidcFixture struct {
	mu           sync.RWMutex
	keys         map[string]*rsa.PrivateKey
	fail         bool
	jwksRequests int
}

func (f *oidcFixture) serve(serverURL string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(discoveryDocument{Issuer: serverURL, JWKSURL: serverURL + "/keys"})
	})
	mux.HandleFunc("/keys", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/jwk-set+json")
		f.mu.Lock()
		defer f.mu.Unlock()
		f.jwksRequests++
		if f.fail {
			http.Error(response, "unavailable", http.StatusServiceUnavailable)
			return
		}
		document := jwksDocument{}
		for kid, key := range f.keys {
			document.Keys = append(document.Keys, jwk{
				Kid: kid, Kty: "RSA", Use: "sig", Alg: "RS256",
				N: base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				E: base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			})
		}
		_ = json.NewEncoder(response).Encode(document)
	})
	return mux
}

func signOIDCToken(t *testing.T, key *rsa.PrivateKey, kid, issuer, audience, tenant string, instances any) string {
	return signOIDCTokenForSubject(t, key, kid, issuer, audience, "user-123", tenant, instances)
}

func signOIDCTokenForSubject(
	t *testing.T,
	key *rsa.PrivateKey,
	kid, issuer, audience, subject, tenant string,
	instances any,
) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": issuer, "aud": audience, "sub": subject, "tenant": tenant,
		"kubebrain_instances": instances,
		"iat":                 time.Now().Add(-time.Minute).Unix(),
		"exp":                 time.Now().Add(time.Minute).Unix(),
	})
	token.Header["kid"] = kid
	signed, err := token.SignedString(key)
	require.NoError(t, err)
	return signed
}

func TestOIDCAuthenticatorValidatesIdentityAndRefreshesUnknownKey(t *testing.T) {
	first, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	second, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"first": first}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()

	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "kubebrain-management", CacheTTL: time.Hour,
	})
	require.NoError(t, err)
	token := signOIDCToken(t, first, "first", server.URL, "kubebrain-management", "tenant-a", []string{"instance-a"})
	principal, err := authenticator.Authenticate(context.Background(), "Bearer "+token)
	require.NoError(t, err)
	require.Equal(t, "user-123", principal.Subject)
	require.Equal(t, "tenant-a", principal.Tenant)
	require.True(t, principal.Allows("instance-a"))
	require.False(t, principal.Allows("instance-b"))
	wildcard := signOIDCToken(t, first, "first", server.URL, "kubebrain-management", "tenant-a", []string{"*"})
	principal, err = authenticator.Authenticate(context.Background(), "Bearer "+wildcard)
	require.NoError(t, err)
	require.True(t, principal.Allows("instance-with-crd-format_1.2"))

	fixture.mu.Lock()
	fixture.keys = map[string]*rsa.PrivateKey{"second": second}
	fixture.mu.Unlock()
	rotated := signOIDCToken(t, second, "second", server.URL, "kubebrain-management", "tenant-a", []string{"instance-b"})
	principal, err = authenticator.Authenticate(context.Background(), "bearer "+rotated)
	require.NoError(t, err, "unknown kid must trigger a bounded JWKS refresh")
	require.True(t, principal.Allows("instance-b"))
}

func TestOIDCAuthenticatorFailsClosed(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"key": key}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "expected",
	})
	require.NoError(t, err)

	tests := []struct {
		name      string
		header    string
		audience  string
		subject   string
		tenant    string
		instances any
		want      string
	}{
		{name: "missing bearer", want: "bearer token is required"},
		{name: "wrong audience", audience: "wrong", tenant: "tenant-a", instances: []string{"instance-a"}, want: "OIDC token is invalid"},
		{name: "invalid subject", audience: "expected", subject: "user\ncontrol", tenant: "tenant-a", instances: []string{"instance-a"}, want: "OIDC identity claims are invalid"},
		{name: "invalid tenant", audience: "expected", tenant: "Tenant_A", instances: []string{"instance-a"}, want: "OIDC identity claims are invalid"},
		{name: "missing instances", audience: "expected", tenant: "tenant-a", want: "OIDC instance claims are missing"},
		{name: "mixed instance types", audience: "expected", tenant: "tenant-a", instances: []any{"instance-a", 3}, want: "OIDC instance claims are invalid"},
		{name: "invalid instance claim", audience: "expected", tenant: "tenant-a", instances: []string{".invalid"}, want: "OIDC instance claims are invalid"},
		{name: "too long instance claim", audience: "expected", tenant: "tenant-a", instances: []string{string(make([]byte, 129))}, want: "OIDC instance claims are invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := test.header
			if test.audience != "" {
				subject := test.subject
				if subject == "" {
					subject = "user-123"
				}
				header = "Bearer " + signOIDCTokenForSubject(
					t, key, "key", server.URL, test.audience, subject, test.tenant, test.instances,
				)
			}
			_, err := authenticator.Authenticate(context.Background(), header)
			require.ErrorContains(t, err, test.want)
		})
	}

	tooLongSubject := signOIDCTokenForSubject(
		t, key, "key", server.URL, "expected", string(make([]byte, 254)), "tenant-a", []string{"instance-a"},
	)
	_, err = authenticator.Authenticate(context.Background(), "Bearer "+tooLongSubject)
	require.ErrorContains(t, err, "OIDC identity claims are invalid")
}

func TestOIDCAuthenticatorRejectsMalformedBearerHeader(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"key": key}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "expected", CacheTTL: time.Nanosecond,
	})
	require.NoError(t, err)
	token := signOIDCToken(t, key, "key", server.URL, "expected", "tenant-a", []string{"instance-a"})
	time.Sleep(time.Millisecond)
	fixture.mu.Lock()
	initialJWKSRequests := fixture.jwksRequests
	fixture.fail = true
	fixture.mu.Unlock()

	for _, tc := range []struct {
		name   string
		header string
	}{
		{name: "leading header space", header: " Bearer " + token},
		{name: "extra token separator", header: "Bearer  " + token},
		{name: "trailing token space", header: "Bearer " + token + " "},
		{name: "embedded token space", header: "Bearer " + token + " extra"},
		{name: "tab separator", header: "Bearer\t" + token},
		{name: "oversized token", header: "Bearer " + strings.Repeat("x", maxBearerTokenBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := authenticator.Authenticate(context.Background(), tc.header)
			require.Error(t, err)
			fixture.mu.RLock()
			defer fixture.mu.RUnlock()
			require.Equal(t, initialJWKSRequests, fixture.jwksRequests,
				"malformed bearer headers must fail before JWKS refresh")
		})
	}
}

func TestOIDCAuthenticatorRejectsExpiredCacheWhenRefreshFails(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"key": key}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "expected", CacheTTL: time.Nanosecond,
	})
	require.NoError(t, err)
	fixture.mu.Lock()
	fixture.fail = true
	fixture.mu.Unlock()
	token := signOIDCToken(t, key, "key", server.URL, "expected", "tenant-a", []string{"instance-a"})
	_, err = authenticator.Authenticate(context.Background(), "Bearer "+token)
	require.Error(t, err, "an expired JWKS cache must not keep trusting a removed key when refresh fails")
	require.ErrorIs(t, err, ErrOIDCUnavailable)
}

func TestOIDCAuthenticatorCollapsesConcurrentUnknownKeyRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"known": key}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "expected", RefreshBackoff: time.Minute,
	})
	require.NoError(t, err)
	unknown := signOIDCToken(t, key, "unknown", server.URL, "expected", "tenant-a", []string{"instance-a"})

	const callers = 64
	start := make(chan struct{})
	errs := make(chan error, callers)
	var workers sync.WaitGroup
	for range callers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, callErr := authenticator.Authenticate(context.Background(), "Bearer "+unknown)
			errs <- callErr
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	for callErr := range errs {
		require.Error(t, callErr)
	}
	known := signOIDCToken(t, key, "known", server.URL, "expected", "tenant-a", []string{"instance-a"})
	principal, err := authenticator.Authenticate(context.Background(), "Bearer "+known)
	require.NoError(t, err, "unknown-key backoff must not reject a cached valid signing key")
	require.Equal(t, "tenant-a", principal.Tenant)
	fixture.mu.RLock()
	defer fixture.mu.RUnlock()
	require.Equal(t, 2, fixture.jwksRequests, "initial load plus one collapsed unknown-kid refresh")
}

func TestOIDCAuthenticatorBacksOffFailedRefreshAndRecovers(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"key": key}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "expected", CacheTTL: time.Nanosecond,
		RefreshBackoff: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	token := signOIDCToken(t, key, "key", server.URL, "expected", "tenant-a", []string{"instance-a"})
	fixture.mu.Lock()
	fixture.fail = true
	fixture.mu.Unlock()

	for range 20 {
		_, err = authenticator.Authenticate(context.Background(), "Bearer "+token)
		require.ErrorIs(t, err, ErrOIDCUnavailable)
	}
	fixture.mu.RLock()
	require.Equal(t, 2, fixture.jwksRequests, "backoff must suppress repeated failed refreshes")
	fixture.mu.RUnlock()

	fixture.mu.Lock()
	fixture.fail = false
	fixture.mu.Unlock()
	time.Sleep(25 * time.Millisecond)
	_, err = authenticator.Authenticate(context.Background(), "Bearer "+token)
	require.NoError(t, err)
	fixture.mu.RLock()
	defer fixture.mu.RUnlock()
	require.Equal(t, 3, fixture.jwksRequests)
}

func TestOIDCAuthenticatorRejectsNonJSONDiscoveryAndJWKS(t *testing.T) {
	t.Run("discovery", func(t *testing.T) {
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "text/html")
			_ = json.NewEncoder(response).Encode(discoveryDocument{
				Issuer: server.URL, JWKSURL: server.URL + "/keys",
			})
		}))
		defer server.Close()

		_, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
			Issuer: server.URL, Audience: "expected",
		})
		require.ErrorContains(t, err, "content type")
	})

	t.Run("jwks", func(t *testing.T) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/.well-known/openid-configuration":
				response.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(response).Encode(discoveryDocument{
					Issuer: server.URL, JWKSURL: server.URL + "/keys",
				})
			case "/keys":
				response.Header().Set("Content-Type", "text/html")
				_ = json.NewEncoder(response).Encode(jwksDocument{Keys: []jwk{{
					Kid: "key", Kty: "RSA", Use: "sig", Alg: "RS256",
					N: base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
					E: base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
				}}})
			default:
				http.NotFound(response, request)
			}
		}))
		defer server.Close()

		_, err = NewOIDCAuthenticator(context.Background(), OIDCConfig{
			Issuer: server.URL, Audience: "expected",
		})
		require.ErrorContains(t, err, "content type")
	})
}

func TestOIDCAuthenticatorUnknownKeyBackoffDoesNotBlockKnownKeyRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"known": key}}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(server.URL).ServeHTTP(response, request)
	}))
	defer server.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "expected", CacheTTL: 10 * time.Millisecond,
		RefreshBackoff: time.Minute,
	})
	require.NoError(t, err)
	unknown := signOIDCToken(t, key, "unknown", server.URL, "expected", "tenant-a", []string{"instance-a"})
	_, err = authenticator.Authenticate(context.Background(), "Bearer "+unknown)
	require.Error(t, err)

	time.Sleep(15 * time.Millisecond)
	known := signOIDCToken(t, key, "known", server.URL, "expected", "tenant-a", []string{"instance-a"})
	_, err = authenticator.Authenticate(context.Background(), "Bearer "+known)
	require.NoError(t, err, "an expired known key must refresh despite an unrelated unknown-key backoff")
	fixture.mu.RLock()
	defer fixture.mu.RUnlock()
	require.Equal(t, 3, fixture.jwksRequests)
}

func TestOIDCAuthenticatorRejectsInsecureRemoteIssuer(t *testing.T) {
	_, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: "http://example.com", Audience: "audience",
	})
	require.ErrorContains(t, err, "HTTPS")
}

func TestSecureURLRejectsUnsafeCharacters(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "newline", raw: "https://issuer.example\n/realms/a"},
		{name: "tab", raw: "https://issuer.example\t/realms/a"},
		{name: "DEL", raw: "https://issuer.example\x7f/realms/a"},
		{name: "quote", raw: `https://issuer.example"/realms/a`},
		{name: "backslash", raw: `https://issuer.example\realms\a`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := secureURL(tc.raw)
			require.ErrorContains(t, err, "unsupported characters")
		})
	}

	got, err := secureURL("https://issuer.example/realms/a")
	require.NoError(t, err)
	require.Equal(t, "/realms/a", got.Path)
}

func TestOIDCAuthenticatorRejectsWeakJWKSRSAKey(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/.well-known/openid-configuration":
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(discoveryDocument{
				Issuer: server.URL, JWKSURL: server.URL + "/keys",
			})
		case "/keys":
			response.Header().Set("Content-Type", "application/jwk-set+json")
			_ = json.NewEncoder(response).Encode(jwksDocument{Keys: []jwk{{
				Kid: "weak", Kty: "RSA", Use: "sig", Alg: "RS256",
				N: base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				E: base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	_, err = NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "audience",
	})
	require.ErrorContains(t, err, "too small")
}

func TestOIDCAuthenticatorRejectsLoopbackHTTPJWKSOnDifferentOrigin(t *testing.T) {
	for _, tc := range []struct {
		name string
		jwks func(*url.URL) string
	}{
		{
			name: "different port",
			jwks: func(issuerURL *url.URL) string {
				return "http://" + net.JoinHostPort(issuerURL.Hostname(), "1") + "/keys"
			},
		},
		{
			name: "different scheme",
			jwks: func(issuerURL *url.URL) string {
				return "https://" + issuerURL.Host + "/keys"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				issuerURL, err := url.Parse(server.URL)
				require.NoError(t, err)
				response.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(response).Encode(discoveryDocument{
					Issuer:  server.URL,
					JWKSURL: tc.jwks(issuerURL),
				})
			}))
			defer server.Close()

			_, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
				Issuer: server.URL, Audience: "audience",
			})
			require.ErrorContains(t, err, "same")
			require.ErrorContains(t, err, "origin")
		})
	}
}
