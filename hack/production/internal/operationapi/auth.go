package operationapi

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"k8s.io/apimachinery/pkg/util/validation"
)

const oidcDocumentLimit = 1 << 20

var ErrOIDCUnavailable = errors.New("OIDC provider unavailable")

type Principal struct {
	Subject   string
	Tenant    string
	Instances map[string]struct{}
}

func (p Principal) Allows(instance string) bool {
	_, all := p.Instances["*"]
	_, exact := p.Instances[instance]
	return all || exact
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

type OIDCConfig struct {
	Issuer         string
	Audience       string
	TenantClaim    string
	InstancesClaim string
	HTTPClient     *http.Client
	CacheTTL       time.Duration
	RefreshBackoff time.Duration
}

type OIDCAuthenticator struct {
	config       OIDCConfig
	jwksURL      string
	refreshMu    sync.Mutex
	mu           sync.RWMutex
	keys         map[string]*rsa.PublicKey
	expires      time.Time
	retryAfter   time.Time
	unknownUntil time.Time
}

type discoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURL string `json:"jwks_uri"`
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func NewOIDCAuthenticator(ctx context.Context, config OIDCConfig) (*OIDCAuthenticator, error) {
	if config.Issuer == "" || config.Audience == "" {
		return nil, errors.New("OIDC issuer and audience are required")
	}
	config.Issuer = strings.TrimSuffix(config.Issuer, "/")
	if config.TenantClaim == "" {
		config.TenantClaim = "tenant"
	}
	if config.InstancesClaim == "" {
		config.InstancesClaim = "kubebrain_instances"
	}
	if config.CacheTTL <= 0 {
		config.CacheTTL = 5 * time.Minute
	}
	if config.RefreshBackoff <= 0 {
		config.RefreshBackoff = 5 * time.Second
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	issuerURL, err := secureURL(config.Issuer)
	if err != nil {
		return nil, fmt.Errorf("invalid OIDC issuer: %w", err)
	}
	var discovery discoveryDocument
	if err := getJSON(ctx, config.HTTPClient, config.Issuer+"/.well-known/openid-configuration", &discovery); err != nil {
		return nil, fmt.Errorf("discover OIDC provider: %w", err)
	}
	if strings.TrimSuffix(discovery.Issuer, "/") != config.Issuer {
		return nil, errors.New("OIDC discovery issuer does not match configured issuer")
	}
	jwksURL, err := secureURL(discovery.JWKSURL)
	if err != nil {
		return nil, fmt.Errorf("invalid OIDC JWKS URL: %w", err)
	}
	if issuerURL.Scheme == "http" && jwksURL.Hostname() != issuerURL.Hostname() {
		return nil, errors.New("loopback HTTP OIDC issuer and JWKS host must match")
	}
	authenticator := &OIDCAuthenticator{config: config, jwksURL: jwksURL.String()}
	if err := authenticator.refresh(ctx); err != nil {
		return nil, err
	}
	return authenticator, nil
}

func secureURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("URL must be an absolute origin without credentials, query, or fragment")
	}
	if parsed.Scheme == "https" {
		return parsed, nil
	}
	host := net.ParseIP(parsed.Hostname())
	if parsed.Scheme == "http" && host != nil && host.IsLoopback() {
		return parsed, nil
	}
	return nil, errors.New("URL must use HTTPS")
}

func getJSON(ctx context.Context, client *http.Client, endpoint string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, oidcDocumentLimit+1))
	if err != nil {
		return err
	}
	if len(payload) > oidcDocumentLimit {
		return errors.New("JSON response exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("JSON response contains trailing data")
	}
	return nil
}

func (a *OIDCAuthenticator) refresh(ctx context.Context) error {
	var document jwksDocument
	if err := getJSON(ctx, a.config.HTTPClient, a.jwksURL, &document); err != nil {
		return fmt.Errorf("fetch OIDC JWKS: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, raw := range document.Keys {
		if raw.Kid == "" || raw.Kty != "RSA" || (raw.Use != "" && raw.Use != "sig") ||
			(raw.Alg != "" && raw.Alg != jwt.SigningMethodRS256.Alg()) {
			continue
		}
		key, err := rsaJWK(raw)
		if err != nil {
			return fmt.Errorf("parse OIDC JWKS key %q: %w", raw.Kid, err)
		}
		if _, duplicate := keys[raw.Kid]; duplicate {
			return fmt.Errorf("duplicate OIDC JWKS key ID %q", raw.Kid)
		}
		keys[raw.Kid] = key
	}
	if len(keys) == 0 {
		return errors.New("OIDC JWKS has no usable RS256 signing keys")
	}
	a.mu.Lock()
	a.keys = keys
	a.expires = time.Now().Add(a.config.CacheTTL)
	a.retryAfter = time.Time{}
	a.mu.Unlock()
	return nil
}

func rsaJWK(raw jwk) (*rsa.PublicKey, error) {
	modulus, err := base64.RawURLEncoding.DecodeString(raw.N)
	if err != nil || len(modulus) == 0 {
		return nil, errors.New("invalid RSA modulus")
	}
	exponentBytes, err := base64.RawURLEncoding.DecodeString(raw.E)
	if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
		return nil, errors.New("invalid RSA exponent")
	}
	exponent := 0
	for _, value := range exponentBytes {
		exponent = exponent<<8 | int(value)
	}
	if exponent < 3 || exponent%2 == 0 {
		return nil, errors.New("invalid RSA exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}, nil
}

func (a *OIDCAuthenticator) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	now := time.Now()
	a.mu.RLock()
	key, found := a.keys[kid]
	expired := now.After(a.expires)
	unknownUntil := a.unknownUntil
	retryAfter := a.retryAfter
	a.mu.RUnlock()
	if found && !expired {
		return key, nil
	}
	if !found && now.Before(unknownUntil) {
		return nil, errors.New("OIDC signing key is unknown")
	}
	if now.Before(retryAfter) {
		return nil, ErrOIDCUnavailable
	}

	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()

	now = time.Now()
	a.mu.RLock()
	key, found = a.keys[kid]
	expired = now.After(a.expires)
	unknownUntil = a.unknownUntil
	retryAfter = a.retryAfter
	a.mu.RUnlock()
	if found && !expired {
		return key, nil
	}
	if !found && now.Before(unknownUntil) {
		return nil, errors.New("OIDC signing key is unknown")
	}
	if now.Before(retryAfter) {
		return nil, ErrOIDCUnavailable
	}
	if err := a.refresh(ctx); err != nil {
		a.mu.Lock()
		a.retryAfter = time.Now().Add(a.config.RefreshBackoff)
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %v", ErrOIDCUnavailable, err)
	}
	a.mu.RLock()
	key, found = a.keys[kid]
	a.mu.RUnlock()
	if !found {
		a.mu.Lock()
		a.unknownUntil = time.Now().Add(a.config.RefreshBackoff)
		a.mu.Unlock()
		return nil, errors.New("OIDC signing key is unknown")
	}
	return key, nil
}

func (a *OIDCAuthenticator) Authenticate(ctx context.Context, authorization string) (Principal, error) {
	scheme, tokenText, found := strings.Cut(strings.TrimSpace(authorization), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(tokenText) == "" {
		return Principal{}, errors.New("Bearer token is required")
	}
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(strings.TrimSpace(tokenText), claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodRS256 {
			return nil, errors.New("OIDC token must use RS256")
		}
		kid, ok := token.Header["kid"].(string)
		if !ok || kid == "" {
			return nil, errors.New("OIDC token has no key ID")
		}
		return a.key(ctx, kid)
	}, jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithIssuer(a.config.Issuer), jwt.WithAudience(a.config.Audience),
		jwt.WithExpirationRequired())
	if errors.Is(err, ErrOIDCUnavailable) {
		return Principal{}, err
	}
	if err != nil || !parsed.Valid {
		return Principal{}, errors.New("OIDC token is invalid")
	}
	subject, _ := claims["sub"].(string)
	tenant, _ := claims[a.config.TenantClaim].(string)
	if subject == "" || len(subject) > 253 || len(validation.IsDNS1123Label(tenant)) != 0 {
		return Principal{}, errors.New("OIDC identity claims are invalid")
	}
	instances := make(map[string]struct{})
	switch values := claims[a.config.InstancesClaim].(type) {
	case []any:
		for _, value := range values {
			instance, ok := value.(string)
			if !ok || instance == "" {
				return Principal{}, errors.New("OIDC instance claims are invalid")
			}
			instances[instance] = struct{}{}
		}
	case []string:
		for _, instance := range values {
			if instance == "" {
				return Principal{}, errors.New("OIDC instance claims are invalid")
			}
			instances[instance] = struct{}{}
		}
	default:
		return Principal{}, errors.New("OIDC instance claims are missing")
	}
	if len(instances) == 0 {
		return Principal{}, errors.New("OIDC instance claims are empty")
	}
	return Principal{Subject: subject, Tenant: tenant, Instances: instances}, nil
}
