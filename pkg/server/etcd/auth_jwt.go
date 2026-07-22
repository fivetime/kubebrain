// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package etcd

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

const (
	jwtSignMethod = "sign-method"
	jwtPublicKey  = "pub-key"
	jwtPrivateKey = "priv-key"
	jwtTTL        = "ttl"

	maxJWTKeyBytes int64 = 1 << 20
)

type jwtTokenProvider struct {
	method     jwt.SigningMethod
	key        any
	ttl        time.Duration
	verifyOnly bool
}

type jwtProviderClaims struct {
	Username string `json:"username"`
	Revision uint64 `json:"revision"`
	jwt.RegisteredClaims
}

// ValidateAuthTokenProvider validates the exact --auth-token syntax and key
// material during startup. Unknown JWT options match etcd's warning-only
// behavior; malformed or duplicate options fail closed.
func ValidateAuthTokenProvider(spec string) error {
	_, err := parseAuthTokenProvider(spec)
	return err
}

func parseAuthTokenProvider(spec string) (*jwtTokenProvider, error) {
	if spec == "" || spec == "simple" {
		return nil, nil
	}
	parts := strings.Split(spec, ",")
	if len(parts) == 0 || parts[0] != "jwt" {
		return nil, fmt.Errorf("auth token provider %q is unsupported", spec)
	}
	opts := make(map[string]string, len(parts)-1)
	for _, raw := range parts[1:] {
		pair := strings.Split(raw, "=")
		if len(pair) != 2 {
			return nil, fmt.Errorf("invalid auth token option %q", raw)
		}
		if _, duplicate := opts[pair[0]]; duplicate {
			return nil, fmt.Errorf("duplicate auth token option %q", pair[0])
		}
		opts[pair[0]] = pair[1]
	}

	method := jwt.GetSigningMethod(opts[jwtSignMethod])
	if method == nil {
		return nil, errors.New("auth: invalid auth signature method")
	}
	ttl := 5 * time.Minute
	if raw := opts[jwtTTL]; raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid JWT ttl %q", raw)
		}
		ttl = parsed
	}
	read := func(option string) ([]byte, error) {
		path := opts[option]
		if path == "" {
			return nil, nil
		}
		return readJWTKeyOption(path, option)
	}
	publicPEM, err := read(jwtPublicKey)
	if err != nil {
		return nil, err
	}
	privatePEM, err := read(jwtPrivateKey)
	if err != nil {
		return nil, err
	}
	key, verifyOnly, err := jwtProviderKey(method, publicPEM, privatePEM)
	if err != nil {
		return nil, err
	}
	return &jwtTokenProvider{method: method, key: key, ttl: ttl, verifyOnly: verifyOnly}, nil
}

func readJWTKeyOption(path, option string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read JWT %s: %w", option, err)
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, maxJWTKeyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read JWT %s: %w", option, err)
	}
	if int64(len(value)) > maxJWTKeyBytes {
		return nil, fmt.Errorf("read JWT %s: key file exceeds %d bytes", option, maxJWTKeyBytes)
	}
	return value, nil
}

func jwtProviderKey(method jwt.SigningMethod, publicPEM, privatePEM []byte) (any, bool, error) {
	switch method.(type) {
	case *jwt.SigningMethodHMAC:
		if len(privatePEM) == 0 {
			return nil, false, errors.New("auth: missing key data")
		}
		return privatePEM, false, nil
	case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
		var private *rsa.PrivateKey
		var public *rsa.PublicKey
		var err error
		if len(privatePEM) != 0 {
			private, err = jwt.ParseRSAPrivateKeyFromPEM(privatePEM)
			if err != nil {
				return nil, false, err
			}
		}
		if len(publicPEM) != 0 {
			public, err = jwt.ParseRSAPublicKeyFromPEM(publicPEM)
			if err != nil {
				return nil, false, err
			}
		}
		if private == nil {
			if public == nil {
				return nil, false, errors.New("auth: missing key data")
			}
			return public, true, nil
		}
		if public != nil && !public.Equal(private.Public()) {
			return nil, false, errors.New("auth: public and private keys don't match")
		}
		return private, false, nil
	case *jwt.SigningMethodECDSA:
		var private *ecdsa.PrivateKey
		var public *ecdsa.PublicKey
		var err error
		if len(privatePEM) != 0 {
			private, err = jwt.ParseECPrivateKeyFromPEM(privatePEM)
			if err != nil {
				return nil, false, err
			}
		}
		if len(publicPEM) != 0 {
			public, err = jwt.ParseECPublicKeyFromPEM(publicPEM)
			if err != nil {
				return nil, false, err
			}
		}
		if private == nil {
			if public == nil {
				return nil, false, errors.New("auth: missing key data")
			}
			return public, true, nil
		}
		if public != nil && !public.Equal(private.Public()) {
			return nil, false, errors.New("auth: public and private keys don't match")
		}
		return private, false, nil
	case *jwt.SigningMethodEd25519:
		var private ed25519.PrivateKey
		var public ed25519.PublicKey
		if len(privatePEM) != 0 {
			key, err := jwt.ParseEdPrivateKeyFromPEM(privatePEM)
			if err != nil {
				return nil, false, err
			}
			private = key.(ed25519.PrivateKey)
		}
		if len(publicPEM) != 0 {
			key, err := jwt.ParseEdPublicKeyFromPEM(publicPEM)
			if err != nil {
				return nil, false, err
			}
			public = key.(ed25519.PublicKey)
		}
		if private == nil {
			if public == nil {
				return nil, false, errors.New("auth: missing key data")
			}
			return public, true, nil
		}
		if public != nil && !public.Equal(private.Public()) {
			return nil, false, errors.New("auth: public and private keys don't match")
		}
		return private, false, nil
	default:
		return nil, false, fmt.Errorf("unsupported JWT signing method %T", method)
	}
}

func (p *jwtTokenProvider) signingKey() (any, error) {
	if p.verifyOnly {
		return nil, errors.New("auth: JWT token provider is verify-only")
	}
	return p.key, nil
}

func (p *jwtTokenProvider) verificationKey() any {
	switch key := p.key.(type) {
	case *rsa.PrivateKey:
		return &key.PublicKey
	case *ecdsa.PrivateKey:
		return &key.PublicKey
	case ed25519.PrivateKey:
		return key.Public()
	default:
		return key
	}
}

func (p *jwtTokenProvider) issue(username string, revision uint64, now time.Time) (string, error) {
	key, err := p.signingKey()
	if err != nil {
		return "", err
	}
	token := jwt.NewWithClaims(p.method, jwtProviderClaims{
		Username: username,
		Revision: revision,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(p.ttl)),
		},
	})
	return token.SignedString(key)
}

func (p *jwtTokenProvider) verify(token string, now time.Time) (authTokenClaims, error) {
	claims := new(jwtProviderClaims)
	parsed, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != p.method.Alg() {
			return nil, errors.New("invalid signing method")
		}
		return p.verificationKey(), nil
	}, jwt.WithTimeFunc(func() time.Time { return now }), jwt.WithValidMethods([]string{p.method.Alg()}))
	if err != nil || !parsed.Valid {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	if claims.Username == "" || claims.Revision == 0 {
		return authTokenClaims{}, rpctypes.ErrInvalidAuthToken
	}
	return authTokenClaims{Username: claims.Username, Revision: claims.Revision}, nil
}
