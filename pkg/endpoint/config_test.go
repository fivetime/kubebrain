// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endpoint

import (
	"crypto/tls"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	ast := assert.New(t)
	ast.False((&Config{}).getServerConfig().ClientCertAuth)

	configs := []Config{
		{},
		{
			Port: 2379,
		},
		{
			Port:     2379,
			PeerPort: 2379,
		},
		{
			Port:                 2379,
			PeerPort:             2380,
			ClientSecurityConfig: &SecurityConfig{ClientAuth: true},
		},
		{
			Port:               2379,
			PeerPort:           2380,
			PeerSecurityConfig: &SecurityConfig{ClientAuth: true},
		},
	}

	for _, config := range configs {
		ast.Error(config.Validate())
	}

	conf := Config{
		Port: 2379, PeerPort: 2380, MaxTxnOps: 64, MaxRequestBytes: 1048576,
		AuthToken: "simple", BcryptCost: 7, AuthTokenTTL: 45,
		ClientSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
		PeerSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ServerName:    "kubebrain-peer.kubebrain-system.svc",
			ClientAuth:    true,
			AllowInsecure: true,
		},
	}

	ast.NoError(conf.Validate())
	ast.NotNil(conf.ClientSecurityConfig.getServerTLSConfig())
	ast.NotNil(conf.PeerSecurityConfig.getServerTLSConfig())
	ast.NotNil(conf.PeerSecurityConfig.getClientTLSConfig())
	ast.Equal("kubebrain-peer.kubebrain-system.svc", conf.PeerSecurityConfig.getClientTLSConfig().ServerName)
	ast.ElementsMatch([]string{"h2", "http/1.1"}, conf.ClientSecurityConfig.getServerTLSConfig().NextProtos)
	ast.ElementsMatch([]string{"h2", "http/1.1"}, conf.PeerSecurityConfig.getServerTLSConfig().NextProtos)
	ast.True(conf.getServerConfig().ClientCertAuth)
	ast.True(conf.getServerConfig().ClientAllowInsecure)
	ast.Equal(uint(64), conf.getServerConfig().MaxTxnOps)
	ast.Equal(uint(1048576), conf.getServerConfig().MaxRequestBytes)
	ast.Equal("simple", conf.getServerConfig().AuthToken)
	ast.Equal(uint(7), conf.getServerConfig().BcryptCost)
	ast.Equal(uint(45), conf.getServerConfig().AuthTokenTTL)
}

func TestAuthTokenProviderValidation(t *testing.T) {
	config := &Config{
		Port: 2379, PeerPort: 2380, AuthToken: "jwt,pub-key=public.pem",
		ClientSecurityConfig: &SecurityConfig{}, PeerSecurityConfig: &SecurityConfig{},
	}
	require.ErrorContains(t, config.Validate(), "invalid auth signature method")
	config.AuthToken = "jwt,sign-method=HS256,priv-key=" + getAuthPath("server.key")
	require.NoError(t, config.Validate())
	config.AuthToken = "simple"
	require.NoError(t, config.Validate())
}

// TestClientCertAuthRequiresTrustedCA pins #50: enabling client cert auth without
// a trusted CA must be rejected, not silently accepted (which would verify client
// certs against the system root pool, trusting any publicly-signed cert).
func TestClientCertAuthRequiresTrustedCA(t *testing.T) {
	ast := assert.New(t)

	// valid key pair, ClientAuth on, but NO trusted CA -> rejected.
	noCA := &SecurityConfig{
		CertFile:   getAuthPath("server.crt"),
		KeyFile:    getAuthPath("server.key"),
		ClientAuth: true,
	}
	err := noCA.validate()
	ast.Error(err, "client-cert-auth without a trusted CA must be rejected")
	ast.Contains(err.Error(), "trusted CA")

	// with a trusted CA it validates and verifies against that CA pool (not system roots).
	withCA := &SecurityConfig{
		CertFile:   getAuthPath("server.crt"),
		KeyFile:    getAuthPath("server.key"),
		CA:         getAuthPath("ca.crt"),
		ClientAuth: true,
	}
	ast.NoError(withCA.validate())
	cfg := withCA.getServerTLSConfig()
	ast.Equal(tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	ast.NotNil(cfg.ClientCAs, "must verify client certs against the configured CA pool, not system roots")
}

func TestGRPCMaxConnectionAgeValidation(t *testing.T) {
	base := func() *Config {
		return &Config{
			Port: 2379, PeerPort: 2380,
			ClientSecurityConfig: &SecurityConfig{},
			PeerSecurityConfig:   &SecurityConfig{},
		}
	}

	config := base()
	require.NoError(t, config.Validate())
	config.GRPCMaxConnectionAge = -time.Second
	require.ErrorContains(t, config.Validate(), "must not be negative")
	config = base()
	config.GRPCMaxConnectionAgeGrace = -time.Second
	require.ErrorContains(t, config.Validate(), "grace must not be negative")

	config = base()
	config.GRPCMaxConnectionAge = time.Hour
	require.ErrorContains(t, config.Validate(), "grace must be positive")

	config.GRPCMaxConnectionAgeGrace = 5 * time.Minute
	require.NoError(t, config.Validate())
}

func TestRequestLimitOverflowValidation(t *testing.T) {
	base := func() *Config {
		return &Config{
			Port: 2379, PeerPort: 2380,
			ClientSecurityConfig: &SecurityConfig{}, PeerSecurityConfig: &SecurityConfig{},
		}
	}
	config := base()
	config.MaxRequestBytes = uint(math.MaxInt - 511)
	require.ErrorContains(t, config.Validate(), "max request bytes")

	config = base()
	config.MaxTxnOps = ^uint(0)
	if uint64(config.MaxTxnOps) > uint64(math.MaxInt) {
		require.ErrorContains(t, config.Validate(), "max txn ops")
	}
}

func TestTLSPolicyValidation(t *testing.T) {
	base := func() *Config {
		return &Config{
			Port: 2379, PeerPort: 2380, TLSMinVersion: "TLS1.2",
			ClientSecurityConfig: &SecurityConfig{}, PeerSecurityConfig: &SecurityConfig{},
		}
	}

	config := base()
	config.TLSMinVersion = "TLS1.1"
	require.ErrorContains(t, config.Validate(), "unexpected TLS version")
	config = base()
	config.TLSMaxVersion = "TLS1.1"
	require.ErrorContains(t, config.Validate(), "unexpected TLS version")
	config = base()
	config.TLSMinVersion, config.TLSMaxVersion = "TLS1.3", "TLS1.2"
	require.ErrorContains(t, config.Validate(), "greater than max")
	config = base()
	config.CipherSuites = []string{"not-a-cipher"}
	require.ErrorContains(t, config.Validate(), "unexpected TLS cipher suite")
	config = base()
	config.TLSMinVersion = "TLS1.3"
	config.CipherSuites = []string{"TLS_AES_128_GCM_SHA256"}
	require.ErrorContains(t, config.Validate(), "cannot be configured")
	config = base()
	config.TLSMaxVersion = "TLS1.2"
	config.CipherSuites = []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"}
	require.NoError(t, config.Validate())
	require.Equal(t, uint16(tls.VersionTLS12), config.ClientSecurityConfig.minVersion)
	require.Equal(t, uint16(tls.VersionTLS12), config.ClientSecurityConfig.maxVersion)
	require.Equal(t, []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}, config.PeerSecurityConfig.cipherSuites)
}
