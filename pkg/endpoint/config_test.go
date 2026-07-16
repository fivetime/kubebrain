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
	"testing"

	"github.com/stretchr/testify/assert"
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
		Port:     2379,
		PeerPort: 2380,
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
