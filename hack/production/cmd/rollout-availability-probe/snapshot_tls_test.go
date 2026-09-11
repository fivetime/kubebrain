// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"golang.org/x/crypto/bcrypt"
)

// Match the deployed probe identity: a CA-signed root client with clientAuth
// only and no SAN. The CA key never leaves this fixture function.
func newClientOnlySnapshotTLSFixture(t *testing.T) restoredSnapshotTLSConfig {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "source-client-ca"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	client := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "root"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, client, ca, &clientKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	require.NoError(t, err)
	dir := t.TempDir()
	cfg := restoredSnapshotTLSConfig{
		caFile: filepath.Join(dir, "ca.crt"), certFile: filepath.Join(dir, "client.crt"),
		keyFile: filepath.Join(dir, "client.key"), serverName: "source.kubebrain.example",
	}
	require.NoError(t, os.WriteFile(cfg.caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0600))
	require.NoError(t, os.WriteFile(cfg.certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0600))
	require.NoError(t, os.WriteFile(cfg.keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600))
	leaf, err := x509.ParseCertificate(clientDER)
	require.NoError(t, err)
	require.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, leaf.ExtKeyUsage)
	require.Empty(t, leaf.DNSNames)
	require.Empty(t, leaf.IPAddresses)
	return cfg
}

// Exercise the deployed certificate identity together with the server's default
// password cost. The cheaper permission-matrix fixtures do not reproduce this
// authentication timing. A pass here is not evidence that an intermittent
// restored Watch authentication failure has been fixed.
func TestRestoredSnapshotClientOnlyTLSWithDefaultPasswordCost(t *testing.T) {
	const prefix = "/probe/auth-default-cost/"
	fixture := newSnapshotAuthFixture(prefix)
	fixture.expected.revision = 31
	fixture.expected.enabled = true
	expected := newStreamProbeExpectations(prefix)
	state := snapshotAuthTestState(t, expected, &fixture.expected)
	for index, user := range fixture.expected.users {
		if user.noPassword {
			continue
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(user.password), bcrypt.DefaultCost)
		require.NoError(t, err)
		state.Auth.Users[index].Password = hash
	}
	state.Auth.Enabled = true
	state.Auth.Users = append(state.Auth.Users, &authpb.User{
		Name: []byte("root"), Roles: []string{"root"}, Options: &authpb.UserAddOptions{NoPassword: true},
	})
	state.Auth.Roles = append(state.Auth.Roles, &authpb.Role{Name: []byte("root")})
	state = attachProductionSnapshotScale(t, prefix, expected, state)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), restoredSnapshotVerificationTimeout+10*time.Second)
	defer cancel()
	partial, err := consumeAndValidateSnapshotWithClusterAuth(ctx, snapshotAuthReceiver(t, state), dir, expected,
		newClientOnlySnapshotTLSFixture(t), &fixture.expected, 3)
	require.NoError(t, err)
	require.True(t, partial)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "restore data and ephemeral server identity must be removed")
}

func TestRestoredSnapshotDoesNotUseSourceClientAsServer(t *testing.T) {
	source := newClientOnlySnapshotTLSFixture(t)
	cfg, err := newRestoredSnapshotConfig(t.TempDir(), 3, source, nil)
	require.NoError(t, err)
	server := newRestoredSnapshotEmbedConfig(cfg, cfg.members[0], cfg.initialCluster)
	require.NotEqual(t, source.certFile, server.ClientTLSInfo.CertFile,
		"a legitimate client-only certificate cannot serve the restored etcd endpoint")
	require.NotEqual(t, source.keyFile, server.ClientTLSInfo.KeyFile)
	require.Equal(t, source, cfg.tls, "source identity and server-name settings must not be rewritten")
	for _, identity := range []struct {
		certFile string
		usage    x509.ExtKeyUsage
		cn       string
	}{
		{cfg.localTLS.server.CertFile, x509.ExtKeyUsageServerAuth, "kubebrain-snapshot-restore"},
		{cfg.localTLS.passwordClient.CertFile, x509.ExtKeyUsageClientAuth, ""},
	} {
		data, err := os.ReadFile(identity.certFile)
		require.NoError(t, err)
		block, _ := pem.Decode(data)
		require.NotNil(t, block)
		leaf, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		require.Equal(t, []x509.ExtKeyUsage{identity.usage}, leaf.ExtKeyUsage)
		require.Equal(t, identity.cn, leaf.Subject.CommonName)
		require.False(t, leaf.IsCA)
		require.LessOrEqual(t, leaf.NotAfter.Sub(leaf.NotBefore), time.Hour+time.Minute)
	}
	for _, path := range []string{cfg.localTLS.server.CertFile, cfg.localTLS.server.KeyFile,
		cfg.localTLS.server.TrustedCAFile, cfg.localTLS.passwordClient.CertFile, cfg.localTLS.passwordClient.KeyFile} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
		info, err = os.Stat(filepath.Dir(path))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	}
}

func TestConsumeAndValidateSnapshotWithClientOnlyTLS(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "enable auth after restore"
		if enabled {
			name = "restore enabled auth with certificate-only administrator"
		}
		t.Run(name, func(t *testing.T) {
			const prefix = "/probe/client-only-tls-restore/"
			fixture := newSnapshotAuthFixture(prefix)
			fixture.expected.revision = 31
			fixture.expected.enabled = enabled
			expected := newStreamProbeExpectations(prefix)
			state := snapshotAuthTestState(t, expected, &fixture.expected)
			if enabled {
				state.Auth.Enabled = true
				state.Auth.Users = append(state.Auth.Users, &authpb.User{
					Name: []byte("root"), Roles: []string{"root"}, Options: &authpb.UserAddOptions{NoPassword: true},
				})
				state.Auth.Roles = append(state.Auth.Roles, &authpb.Role{Name: []byte("root")})
			}
			state = attachProductionSnapshotScale(t, prefix, expected, state)
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), restoredSnapshotVerificationTimeout+10*time.Second)
			defer cancel()
			partial, err := consumeAndValidateSnapshotWithClusterAuth(ctx, snapshotAuthReceiver(t, state), dir, expected,
				newClientOnlySnapshotTLSFixture(t), &fixture.expected, 3)
			require.NoError(t, err)
			require.True(t, partial)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries, "restore data and ephemeral server identity must be removed")
		})
	}
}

func TestRestoredSnapshotTLSRejectsMissingLocalIdentityAndInvalidSourceCA(t *testing.T) {
	source := newClientOnlySnapshotTLSFixture(t)
	cfg, err := newRestoredSnapshotConfig(t.TempDir(), 1, source, nil)
	require.NoError(t, err)
	for name, mutate := range map[string]func(*restoredSnapshotConfig){
		"missing identity":               func(c *restoredSnapshotConfig) { c.localTLS = nil },
		"missing server key":             func(c *restoredSnapshotConfig) { c.localTLS.server.KeyFile = "" },
		"missing client trust":           func(c *restoredSnapshotConfig) { c.localTLS.server.TrustedCAFile = "" },
		"disabled client authentication": func(c *restoredSnapshotConfig) { c.localTLS.server.ClientCertAuth = false },
		"missing password client":        func(c *restoredSnapshotConfig) { c.localTLS.passwordClient.CertFile = "" },
		"source hostname reused":         func(c *restoredSnapshotConfig) { c.localTLS.server.ServerName = source.serverName },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cfg
			identity := *cfg.localTLS
			copy.localTLS = &identity
			mutate(&copy)
			_, err := copy.clientTLSConfig()
			require.ErrorContains(t, err, "independent loopback server identity")
		})
	}
	for _, missing := range []bool{false, true} {
		dir := t.TempDir()
		source.caFile = filepath.Join(t.TempDir(), "invalid-ca.crt")
		if !missing {
			require.NoError(t, os.WriteFile(source.caFile, []byte("not a certificate"), 0600))
		}
		_, err := newRestoredSnapshotConfig(dir, 1, source, nil)
		require.Error(t, err)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries)
	}
}

func TestRestoredSnapshotTLSRejectsInvalidPeers(t *testing.T) {
	source := newClientOnlySnapshotTLSFixture(t)
	cfg, err := newRestoredSnapshotConfig(t.TempDir(), 1, source, nil)
	require.NoError(t, err)
	serverTLS, err := newRestoredSnapshotEmbedConfig(cfg, cfg.members[0], cfg.initialCluster).ClientTLSInfo.ServerConfig()
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, serverTLS.ClientAuth)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.TLS.PeerCertificates[0].Subject.CommonName)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	adminTLS, err := cfg.clientTLSConfig()
	require.NoError(t, err)
	require.False(t, adminTLS.InsecureSkipVerify)
	passwordTLS, err := cfg.localTLS.passwordClient.ClientConfig()
	require.NoError(t, err)
	require.False(t, passwordTLS.InsecureSkipVerify)
	unknown := newClientOnlySnapshotTLSFixture(t)
	unknownIdentity, err := tls.LoadX509KeyPair(unknown.certFile, unknown.keyFile)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		config  *tls.Config
		mutate  func(*tls.Config)
		wantCN  string
		wantErr bool
	}{
		{name: "original certificate principal", config: adminTLS, wantCN: "root"},
		{name: "password transport has no certificate principal", config: passwordTLS},
		{name: "wrong server name", config: adminTLS, wantErr: true, mutate: func(c *tls.Config) { c.ServerName = source.serverName }},
		{name: "untrusted server", config: adminTLS, wantErr: true, mutate: func(c *tls.Config) { c.RootCAs = x509.NewCertPool() }},
		{name: "expired server", config: adminTLS, wantErr: true, mutate: func(c *tls.Config) { c.Time = func() time.Time { return time.Now().Add(2 * time.Hour) } }},
		{name: "no client identity", config: adminTLS, wantErr: true, mutate: func(c *tls.Config) { c.Certificates = nil; c.GetClientCertificate = nil }},
		{name: "untrusted client", config: adminTLS, wantErr: true, mutate: func(c *tls.Config) {
			c.Certificates = nil
			c.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &unknownIdentity, nil }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientTLS := tc.config.Clone()
			if tc.mutate != nil {
				tc.mutate(clientTLS)
			}
			tr := &http.Transport{TLSClientConfig: clientTLS}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr, Timeout: 3 * time.Second}
			response, err := client.Get(server.URL)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, tc.wantCN, string(body))
		})
	}
}
