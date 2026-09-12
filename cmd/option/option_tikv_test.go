// Copyright 2026 ByteDance and/or its affiliates
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

//go:build !badger
// +build !badger

package option

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	tikvcfg "github.com/tikv/client-go/v2/config"
)

func TestTiKVExperimentalOnePCStartupPolicy(t *testing.T) {
	// Keep global SDK state local to this non-parallel test.
	t.Cleanup(tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) {
		c.Enable1PC = true
		c.EnableAsyncCommit = true
	}))
	s := newStorageConfig()
	fs := pflag.NewFlagSet("protocol", pflag.ContinueOnError)
	s.addFlag(fs)
	require.False(t, s.experimental1PC)
	require.Equal(t, "false", fs.Lookup("experimental-tikv-enable-1pc").DefValue)
	s.configureCommitProtocol()
	require.False(t, tikvcfg.GetGlobalConfig().Enable1PC)
	require.False(t, tikvcfg.GetGlobalConfig().EnableAsyncCommit)
	require.NoError(t, fs.Parse([]string{"--experimental-tikv-enable-1pc=true"}))
	s.configureCommitProtocol()
	require.True(t, tikvcfg.GetGlobalConfig().Enable1PC)
	require.False(t, tikvcfg.GetGlobalConfig().EnableAsyncCommit)
	require.NoError(t, fs.Set("experimental-tikv-enable-1pc", "false"))
	s.configureCommitProtocol()
	require.False(t, tikvcfg.GetGlobalConfig().Enable1PC)
	require.False(t, tikvcfg.GetGlobalConfig().EnableAsyncCommit)
}

// TiKV/PD cluster TLS is mutual, so a partial cert set must be rejected up front
// rather than silently falling back to plaintext or failing deep inside the
// client-go global config (#33).
func TestTiKVTLSValidateRequiresAllOrNothing(t *testing.T) {
	cases := []struct {
		name    string
		ca      string
		cert    string
		key     string
		wantErr bool
	}{
		{name: "plaintext (none set)", wantErr: false},
		{name: "full mTLS", ca: "/ca.crt", cert: "/tls.crt", key: "/tls.key", wantErr: false},
		{name: "ca only", ca: "/ca.crt", wantErr: true},
		{name: "cert only", cert: "/tls.crt", wantErr: true},
		{name: "key only", key: "/tls.key", wantErr: true},
		{name: "cert+key, no ca", cert: "/tls.crt", key: "/tls.key", wantErr: true},
		{name: "ca+cert, no key", ca: "/ca.crt", cert: "/tls.crt", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &storageConfig{
				pdAddrs:   []string{"127.0.0.1:2379"},
				clientNum: 1,
				caFile:    tc.ca,
				certFile:  tc.cert,
				keyFile:   tc.key,
			}
			err := s.validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTiKVClientNumValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		clientNum int
		wantErr   bool
	}{
		{name: "zero", clientNum: 0, wantErr: true},
		{name: "negative", clientNum: -1, wantErr: true},
		{name: "one", clientNum: 1},
		{name: "maximum", clientNum: 128},
		{name: "above maximum", clientNum: 129, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (&storageConfig{pdAddrs: []string{"127.0.0.1:2379"}, clientNum: tc.clientNum}).validate()
			if tc.wantErr {
				require.ErrorContains(t, err, "--tikv-client-num")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// The --tikv-* flags must bind to the storageConfig fields that buildStorage
// forwards to storagetikv.Security, or a configured data-plane TLS silently
// stays plaintext (#33).
func TestTiKVTLSFlagsBindToStorageConfig(t *testing.T) {
	o := NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)

	require.NoError(t, fs.Parse([]string{
		"--pd-addrs=pd-0:2379,pd-1:2379",
		"--tikv-ca-file=/tikv/ca.crt",
		"--tikv-cert-file=/tikv/tls.crt",
		"--tikv-key-file=/tikv/tls.key",
		"--tikv-verify-cn=tikv,pd",
	}))

	require.Equal(t, []string{"pd-0:2379", "pd-1:2379"}, o.storageConfig.pdAddrs)
	require.Equal(t, "/tikv/ca.crt", o.storageConfig.caFile)
	require.Equal(t, "/tikv/tls.crt", o.storageConfig.certFile)
	require.Equal(t, "/tikv/tls.key", o.storageConfig.keyFile)
	require.Equal(t, []string{"tikv", "pd"}, o.storageConfig.verifyCN)
}
