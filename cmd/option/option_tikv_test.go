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
)

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
