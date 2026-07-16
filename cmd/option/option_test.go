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

package option

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestTLSFlagsBindToExpectedSecurityConfigFields(t *testing.T) {
	o := NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)

	require.NoError(t, fs.Parse([]string{
		"--cert-file=/client/tls.crt",
		"--key-file=/client/tls.key",
		"--client-cert-file=/client/outbound.crt",
		"--client-key-file=/client/outbound.key",
		"--trusted-ca-file=/client/ca.crt",
		"--client-cert-allowed-hostname=client-one.example,client-two.example",
		"--tls-server-name=kubebrain-client.kubebrain-system.svc",
		"--allow-insecure=true",
		"--peer-cert-file=/peer/tls.crt",
		"--peer-key-file=/peer/tls.key",
		"--peer-client-cert-file=/peer/outbound.crt",
		"--peer-client-key-file=/peer/outbound.key",
		"--peer-trusted-ca-file=/peer/ca.crt",
		"--peer-cert-allowed-cn=peer-one,peer-two",
		"--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc",
		"--peer-allow-insecure=true",
		"--client-crl-file=/client/revoked.crl",
		"--peer-crl-file=/peer/revoked.crl",
		"--info-crl-file=/info/revoked.crl",
		"--tls-min-version=TLS1.2",
		"--tls-max-version=TLS1.2",
		"--cipher-suites=TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
		"--grpc-max-connection-age=1h",
		"--grpc-max-connection-age-grace=5m",
		"--max-txn-ops=64",
		"--max-request-bytes=1048576",
	}))

	require.Equal(t, "/client/tls.crt", o.epsConf.ClientSecurityConfig.CertFile)
	require.Equal(t, "/client/tls.key", o.epsConf.ClientSecurityConfig.KeyFile)
	require.Equal(t, "/client/outbound.crt", o.epsConf.ClientSecurityConfig.ClientCertFile)
	require.Equal(t, "/client/outbound.key", o.epsConf.ClientSecurityConfig.ClientKeyFile)
	require.Equal(t, "/client/ca.crt", o.epsConf.ClientSecurityConfig.CA)
	require.Equal(t, []string{"client-one.example", "client-two.example"}, o.epsConf.ClientSecurityConfig.AllowedHostnames)
	require.Equal(t, "kubebrain-client.kubebrain-system.svc", o.epsConf.ClientSecurityConfig.ServerName)
	require.True(t, o.epsConf.ClientSecurityConfig.AllowInsecure)
	require.Equal(t, "/peer/tls.crt", o.epsConf.PeerSecurityConfig.CertFile)
	require.Equal(t, "/peer/tls.key", o.epsConf.PeerSecurityConfig.KeyFile)
	require.Equal(t, "/peer/outbound.crt", o.epsConf.PeerSecurityConfig.ClientCertFile)
	require.Equal(t, "/peer/outbound.key", o.epsConf.PeerSecurityConfig.ClientKeyFile)
	require.Equal(t, "/peer/ca.crt", o.epsConf.PeerSecurityConfig.CA)
	require.Equal(t, []string{"peer-one", "peer-two"}, o.epsConf.PeerSecurityConfig.AllowedCNs)
	require.Equal(t, "kubebrain-peer.kubebrain-system.svc", o.epsConf.PeerSecurityConfig.ServerName)
	require.True(t, o.epsConf.PeerSecurityConfig.AllowInsecure)
	require.Equal(t, "/client/revoked.crl", o.epsConf.ClientSecurityConfig.CRL)
	require.Equal(t, "/peer/revoked.crl", o.epsConf.PeerSecurityConfig.CRL)
	require.Equal(t, "/info/revoked.crl", o.epsConf.InfoSecurityConfig.CRL)
	require.Equal(t, "TLS1.2", o.epsConf.TLSMinVersion)
	require.Equal(t, "TLS1.2", o.epsConf.TLSMaxVersion)
	require.Equal(t, []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"}, o.epsConf.CipherSuites)
	require.Equal(t, time.Hour, o.epsConf.GRPCMaxConnectionAge)
	require.Equal(t, 5*time.Minute, o.epsConf.GRPCMaxConnectionAgeGrace)
	require.Equal(t, uint(64), o.epsConf.MaxTxnOps)
	require.Equal(t, uint(1048576), o.epsConf.MaxRequestBytes)
}

// TestWatchProgressNotifyIntervalValidation locks the k8s-1.37-review guard:
// kube-apiserver blocks consistent reads on watch progress for only 3s before
// falling back to a full storage LIST, so an interval at/above that cliff must
// be rejected at startup instead of silently degrading every consistent read.
func TestWatchProgressNotifyIntervalValidation(t *testing.T) {
	newValid := func() *KubeBrainOption {
		o := NewOptions()
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		o.AddFlags(fs)
		require.NoError(t, fs.Parse([]string{"--pd-addrs=127.0.0.1:2379"}))
		return o
	}

	o := newValid()
	require.NoError(t, o.Validate(), "default 1s must validate")

	o = newValid()
	o.watchProgressNotifyInterval = 2 * time.Second
	require.NoError(t, o.Validate(), "2s is under the 2.5s cap")

	o = newValid()
	o.watchProgressNotifyInterval = 3 * time.Second
	err := o.Validate()
	require.Error(t, err, ">=2.5s must be rejected")
	require.Contains(t, err.Error(), "watch-progress-notify-interval")

	// <=0 keeps the "use built-in default" semantic and must stay accepted.
	o = newValid()
	o.watchProgressNotifyInterval = 0
	require.NoError(t, o.Validate())
}
