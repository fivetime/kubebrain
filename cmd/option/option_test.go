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
		"--trusted-ca-file=/client/ca.crt",
		"--tls-server-name=kubebrain-client.kubebrain-system.svc",
		"--allow-insecure=true",
		"--peer-cert-file=/peer/tls.crt",
		"--peer-key-file=/peer/tls.key",
		"--peer-trusted-ca-file=/peer/ca.crt",
		"--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc",
		"--peer-allow-insecure=true",
		"--grpc-max-connection-age=1h",
		"--grpc-max-connection-age-grace=5m",
	}))

	require.Equal(t, "/client/tls.crt", o.epsConf.ClientSecurityConfig.CertFile)
	require.Equal(t, "/client/tls.key", o.epsConf.ClientSecurityConfig.KeyFile)
	require.Equal(t, "/client/ca.crt", o.epsConf.ClientSecurityConfig.CA)
	require.Equal(t, "kubebrain-client.kubebrain-system.svc", o.epsConf.ClientSecurityConfig.ServerName)
	require.True(t, o.epsConf.ClientSecurityConfig.AllowInsecure)
	require.Equal(t, "/peer/tls.crt", o.epsConf.PeerSecurityConfig.CertFile)
	require.Equal(t, "/peer/tls.key", o.epsConf.PeerSecurityConfig.KeyFile)
	require.Equal(t, "/peer/ca.crt", o.epsConf.PeerSecurityConfig.CA)
	require.Equal(t, "kubebrain-peer.kubebrain-system.svc", o.epsConf.PeerSecurityConfig.ServerName)
	require.True(t, o.epsConf.PeerSecurityConfig.AllowInsecure)
	require.Equal(t, time.Hour, o.epsConf.GRPCMaxConnectionAge)
	require.Equal(t, 5*time.Minute, o.epsConf.GRPCMaxConnectionAgeGrace)
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
