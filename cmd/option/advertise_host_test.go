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

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// On a multi-homed host util.GetHost picks the lexicographically smallest
// private IPv4, which is almost never the intended peer network. --advertise-host
// must win over auto-detect so the leader-election identity and the follower→leader
// dial address are pinned to the operator's chosen interface.
func TestBuildIdentityAdvertiseHostWinsOverAutoDetect(t *testing.T) {
	o := NewOptions()
	o.advertiseHost = "10.32.32.101" // NOT necessarily this box's smallest private IPv4
	id, err := o.buildIdentity()
	require.NoError(t, err)
	require.Equal(t, "10.32.32.101:2380", id, "identity must use the advertised host + peer port verbatim")
}

// A custom peer port must be reflected in the identity host:port.
func TestBuildIdentityUsesPeerPort(t *testing.T) {
	o := NewOptions()
	o.advertiseHost = "10.32.32.101"
	o.epsConf.PeerPort = 3380
	id, err := o.buildIdentity()
	require.NoError(t, err)
	require.Equal(t, "10.32.32.101:3380", id)
}

// The flag must bind to the field.
func TestAdvertiseHostFlagBinds(t *testing.T) {
	o := NewOptions()
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--advertise-host=10.32.32.101", "--peer-port=3380"}))
	require.Equal(t, "10.32.32.101", o.advertiseHost)

	id, err := o.buildIdentity()
	require.NoError(t, err)
	require.Equal(t, "10.32.32.101:3380", id)
}

func TestAdvertiseClientURLsFlagBinds(t *testing.T) {
	o := NewOptions()
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"--advertise-client-urls=https://etcd-a.example.com:2379,https://etcd-b.example.com:2379",
	}))
	require.Equal(t, []string{
		"https://etcd-a.example.com:2379", "https://etcd-b.example.com:2379",
	}, o.advertiseClientURLs)
}

func TestValidateRejectsInvalidAdvertiseClientURLs(t *testing.T) {
	o := NewOptions()
	o.advertiseClientURLs = []string{"http://etcd.example.com"}
	err := o.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "advertise-client-urls")
}

// A bare IPv6 (no brackets) would be mangled when joined with the peer port
// (SplitHostPort'd downstream by the etcd proxy). Validate must reject it up
// front; the bracketed form and IPv4 must pass this check. The advertise-host
// check runs before storage validation, so the returned error is deterministic
// regardless of storage configuration.
func TestValidateRejectsUnbracketedIPv6AdvertiseHost(t *testing.T) {
	o := NewOptions()
	o.advertiseHost = "2001:db8::1" // unbracketed -> "2001:db8::1:2380" is ambiguous
	err := o.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "advertise-host")
}

func TestValidateAcceptsBracketedIPv6AndIPv4(t *testing.T) {
	// These must pass the advertise-host check specifically. We assert that the
	// error, if any, is NOT the advertise-host error (storage validation may
	// still fail, which is unrelated to this flag).
	for _, host := range []string{"[2001:db8::1]", "10.32.32.101"} {
		o := NewOptions()
		o.advertiseHost = host
		if err := o.Validate(); err != nil {
			require.NotContains(t, err.Error(), "advertise-host",
				"host %q must pass the advertise-host validation", host)
		}
	}
}
