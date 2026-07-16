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

package server

import (
	"crypto/tls"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/kubewharf/kubebrain/pkg/server/service"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
)

// Config is the configuration of server
type Config struct {

	// ClientTLS is tls config for peer communication
	ClientTLS *tls.Config

	// ClientCertAuth enables etcd-compatible TLS CommonName authentication on
	// the client endpoint after the TLS layer has verified the certificate.
	ClientCertAuth bool

	// ClientAllowInsecure permits the follower proxy to fall back to plaintext
	// only when the client endpoint itself explicitly serves plaintext too.
	ClientAllowInsecure bool

	// ClientPort is the port every node's client-facing etcd endpoint listens
	// on (deployments are homogeneous). The etcd proxy dials the LEADER's
	// client endpoint with it — the peer port carried by the election identity
	// multiplexes gRPC through cmux and does not reliably serve the KV service
	// (#41: proxied counts to leader:peerPort hung to the deadline).
	ClientPort int

	// EnableEtcdProxy is the flag if etcd proxy should start
	EnableEtcdProxy bool

	// Leader-election durations (0 = default). Smaller LeaseDuration shortens the
	// failover leaderless window at the cost of more spurious failovers under load.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	MaxTxnOps       uint
	MaxRequestBytes uint

	ClusterMembers []*etcdserverpb.Member
}

func (c Config) getPeerServiceConfig() service.Config {
	return service.Config{
		TLS:             c.ClientTLS,
		AllowInsecure:   c.ClientAllowInsecure,
		EnableEtcdProxy: c.EnableEtcdProxy,
		ClientPort:      c.ClientPort,
		MaxRequestBytes: c.MaxRequestBytes,
	}
}

func (c Config) getLeaderConfig() leader.Config {
	return leader.Config{
		LeaseDuration: c.LeaseDuration,
		RenewDeadline: c.RenewDeadline,
		RetryPeriod:   c.RetryPeriod,
	}
}
