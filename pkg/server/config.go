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

	// ClientPort and ClientTLS describe the advertised public endpoint.
	ClientPort int
	// ProxyTLS and ProxyAllowInsecure describe the internal peer endpoint used
	// for follower-to-leader forwarding. Proxy traffic must not re-enter public
	// admission on the leader and consume a second tenant token/slot.
	ProxyTLS           *tls.Config
	ProxyAllowInsecure bool

	// EnableEtcdProxy is the flag if etcd proxy should start
	EnableEtcdProxy bool

	// Leader-election durations (0 = default). Smaller LeaseDuration shortens the
	// failover leaderless window at the cost of more spurious failovers under load.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	MaxTxnOps       uint
	MaxRequestBytes uint
	// MaxRequestsInFlight limits concurrent public client RPCs per process.
	// Zero preserves etcd's unlimited default. Peer RPCs are deliberately
	// excluded so overload cannot block leader and revision coordination.
	MaxRequestsInFlight uint32
	// MaxRequestRate limits public client request messages per second. Unary
	// RPCs consume one token; streaming RPCs consume one per inbound message.
	// RequestRateBurst is the token bucket capacity. Both zero disable it.
	MaxRequestRate   uint32
	RequestRateBurst uint32
	// MaxDeleteRangeKeys bounds keys in one atomic range deletion. Zero keeps
	// etcd's unlimited behavior.
	MaxDeleteRangeKeys uint32
	// MaxWatches limits active logical watches per process. One gRPC Watch
	// stream can multiplex many watches, so the RPC limit cannot substitute it.
	// Zero preserves etcd's unlimited behavior.
	MaxWatches   uint32
	AuthToken    string
	BcryptCost   uint
	AuthTokenTTL uint

	ClusterMembers      []*etcdserverpb.Member
	AdvertiseClientURLs []string
}

func (c Config) getPeerServiceConfig() service.Config {
	return service.Config{
		TLS:             c.ProxyTLS,
		AllowInsecure:   c.ProxyAllowInsecure,
		EnableEtcdProxy: c.EnableEtcdProxy,
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
