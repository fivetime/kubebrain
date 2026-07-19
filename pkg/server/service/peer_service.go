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

package service

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/server/service/revision"
)

// PeerService defines the interface provided for peer communication
type PeerService interface {
	revision.RevisionSyncer
	leader.LeaderElection
	etcdproxy.EtcdProxy
}

// Config is the config of peer revision
type Config struct {

	// TLS is the secure config of peer service client
	TLS *tls.Config
	// AllowInsecure mirrors the peer endpoint's explicit mixed TLS/plaintext
	// mode. A TLS-only deployment must never downgrade proxy traffic.
	AllowInsecure bool

	// EnableEtcdProxy is the flag if etcd proxy should start
	EnableEtcdProxy bool

	// MaxRequestBytes keeps follower forwarding at the same admission ceiling
	// as the exposed client server.
	MaxRequestBytes uint
}

// NewPeerService return a PeerService for server
func NewPeerService(ctx context.Context, le leader.LeaderElection, m metrics.Metrics, b backend.Backend, config Config) PeerService {
	ps := &peerService{
		LeaderElection: le,
		RevisionSyncer: revision.NewRevisionSyncer(b, m, le, config.TLS),
		metricCli:      m,
		EtcdProxy:      etcdproxy.NewDisabledEtcdProxy(),
		backend:        b,
		config:         config,
	}
	if config.EnableEtcdProxy {
		ps.EtcdProxy = etcdproxy.NewEtcdProxy(ctx, le, config.TLS, config.AllowInsecure, config.MaxRequestBytes)
	}
	return ps
}

type peerService struct {
	leader.LeaderElection
	etcdproxy.EtcdProxy
	revision.RevisionSyncer

	config    Config
	metricCli metrics.Metrics
	backend   backend.Backend
}

func (p *peerService) Close() error {
	var proxyErr error
	if closer, ok := p.EtcdProxy.(interface{ Close() error }); ok {
		proxyErr = closer.Close()
	}
	return errors.Join(proxyErr, p.RevisionSyncer.Close())
}

// HasLeader exposes the freshness-aware election state to etcd's
// require-leader admission without widening the PeerService interface used by
// embedders and test doubles.
func (p *peerService) HasLeader() bool {
	if availability, ok := p.LeaderElection.(interface{ HasLeader() bool }); ok {
		return availability.HasLeader()
	}
	return p.LeaderElection.IsLeader()
}
