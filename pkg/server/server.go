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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/brain"
	"github.com/kubewharf/kubebrain/pkg/server/etcd"
	"github.com/kubewharf/kubebrain/pkg/server/service"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/server/service/revision"
)

// Server is the application layer server providing services for clients and peers
type Server interface {
	// RegisterClient registers grpc service for clients
	RegisterClient(server *grpc.Server)

	// ClientServerOptions returns grpc.ServerOptions that must be applied when
	// building the CLIENT grpc server (before service registration) — currently
	// request admission and etcd response-header stamping interceptors.
	ClientServerOptions() []grpc.ServerOption

	// PeerServerOptions returns response options for the peer listener. It
	// intentionally excludes public-client admission so internal coordination
	// retains capacity during client overload.
	PeerServerOptions() []grpc.ServerOption

	// RegisterPeer registers grpc service for peer
	RegisterPeer(server *grpc.Server)

	// GetClientHttpHandlers returns http handlers for client
	GetClientHttpHandlers() map[string]http.Handler

	// GetPeerHttpHandlers returns http handlers for peer
	GetPeerHttpHandlers() map[string]http.Handler

	// GetInfoHttpHandlers returns http handlers for node info
	GetInfoHttpHandlers() map[string]http.Handler

	// Close stops background service work and releases client resources.
	Close() error
}

type server struct {
	// etcd protocol grpc server
	etcdServer *etcd.RPCServer
	// kube brain protocol grpc server
	brainServer *brain.Server
	// health server to tell client whether this instance is leader
	healthServer *health.Server

	leaderElection leader.LeaderElection
	peers          service.PeerService
	metricCli      metrics.Metrics
	backend        backend.Backend

	config Config

	cancel           context.CancelFunc
	campaignDone     chan struct{}
	quotaMetricsDone chan struct{}
	closeOnce        sync.Once
	closeErr         error
}

func (s *server) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.campaignDone != nil {
			<-s.campaignDone
		}
		if s.quotaMetricsDone != nil {
			<-s.quotaMetricsDone
		}
		if s.etcdServer != nil {
			s.etcdServer.Close()
		}
		if s.peers != nil {
			s.closeErr = s.peers.Close()
		}
	})
	return s.closeErr
}

// NewServer returns the server
func NewServer(ctx context.Context, backend backend.Backend, metricCli metrics.Metrics, config Config) Server {
	runCtx, cancel := context.WithCancel(ctx)
	s := &server{
		// health server to tell client whether this instance is leader
		healthServer:     health.NewServer(),
		metricCli:        metricCli,
		backend:          backend,
		config:           config,
		cancel:           cancel,
		campaignDone:     make(chan struct{}),
		quotaMetricsDone: make(chan struct{}),
	}
	// leader election callbacks are methods on s; s.etcdServer is assigned below
	// (before Campaign runs) and read by onStartedLeading.
	election := leader.NewLeaderElection(
		backend, metricCli, s.onPreparingLeading, s.onStartedLeading, s.onStoppedLeading,
		config.getLeaderConfig(),
	)
	// Wire the write fence: the backend re-checks this leadership epoch/freshness
	// immediately before every data commit, so a deposed leader's in-flight write
	// is rejected instead of committed-yet-unwatched (FINDING #39).
	backend.SetLeadershipFence(election.EpochAndLeadingFresh)
	// revisionSyncer sync revision from leader to follower
	peerService := service.NewPeerService(runCtx, election, metricCli, backend, config.getPeerServiceConfig())
	// construct etcd & brian grpc server
	s.etcdServer = etcd.New(backend, metricCli, peerService)
	s.etcdServer.SetRequestLimits(config.MaxTxnOps, config.MaxRequestBytes)
	s.etcdServer.SetMaxRequestsInFlight(config.MaxRequestsInFlight)
	s.etcdServer.SetRequestRateLimit(config.MaxRequestRate, config.RequestRateBurst)
	s.etcdServer.SetMaxDeleteRangeKeys(config.MaxDeleteRangeKeys)
	s.etcdServer.SetMaxWatches(config.MaxWatches)
	s.etcdServer.SetAuthConfiguration(config.AuthToken, config.BcryptCost, config.AuthTokenTTL)
	s.etcdServer.SetClientCertAuth(config.ClientCertAuth)
	// MemberList ClientURLs: advertise the homogeneous client port with the
	// scheme clients actually dial (https iff the client port serves TLS),
	// instead of the peer identity's http://host:peerPort.
	s.etcdServer.SetAdvertiseClientInfo(config.ClientPort, config.ClientTLS != nil, config.AdvertiseClientURLs...)
	s.etcdServer.SetStaticMembers(config.ClusterMembers)
	s.brainServer = brain.New(backend, metricCli, peerService)
	s.leaderElection = election
	s.peers = peerService
	go func() {
		defer close(s.campaignDone)
		peerService.Campaign(runCtx)
	}()
	go func() {
		defer close(s.quotaMetricsDone)
		s.runQuotaMetricsRefresh(runCtx, quotaMetricsRefreshInterval, quotaMetricsRefreshTimeout)
	}()
	return s
}

const quotaMetricsRefreshInterval = 15 * time.Second
const quotaMetricsRefreshTimeout = 5 * time.Second

func (s *server) refreshQuotaMetrics(ctx context.Context) {
	_, _, _, err := s.backend.QuotaStatus(ctx)
	if err == nil || errors.Is(err, backend.ErrQuotaUninitialized) {
		return
	}
	s.metricCli.EmitCounter("quota.refresh.err", 1)
	klog.ErrorS(err, "refresh quota metrics from shared storage failed")
}

func (s *server) runQuotaMetricsRefresh(ctx context.Context, interval, timeout time.Duration) {
	refresh := func() {
		refreshCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		s.refreshQuotaMetrics(refreshCtx)
	}
	refresh()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

// onStartedLeading is invoked when this instance acquires leadership.
// leaderReloadRetryInterval is how long onStartedLeading waits before retrying a
// failed lease reload; until it succeeds the node does not advertise readiness.
const leaderReloadRetryInterval = time.Second

func (s *server) onPreparingLeading() {
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	if s.etcdServer != nil {
		s.etcdServer.PrepareLeaseReload()
	}
}

func (s *server) onStartedLeading(ctx context.Context) {
	// Register this leadership lifecycle before any other startup work. Physical
	// compaction and the TiKV GC-safepoint driver both derive their in-flight
	// contexts from it, so a term loss cancels shared-storage maintenance even
	// while lease/event initialization is still retrying.
	for {
		err := s.backend.ResumePhysicalCompaction(ctx)
		if err == nil {
			break
		}
		s.metricCli.EmitCounter("compact.resume.err", 1)
		klog.ErrorS(err, "resume physical compaction on leadership acquisition failed; retrying before serving")
		select {
		case <-ctx.Done():
			return
		case <-time.After(leaderReloadRetryInterval):
		}
	}
	for {
		err := s.backend.EnsureQuotaInitialized(ctx)
		if err == nil {
			break
		}
		s.metricCli.EmitCounter("quota.initialize.err", 1)
		klog.ErrorS(err, "quota usage initialization failed; retrying before serving")
		select {
		case <-ctx.Done():
			return
		case <-time.After(leaderReloadRetryInterval):
		}
	}
	// Reconstruct lease state from storage BEFORE advertising readiness (review
	// #6). A stale follower snapshot's expiry timers would otherwise wrongly delete
	// kept-alive leases or orphan newly-granted ones. Retry on failure rather than
	// serving with unreconstructed lease state (the old code logged the error and
	// proceeded). ctx cancellation (lost leadership / shutdown) ends the wait.
	if s.etcdServer != nil {
		for {
			err := s.etcdServer.ReloadLeases(ctx)
			if err == nil {
				break
			}
			s.metricCli.EmitCounter("lease.reload.err", 1)
			klog.ErrorS(err, "reload leases on leadership acquisition failed; retrying before serving")
			select {
			case <-ctx.Done():
				return
			case <-time.After(leaderReloadRetryInterval):
			}
		}
	}
	// Push the event log's completeness watermark to this term's start BEFORE
	// advertising readiness (#45, review #51): the failover reconnect herd
	// arrives the moment SERVING flips, and until the watermark is advanced the
	// stored value still vouches for the previous term — whose writer may have
	// been an older binary that wrote no log entries at all — so a replay would
	// serve that window with silent holes. Failure must retry, not proceed: a
	// failed advance leaves the OLD watermark in place (the log keeps serving,
	// wrongly), not "unavailable" as the previous code assumed.
	for {
		err := s.backend.EnsureEventLogStart(ctx)
		if err == nil {
			break
		}
		s.metricCli.EmitCounter("event_log.ensure.err", 1)
		klog.ErrorS(err, "event log start initialization failed; retrying before serving")
		select {
		case <-ctx.Done():
			return
		case <-time.After(leaderReloadRetryInterval):
		}
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	// The count index can trail readiness: until it is Ready() at a revision,
	// counts fall back to a full scan (never a wrong count), so a rebuild failure
	// must not gate serving. Rebuild from a fresh snapshot; a follower's collector
	// did not maintain it while it was not leading.
	if err := s.backend.RebuildCountIndex(ctx); err != nil {
		s.metricCli.EmitCounter("count_index.rebuild.err", 1)
		klog.ErrorS(err, "rebuild count index on leadership acquisition failed")
	}
}

// onStoppedLeading is invoked when this instance loses leadership. It must report
// NOT_SERVING so health-checking clients/load balancers stop routing to this node
// as leader — previously it wrongly set SERVING (#61).
func (s *server) onStoppedLeading() {
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	// On losing leadership, stop the lease expiry timers and drop the now
	// non-authoritative lease snapshot; the leader owns lease expiry and the new
	// leader has advanced this state. Re-acquiring leadership reloads it (#57).
	if s.etcdServer != nil {
		s.etcdServer.StopLeases()
	}
}

// RegisterClient implements Server interface
func (s *server) RegisterClient(server *grpc.Server) {
	s.register(server)
}

// ClientServerOptions implements Server interface
func (s *server) ClientServerOptions() []grpc.ServerOption {
	return s.etcdServer.ClientServerOptions()
}

// PeerServerOptions implements Server interface.
func (s *server) PeerServerOptions() []grpc.ServerOption {
	return s.etcdServer.PeerServerOptions()
}

// RegisterPeer implement Server interface
func (s *server) RegisterPeer(server *grpc.Server) {
	s.register(server)
}

func (s *server) register(server *grpc.Server) {
	// register grpc method to grpc server
	s.etcdServer.Register(server)
	s.brainServer.Register(server)
	// set NOT_SERVEING when initialized
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, s.healthServer)
}

// GetClientHttpHandlers implements Server interface
func (s *server) GetClientHttpHandlers() map[string]http.Handler {
	handlers := map[string]http.Handler{
		"/health":  http.HandlerFunc(s.httpHealthHandler),
		"/ping":    http.HandlerFunc(s.httpPingHandler),
		"/ready":   http.HandlerFunc(s.httpReadyHandler),
		"/version": http.HandlerFunc(s.versionHandler),
	}
	s.addEtcdHealthCheckHandlers(handlers)
	return handlers
}

// GetPeerHttpHandlers implements Server interface
func (s *server) GetPeerHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/status": http.HandlerFunc(s.revisionHandler),
	}
}

// GetInfoHttpHandlers implements Server interface
func (s *server) GetInfoHttpHandlers() map[string]http.Handler {
	handlers := map[string]http.Handler{
		"/health":   http.HandlerFunc(s.httpHealthHandler),
		"/ping":     http.HandlerFunc(s.httpPingHandler),
		"/ready":    http.HandlerFunc(s.httpReadyHandler),
		"/status":   http.HandlerFunc(s.revisionHandler),
		"/election": http.HandlerFunc(s.electionHandler),
		// kubeadm 1.37's ExternalEtcd.HTTPEndpoints lets users point etcd HTTP
		// probes at a separate port from gRPC; its preflight GETs /version there
		// and treats a 404 as a fatal parse error. Serve it on the info port too
		// so either port satisfies the check.
		"/version": http.HandlerFunc(s.versionHandler),
	}
	s.addEtcdHealthCheckHandlers(handlers)
	return handlers
}

func (s *server) electionHandler(w http.ResponseWriter, req *http.Request) {
	info, err := s.leaderElection.GetElectionInfo()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	respBytes, _ := json.Marshal(info)
	w.Write(respBytes)
	return
}

func (s *server) revisionHandler(w http.ResponseWriter, req *http.Request) {
	if !s.leaderElection.IsLeader() {
		s.metricCli.EmitCounter("leader.invalid", 1)
		w.WriteHeader(400)
		w.Write([]byte("i'm not leader, so can't tell you revision"))
		return
	}
	rev := s.backend.GetCurrentRevision()
	w.WriteHeader(200)
	responseBody, _ := json.Marshal(&revision.LeaderRevision{
		Revision: rev,
	})
	s.metricCli.EmitGauge("leader.revision", rev)
	w.Write(responseBody)
}

const (
	HealthResponse       = `{"health":"true","reason":""}`
	healthCheckTimeout   = 5 * time.Second
	healthNoLeaderReason = "RAFT NO LEADER"
	healthNoSpaceReason  = "ALARM NOSPACE"
)

type healthResponse struct {
	Health string `json:"health"`
	Reason string `json:"reason"`
}

// versionHandler serves the etcd-compatible GET /version endpoint. kubeadm's
// ExternalEtcdVersion preflight (and other etcd tooling) GETs this and parses
// {"etcdserver":...,"etcdcluster":...}; without it the 404 body "404 page not
// found" is mis-parsed as the JSON number 404. The version string is the same
// single source of truth reported by the Maintenance.Status gRPC (etcd.Version).
func (s *server) versionHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		klog.Warningf("/version error (status code %d)", http.StatusMethodNotAllowed)
		return
	}
	respBytes, _ := json.Marshal(struct {
		EtcdServer  string `json:"etcdserver"`
		EtcdCluster string `json:"etcdcluster"`
	}{EtcdServer: etcd.Version, EtcdCluster: etcd.Version})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}

func (s *server) httpHealthHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		klog.Warningf("/health error (status code %d)", http.StatusMethodNotAllowed)
		return
	}
	serializable := req.URL.Query().Get("serializable") == "true"
	excludedAlarms := make(map[string]struct{})
	for _, alarm := range req.URL.Query()["exclude"] {
		if alarm != "" {
			excludedAlarms[alarm] = struct{}{}
		}
	}
	if reason := s.healthAlarmFailureReason(req.Context(), excludedAlarms); reason != "" {
		s.recordLegacyHealth(false)
		s.writeUnhealthy(w, reason)
		return
	}
	if reason := s.healthFailureReason(req.Context(), serializable); reason != "" {
		s.recordLegacyHealth(false)
		s.writeUnhealthy(w, reason)
		return
	}
	s.recordLegacyHealth(true)
	s.writeLegacyHealthHealthy(w)
}

func (s *server) recordLegacyHealth(success bool) {
	if s.metricCli == nil {
		return
	}
	name := "etcd.server.health_failures"
	if success {
		name = "etcd.server.health_success"
	}
	_ = s.metricCli.EmitCounter(name, 1)
}

func (s *server) httpPingHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	s.writeHealthy(w)
}

func (s *server) writeHealthy(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(HealthResponse))
}

func (s *server) writeLegacyHealthHealthy(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(HealthResponse))
}

func (s *server) healthAlarmFailureReason(ctx context.Context, excluded map[string]struct{}) string {
	if _, ok := excluded["NOSPACE"]; ok {
		return ""
	}
	if s.backend == nil {
		return ""
	}
	_, _, noSpace, err := s.backend.QuotaStatus(ctx)
	if err != nil {
		return "ALARM ERROR:" + err.Error()
	}
	if noSpace {
		return healthNoSpaceReason
	}
	return ""
}

func (s *server) writeUnhealthy(w http.ResponseWriter, reason string) {
	body, _ := json.Marshal(healthResponse{Health: "false", Reason: reason})
	http.Error(w, string(body), http.StatusServiceUnavailable)
}

func (s *server) healthFailureReason(ctx context.Context, serializable bool) string {
	if !serializable && !s.requestPathReady() {
		return healthNoLeaderReason
	}
	err := s.readHealthCheck(ctx, serializable)
	if err != nil {
		return "RANGE ERROR:" + err.Error()
	}
	return ""
}

func (s *server) readHealthCheck(ctx context.Context, serializable bool) error {
	if !serializable && !s.requestPathReady() {
		return errors.New(healthNoLeaderReason)
	}
	if s.backend == nil {
		return errors.New("backend is not initialized")
	}
	checkCtx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	if !serializable && s.brainServer != nil {
		_, err := s.brainServer.Get(checkCtx, &proto.GetRequest{Key: []byte{0}})
		return err
	}
	_, err := s.backend.Get(checkCtx, &proto.GetRequest{Key: []byte{0}})
	return err
}

func (s *server) requestPathReady() bool {
	if s.leaderElection != nil && s.leaderElection.IsLeader() && s.leaderServing() {
		return true
	}
	if s.config.EnableEtcdProxy && s.peers != nil && s.peers.EtcdProxyEnabled() {
		return s.peers.Ready() == nil
	}
	return false
}

func (s *server) httpReadyHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		klog.Warningf("/ready error (status code %d)", http.StatusMethodNotAllowed)
		return
	}
	if reason := s.healthFailureReason(req.Context(), false); reason != "" {
		s.writeUnhealthy(w, reason)
		return
	}
	s.writeHealthy(w)
}

func (s *server) leaderServing() bool {
	resp, err := s.healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{})
	return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
}
