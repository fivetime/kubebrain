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

package endpoint

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	// Deliberately NOT importing net/http/pprof: its init() registers handlers on
	// http.DefaultServeMux, which would re-expose unauthenticated pprof the moment
	// anything serves DefaultServeMux. pprof is wired explicitly and gated behind
	// EnablePprof in pprof.go (#32).

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protojson"
	"k8s.io/klog/v2"

	etcdservergw "go.etcd.io/etcd/api/v3/etcdserverpb/gw"
	v3electiongw "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb/gw"
	v3lockgw "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb/gw"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

// Endpoint is the endpoint for serving
type Endpoint struct {
	metrics       metrics.Metrics
	backend       backend.Backend
	server        server.Server
	config        *Config
	tlsIdentities *transportidentity.Registry
}

// NewEndpoint returns an Endpoint for serving
func NewEndpoint(b backend.Backend, m metrics.Metrics, config *Config) *Endpoint {
	klog.InfoS("new endpoints", "config", config)
	return &Endpoint{
		metrics:       m,
		backend:       b,
		config:        config,
		tlsIdentities: &transportidentity.Registry{},
	}
}

// Run start grpc and http server, and block until there is any error
//
//	each port may start secure server and insecure server
//	ClientPort─┬─ Secure(TLS) ─┬─ GRPC
//	           │               └─ HTTP
//	           │
//	           └─  Insecure   ─┬─ GRPC
//	                           └─ HTTP
//
//	PeerPort  ─┬─ Secure(TLS) ─┬─ GRPC
//	           │               └─ HTTP
//	           │
//	           └─  Insecure   ─┬─ GRPC
//	                           └─ HTTP
//	InfoPort  ───  Insecure   ─── HTTP
func (e *Endpoint) Run(ctx context.Context) (err error) {
	runCtx, cancel := context.WithCancel(ctx)
	e.server = server.NewServer(runCtx, e.backend, e.metrics, e.config.getServerConfig())
	defer func() {
		cancel()
		err = errors.Join(err, e.server.Close())
		if closer, ok := e.backend.(interface{ Close() error }); ok {
			err = errors.Join(err, closer.Close())
		}
		if err != nil {
			klog.ErrorS(err, "shutdown")
		} else {
			klog.Info("shutdown complete")
		}
	}()

	group, runCtx := errgroup.WithContext(runCtx)

	group.Go(func() error {
		defer cancel()
		return e.runClientServer(runCtx)
	})

	group.Go(func() error {
		defer cancel()
		return e.runPeerServer(runCtx)
	})

	if e.config.InfoPort != 0 {
		group.Go(func() error {
			defer cancel()
			return e.runMetricsServer(runCtx)
		})

	}

	// block until all server exit
	return group.Wait()
}

func (e *Endpoint) buildExposedServers(sc *SecurityConfig, servers ...exposedServer) (ret []exposedServer) {
	mode := sc.mode()
	klog.InfoS("build exposed servers", "mode", mode)
	switch mode {
	case modeOnlyInsecure:
		return servers
	case modeOnlySecure:
		return []exposedServer{newSecureServer(sc, e.tlsIdentities, servers...)}
	case modeBothInsecureAndSecure:
		return append([]exposedServer{newSecureServer(sc, e.tlsIdentities, servers...)}, servers...)
	}
	return nil
}

func (e *Endpoint) runClientServer(ctx context.Context) error {
	clientHttp, gatewayConn, err := e.buildClientHttpServer(ctx)
	if err != nil {
		return err
	}
	if gatewayConn != nil {
		defer func() {
			if err := gatewayConn.Close(); err != nil {
				klog.ErrorS(err, "close gRPC gateway connection")
			}
		}()
	}
	clientGrpc := e.buildClientGrpcServer()
	exposedServers := e.buildExposedServers(e.config.ClientSecurityConfig, clientHttp, clientGrpc)
	clientServiceGroup := newRootServer(e.config.Port, exposedServers...)
	return clientServiceGroup.run(ctx)
}

func (e *Endpoint) runPeerServer(ctx context.Context) error {
	peerHttp := e.buildPeerHttpServer()
	peerGrpc := e.buildPeerGrpcServer()
	exposedServers := e.buildExposedServers(e.config.PeerSecurityConfig, peerHttp, peerGrpc)
	peerServiceGroup := newRootServer(e.config.PeerPort, exposedServers...)
	return peerServiceGroup.run(ctx)
}

func (e *Endpoint) runMetricsServer(ctx context.Context) error {
	metricsHttp := e.buildMetricsHttpServer()
	// The info port (metrics + opt-in pprof) was always plaintext. Wrap it with a
	// security config like the client/peer ports so it can serve TLS; an empty
	// InfoSecurityConfig stays plaintext, preserving existing behavior (#32).
	exposedServers := e.buildExposedServers(e.config.InfoSecurityConfig, metricsHttp)
	infoServiceGroup := newRootServer(e.config.InfoPort, exposedServers...)
	return infoServiceGroup.run(ctx)
}

func (e *Endpoint) buildClientHttpServer(ctx context.Context) (exposedServer, *grpc.ClientConn, error) {
	// The client port is the production data plane reachable by every etcd client.
	// It must NOT expose /metrics or /debug/pprof there: those are unauthenticated
	// info-disclosure and (pprof) CPU/heap DoS vectors. Metrics and (opt-in) pprof
	// live only on the diagnostic info port (#32). The client port serves just the
	// client HTTP handlers (health/ready) alongside the etcd gRPC service.
	handlersMaps := []map[string]http.Handler{
		e.server.GetClientHttpHandlers(),
	}
	var gatewayConn *grpc.ClientConn
	if e.config.EnableGRPCGateway {
		gateway, conn, err := e.buildGRPCGateway(ctx)
		if err != nil {
			return nil, nil, err
		}
		gatewayConn = conn
		handlersMaps = append(handlersMaps, map[string]http.Handler{"/": gateway})
	}

	return newHTTPAccessControlledServer(e.config.CORS, e.config.HostWhitelist, handlersMaps...), gatewayConn, nil
}

type gatewayRegisterFunc func(context.Context, *runtime.ServeMux, *grpc.ClientConn) error

func (e *Endpoint) buildGRPCGateway(ctx context.Context) (http.Handler, *grpc.ClientConn, error) {
	target := net.JoinHostPort("127.0.0.1", strconv.Itoa(e.config.Port))
	dialOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	}
	if e.config.ClientSecurityConfig.mode() == modeOnlySecure {
		tlsConfig := e.config.ClientSecurityConfig.getClientTLSConfig()
		if tlsConfig == nil {
			return nil, nil, fmt.Errorf("gRPC gateway requires client TLS config for TLS-only endpoint")
		}
		tlsConfig = tlsConfig.Clone()
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.ServerName = ""
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("create gRPC gateway client: %w", err)
	}

	mux, err := newGRPCGatewayMux(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return mux, conn, nil
}

func newGRPCGatewayMux(ctx context.Context, conn *grpc.ClientConn) (http.Handler, error) {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.HTTPBodyMarshaler{
			Marshaler: &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{
					UseProtoNames:   true,
					EmitUnpopulated: false,
				},
				UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
			},
		}),
	)
	registers := []gatewayRegisterFunc{
		etcdservergw.RegisterKVHandler,
		etcdservergw.RegisterWatchHandler,
		etcdservergw.RegisterLeaseHandler,
		etcdservergw.RegisterClusterHandler,
		etcdservergw.RegisterMaintenanceHandler,
		etcdservergw.RegisterAuthHandler,
		v3lockgw.RegisterLockHandler,
		v3electiongw.RegisterElectionHandler,
	}
	for _, register := range registers {
		if err := register(ctx, mux, conn); err != nil {
			return nil, fmt.Errorf("register gRPC gateway handler: %w", err)
		}
	}
	return mux, nil
}

func (e *Endpoint) buildPeerHttpServer() exposedServer {
	handlersMaps := []map[string]http.Handler{
		e.server.GetPeerHttpHandlers(),
	}

	return newHttpServerWithHandlers(handlersMaps...)
}

// grpcTransportOptions matches etcd's HTTP/2 stream and keepalive posture. etcd
// clients (the apiserver's included) send keepalive pings every ~10s on
// long-lived watch connections; gRPC's DEFAULT enforcement demands >=5min
// between pings and answers faster ones with a GoAway
// (ENHANCE_YOUR_CALM "too_many_pings"), tearing down every idle watch — the
// apiserver's informers never stabilized on a quiet cluster (#46: only
// surfaced at zero write rate, since busy connections ping rarely). etcd
// runs MinTime=5s with PermitWithoutStream=false (embed/etcd.go): a watch
// connection always has an active stream, so MinTime alone fixes the bug,
// and permitting stream-less pings would let dead-idle connections pin
// themselves open — mirror etcd exactly (review #51).
func grpcTransportOptions(config *Config) []grpc.ServerOption {
	opts := []grpc.ServerOption{grpc.MaxConcurrentStreams(config.MaxConcurrentStreams)}
	if config.GRPCKeepAliveMinTime > 0 {
		opts = append(opts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             config.GRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}))
	}
	parameters := keepalive.ServerParameters{}
	configured := false
	if config.GRPCKeepAliveInterval > 0 && config.GRPCKeepAliveTimeout > 0 {
		parameters.Time = config.GRPCKeepAliveInterval
		parameters.Timeout = config.GRPCKeepAliveTimeout
		configured = true
	}
	if config.GRPCMaxConnectionAge > 0 {
		parameters.MaxConnectionAge = config.GRPCMaxConnectionAge
		parameters.MaxConnectionAgeGrace = config.GRPCMaxConnectionAgeGrace
		configured = true
	}
	if configured {
		opts = append(opts, grpc.KeepaliveParams(parameters))
	}
	return opts
}

func (e *Endpoint) buildClientGrpcServer() exposedServer {
	grpcServer := grpc.NewServer(e.clientGrpcServerOptions()...)
	e.server.RegisterClient(grpcServer)
	return newGrpcServer(grpcServer)
}

func (e *Endpoint) clientGrpcServerOptions() []grpc.ServerOption {
	// Keep metrics interceptors outermost. They must observe calls rejected by
	// admission/require-leader interceptors before a service handler runs, matching
	// upstream etcd's serverMetrics-before-newInterceptor ordering (0c68e485a).
	opts := append(grpcTransportOptions(e.config), e.metrics.GetGrpcServerOption()...)
	opts = append(opts, grpc.StatsHandler(e.tlsIdentities))
	opts = append(opts, e.server.ClientServerOptions()...)
	return opts
}

func (e *Endpoint) buildPeerGrpcServer() exposedServer {
	grpcServer := grpc.NewServer(e.peerGrpcServerOptions()...)
	e.server.RegisterPeer(grpcServer)
	return newGrpcServer(grpcServer)
}

func (e *Endpoint) peerGrpcServerOptions() []grpc.ServerOption {
	opts := append(grpcTransportOptions(e.config), e.metrics.GetGrpcServerOption()...)
	opts = append(opts, grpc.StatsHandler(e.tlsIdentities))
	// The peer listener registers the same RPC surface for internal forwarding,
	// but keeps reserved capacity while still applying request-size and response
	// identity handling.
	opts = append(opts, e.server.PeerServerOptions()...)
	return opts
}

func (e *Endpoint) buildMetricsHttpServer() exposedServer {
	handlersMaps := []map[string]http.Handler{
		e.metrics.GetHttpHandlers(),
		e.server.GetInfoHttpHandlers(),
	}
	// pprof is a debug-only, DoS-capable surface; register it only when explicitly
	// enabled, and only here on the info port -- never on the client data port (#32).
	if e.config.EnablePprof {
		handlersMaps = append(handlersMaps, getPProfHandlers())
	}
	return newHttpServerWithHandlers(handlersMaps...)
}
