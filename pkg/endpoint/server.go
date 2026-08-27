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
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/util"
)

type exposedServer interface {
	name() string
	matchWriters() []cmux.MatchWriter
	serve(listener net.Listener) error
	close() error
}

func matchersToMatchWriters(matchers ...cmux.Matcher) []cmux.MatchWriter {
	writers := make([]cmux.MatchWriter, 0, len(matchers))
	for _, matcher := range matchers {
		matcher := matcher
		writers = append(writers, func(_ io.Writer, reader io.Reader) bool {
			return matcher(reader)
		})
	}
	return writers
}

func normalizeServeError(err error) error {
	if err == nil {
		return nil
	}
	// errors.Is on a joined error succeeds when any child matches. Normalize
	// children independently so an expected net.ErrClosed sibling cannot hide a
	// real transport failure returned by another listener/server.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var result error
		for _, child := range joined.Unwrap() {
			result = errors.Join(result, normalizeServeError(child))
		}
		return result
	}
	if errors.Is(err, http.ErrServerClosed) ||
		errors.Is(err, grpc.ErrServerStopped) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, cmux.ErrListenerClosed) ||
		errors.Is(err, cmux.ErrServerClosed) ||
		strings.Contains(err.Error(), "use of closed network connection") {
		return nil
	}
	return err
}

func newHttpServerWithHandlers(handlersMaps ...map[string]http.Handler) exposedServer {
	return newHttpServer(newHTTPMuxWithHandlers(handlersMaps...))
}

func newHTTPMuxWithHandlers(handlersMaps ...map[string]http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	for _, handlersMap := range handlersMaps {
		for pattern, handler := range handlersMap {
			mux.Handle(pattern, handler)
		}
	}
	return mux
}

func newHTTPAccessControlledHandler(cors, hostWhitelist []string, handlersMaps ...map[string]http.Handler) http.Handler {
	mux := http.NewServeMux()
	for _, handlersMap := range handlersMaps {
		for pattern, handler := range handlersMap {
			mux.Handle(pattern, handler)
		}
	}
	return newHTTPAccessController(cors, hostWhitelist, mux)
}

type httpAccessController struct {
	cors          map[string]struct{}
	hostWhitelist map[string]struct{}
	next          http.Handler
}

func newHTTPAccessController(cors, hostWhitelist []string, next http.Handler) http.Handler {
	return &httpAccessController{
		cors:          stringSet(cors),
		hostWhitelist: stringSet(hostWhitelist),
		next:          next,
	}
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func allows(set map[string]struct{}, value string) bool {
	if len(set) == 0 {
		return true
	}
	if _, ok := set["*"]; ok {
		return true
	}
	_, ok := set[value]
	return ok
}

func requestHostname(req *http.Request) string {
	if req == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(req.Host)
	if err == nil {
		return host
	}
	return req.Host
}

func (ac *httpAccessController) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req == nil {
		http.Error(w, "Request is nil", http.StatusBadRequest)
		return
	}

	if req.URL != nil && strings.HasPrefix(req.URL.Path, "/v3beta/") {
		req.URL.Path = strings.Replace(req.URL.Path, "/v3beta/", "/v3/", 1)
	}

	host := requestHostname(req)
	if req.TLS == nil && !allows(ac.hostWhitelist, host) {
		http.Error(w, fmt.Sprintf(`
etcd received your request, but the Host header was unrecognized.

To fix this, choose one of the following options:
- Enable TLS, then any HTTPS request will be allowed.
- Add the hostname you want to use to the whitelist in settings.
  - e.g. etcd --host-whitelist %q

This requirement has been added to help prevent DNS Rebinding attacks (CVE-2018-5702).
`, host), http.StatusMisdirectedRequest)
		return
	}

	origin := req.Header.Get("Origin")
	if allows(ac.cors, "*") {
		addCORSHeaders(w, "*")
	} else if origin != "" && allows(ac.cors, origin) {
		addCORSHeaders(w, origin)
	}
	if req.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	ac.next.ServeHTTP(w, req)
}

func addCORSHeaders(w http.ResponseWriter, origin string) {
	w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Add("Access-Control-Allow-Origin", origin)
	w.Header().Add("Access-Control-Allow-Headers", "accept, content-type, authorization")
}

const (
	// Match upstream etcd (0e3027bdd): bound slow-header connections without
	// rejecting legitimate clients behind high-latency or backpressured proxies.
	// TLS handshakes retain their independent 10-second deadline.
	httpReadHeaderTimeout = 5 * time.Minute
	httpIdleTimeout       = 2 * time.Minute
	httpShutdownTimeout   = 2 * time.Second
	// Keep admitted streams active for one bounded window after Shutdown starts.
	// This makes net/http send HTTP/2 GOAWAY on a non-idle connection and gives
	// clientv3 time to establish a replacement before RPCServer retires streams.
	transportGoAwayPropagationPeriod = time.Second
	// Match net/http's default, which is also the effective limit used by
	// upstream etcd. Keeping the value explicit preserves bounded admission
	// without rejecting metadata that an etcd endpoint accepts.
	httpMaxHeaderBytes = http.DefaultMaxHeaderBytes
)

func newHttpServer(handler http.Handler) exposedServer {
	return newHTTPServer(handler)
}

func newHTTPServer(handler http.Handler) *httpServer {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	svr := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    httpMaxHeaderBytes,
		Protocols:         protocols,
	}
	shutdownStarted := make(chan struct{})
	var shutdownStartedOnce sync.Once
	svr.RegisterOnShutdown(func() {
		shutdownStartedOnce.Do(func() { close(shutdownStarted) })
	})
	return &httpServer{
		svr:             svr,
		shutdownTimeout: httpShutdownTimeout,
		shutdownStarted: shutdownStarted,
	}
}

type httpServer struct {
	svr             *http.Server
	shutdownTimeout time.Duration
	shutdownStarted <-chan struct{}
}

func (h *httpServer) name() string {
	return "http"
}

func (h *httpServer) matchWriters() []cmux.MatchWriter {
	return matchersToMatchWriters(cmux.HTTP1Fast(), cmux.HTTP2())
}

func (h *httpServer) serve(listener net.Listener) error {
	return h.svr.Serve(listener)
}

func (h *httpServer) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), h.shutdownTimeout)
	defer cancel()
	if err := h.svr.Shutdown(ctx); err != nil {
		// Long-lived watch/gateway requests may outlive the bounded rollout
		// window. Force them closed after Shutdown has stopped admission and sent
		// HTTP/2 GOAWAY; a timeout is expected in that case, while Close failures
		// still remain observable.
		closeErr := h.svr.Close()
		if errors.Is(err, context.DeadlineExceeded) {
			return closeErr
		}
		return errors.Join(err, closeErr)
	}
	return nil
}

// nativeGRPCServer keeps grpc-go in ownership of HTTP/2. grpc.Server.ServeHTTP
// instead delegates the connection to net/http, where grpc-go's keepalive,
// max-connection-age and max-concurrent-stream options are not applied.
type nativeGRPCServer struct {
	server                 *grpc.Server
	quiescing              atomic.Bool
	gracefulOnce           sync.Once
	gracefulDone           chan struct{}
	goAwayPropagationDelay time.Duration
	shutdownTimeout        time.Duration
}

func newNativeGRPCServer(server *grpc.Server) *nativeGRPCServer {
	return &nativeGRPCServer{
		server:                 server,
		gracefulDone:           make(chan struct{}),
		goAwayPropagationDelay: transportGoAwayPropagationPeriod,
		shutdownTimeout:        httpShutdownTimeout,
	}
}

func (s *nativeGRPCServer) name() string { return "grpc" }

func (s *nativeGRPCServer) matchWriters() []cmux.MatchWriter {
	// Native mode deliberately owns every HTTP/2 connection. Same-port health,
	// version and JSON gateway requests remain available over HTTP/1.1.
	return matchersToMatchWriters(cmux.HTTP2())
}

func (s *nativeGRPCServer) serve(listener net.Listener) error { return s.server.Serve(listener) }

func (s *nativeGRPCServer) startGracefulStop() {
	s.gracefulOnce.Do(func() {
		go func() {
			s.server.GracefulStop()
			close(s.gracefulDone)
		}()
	})
}

func (s *nativeGRPCServer) quiesce() {
	s.quiescing.Store(true)
	s.startGracefulStop()
	if s.goAwayPropagationDelay <= 0 {
		return
	}
	timer := time.NewTimer(s.goAwayPropagationDelay)
	defer timer.Stop()
	select {
	case <-s.gracefulDone:
	case <-timer.C:
	}
}

// quiesceAndStop is the public-client drain path. GracefulStop sends GOAWAY,
// then the propagation delay gives grpc-go clients time to create a replacement
// transport. A long-lived Watch can otherwise keep this retiring transport
// alive until leadership handoff completes and strand new unary retries on the
// old Pod. Force-close only after the bounded GOAWAY window; peer transports use
// quiesce instead and remain attached through durable handoff.
func (s *nativeGRPCServer) quiesceAndStop() {
	s.quiesce()
	select {
	case <-s.gracefulDone:
		return
	default:
	}
	s.server.Stop()
	<-s.gracefulDone
}

func (s *nativeGRPCServer) isQuiescing() bool { return s.quiescing.Load() }

func (s *nativeGRPCServer) close() error {
	s.quiescing.Store(true)
	s.startGracefulStop()
	timeout := s.shutdownTimeout
	if timeout <= 0 {
		timeout = httpShutdownTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-s.gracefulDone:
		return nil
	case <-timer.C:
		s.server.Stop()
		<-s.gracefulDone
		return nil
	}
}

func newGRPCMuxedHTTPServer(grpcServer *grpc.Server, httpHandler http.Handler) *grpcMuxedHTTPServer {
	return &grpcMuxedHTTPServer{
		grpcServer:             grpcServer,
		httpServer:             newHTTPServer(grpcHandlerFunc(grpcServer, httpHandler)),
		quiesceDone:            make(chan struct{}),
		goAwayPropagationDelay: transportGoAwayPropagationPeriod,
	}
}

type grpcMuxedHTTPServer struct {
	grpcServer             *grpc.Server
	httpServer             *httpServer
	quiescing              atomic.Bool
	quiesceDone            chan struct{}
	quiesceErr             error
	goAwayPropagationDelay time.Duration
}

func (s *grpcMuxedHTTPServer) name() string {
	return "http+grpc"
}

func (s *grpcMuxedHTTPServer) matchWriters() []cmux.MatchWriter {
	return s.httpServer.matchWriters()
}

func (s *grpcMuxedHTTPServer) serve(listener net.Listener) error {
	return s.httpServer.serve(listener)
}

func (s *grpcMuxedHTTPServer) quiesce() {
	if !s.quiescing.CompareAndSwap(false, true) {
		return
	}
	// net/http owns these gRPC HTTP/2 transports. Start Shutdown while admitted
	// streams still keep the connection non-idle, then acknowledge the transport
	// callback only after a bounded GOAWAY propagation window. RPCServer retires
	// those streams after this method returns.
	go func() {
		s.quiesceErr = s.httpServer.close()
		if s.quiesceErr != nil {
			klog.ErrorS(s.quiesceErr, "quiesce muxed HTTP/2 transport")
		}
		close(s.quiesceDone)
	}()
	select {
	case <-s.httpServer.shutdownStarted:
	case <-s.quiesceDone:
		return
	}
	if s.goAwayPropagationDelay > 0 {
		timer := time.NewTimer(s.goAwayPropagationDelay)
		<-timer.C
	}
}

func (s *grpcMuxedHTTPServer) isQuiescing() bool {
	return s.quiescing.Load()
}

func (s *grpcMuxedHTTPServer) close() error {
	// grpc.Server.ServeHTTP is owned by net/http. Match upstream etcd's
	// shutdown order: let http.Server.Shutdown send GOAWAY and drain handlers,
	// then stop gRPC. Calling http.Server.Close first drops persistent client
	// connections with EOF during a Kubernetes rollout.
	var httpErr error
	if s.quiescing.Load() {
		<-s.quiesceDone
		httpErr = s.quiesceErr
	} else {
		httpErr = s.httpServer.close()
	}
	s.grpcServer.Stop()
	return httpErr
}

func grpcHandlerFunc(grpcServer *grpc.Server, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.ProtoMajor == 2 && strings.Contains(req.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, req)
			return
		}
		httpHandler.ServeHTTP(w, req)
	})
}

type rootServer struct {
	port               int
	services           []exposedServer
	initialReadTimeout time.Duration
}

func newRootServer(port int, ss ...exposedServer) *rootServer {
	var initialReadTimeout time.Duration
	for _, service := range ss {
		if bounded, ok := service.(interface{ initialReadTimeout() time.Duration }); ok {
			candidate := bounded.initialReadTimeout()
			if candidate > 0 && (initialReadTimeout == 0 || candidate < initialReadTimeout) {
				initialReadTimeout = candidate
			}
		}
	}
	return &rootServer{
		port:               port,
		services:           ss,
		initialReadTimeout: initialReadTimeout,
	}
}

func (gs *rootServer) run(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			klog.ErrorS(err, "root servers shutdown", "port", gs.port)
		} else {
			klog.InfoS("root servers shutdown", "port", gs.port)
		}
	}()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", gs.port))
	if err != nil {
		return err
	}
	mux := cmux.New(listener)
	if gs.initialReadTimeout > 0 {
		mux.SetReadTimeout(gs.initialReadTimeout)
	}
	klog.InfoS("root server start to listen", "port", gs.port)
	return serveMuxAndServers(ctx, listener, mux, gs.services, true)
}

// runServers run servers concurrently and shutdown all servers if anyone is error
func prepareServers(ctx context.Context, mux cmux.CMux, servers []exposedServer) func() error {
	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)
	for _, server := range servers {

		// ! NOTICE: `mux.Match` is not thread-safe, so it should be called serially.
		lsn := mux.MatchWithWriters(server.matchWriters()...)

		runner := runSubServer(ctx, lsn, server)
		group.Go(func() (err error) {
			// if any sub exposedServer exit, call `cancel` to make other sub exposedServer exit
			defer cancel()
			return runner()
		})
	}
	return func() error {
		defer cancel()
		return group.Wait()
	}
}

func serveMuxAndServers(ctx context.Context, listener net.Listener, mux cmux.CMux, servers []exposedServer, failOnUnexpectedStop bool) error {
	// Register every matcher before Serve starts; cmux matcher registration is not
	// thread-safe and an early accepted connection must not race an incomplete map.
	runServers := prepareServers(ctx, mux, servers)
	muxErrCh := make(chan error, 1)
	serversErrCh := make(chan error, 1)
	go func() {
		var muxErr error
		defer func() { muxErrCh <- muxErr }()
		defer util.Recover()
		muxErr = normalizeServeError(mux.Serve())
	}()
	go func() { serversErrCh <- runServers() }()

	var muxErr, serversErr, listenerCloseErr error
	select {
	case muxErr = <-muxErrCh:
		listenerCloseErr = normalizeServeError(listener.Close())
		serversErr = <-serversErrCh
	case serversErr = <-serversErrCh:
		listenerCloseErr = normalizeServeError(listener.Close())
		muxErr = <-muxErrCh
	}
	result := errors.Join(muxErr, serversErr, listenerCloseErr)
	if result == nil && failOnUnexpectedStop && ctx.Err() == nil {
		return errors.New("root endpoint stopped while its context was still active")
	}
	return result
}

func runSubServer(ctx context.Context, lsn net.Listener, server exposedServer) func() error {
	return func() (err error) {

		closed := make(chan error, 1)
		defer func() {
			serverCloseErr := normalizeServeError(server.close())
			listenerCloseErr := normalizeServeError(lsn.Close())
			// wait until closed
			<-closed
			err = errors.Join(err, serverCloseErr, listenerCloseErr)
		}()

		// run server in a new goroutine
		go func() {
			defer util.Recover()

			// run until server is closed or has an internal error
			klog.InfoS("run server", "name", server.name(), "addr", lsn.Addr())
			serveErr := normalizeServeError(server.serve(lsn))
			if serveErr != nil {
				klog.ErrorS(serveErr, "exposed server stop", "name", server.name(), "addr", lsn.Addr())
			}
			closed <- serveErr
			close(closed)
		}()

		// Block until exposedServer stop or context done. A successful transport
		// quiesce deliberately ends Serve after GOAWAY; keep the root endpoint
		// alive until process shutdown instead of treating that as a fatal stop.
		select {
		case <-ctx.Done():
			return nil
		case serveErr := <-closed:
			if quiesced, ok := server.(interface{ isQuiescing() bool }); ok && quiesced.isQuiescing() && serveErr == nil {
				<-ctx.Done()
				return nil
			}
			return serveErr
		}
	}

}

func waitFor(ctx context.Context, closed chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-closed:
		return err
	}
}
