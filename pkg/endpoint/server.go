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
	"net"
	"net/http"
	"time"

	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/util"
)

type exposedServer interface {
	name() string
	matcher() cmux.Matcher
	serve(listener net.Listener) error
	close() error
}

func normalizeServeError(err error) error {
	if errors.Is(err, http.ErrServerClosed) ||
		errors.Is(err, grpc.ErrServerStopped) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, cmux.ErrListenerClosed) ||
		errors.Is(err, cmux.ErrServerClosed) {
		return nil
	}
	return err
}

func newHttpServerWithHandlers(handlersMaps ...map[string]http.Handler) exposedServer {
	mux := http.NewServeMux()
	for _, handlersMap := range handlersMaps {
		for pattern, handler := range handlersMap {
			mux.Handle(pattern, handler)
		}
	}
	return newHttpServer(mux)
}

func newHTTPAccessControlledServer(cors, hostWhitelist []string, handlersMaps ...map[string]http.Handler) exposedServer {
	mux := http.NewServeMux()
	for _, handlersMap := range handlersMaps {
		for pattern, handler := range handlersMap {
			mux.Handle(pattern, handler)
		}
	}
	return newHttpServer(newHTTPAccessController(cors, hostWhitelist, mux))
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
	host, _, err := net.SplitHostPort(req.Host)
	if err == nil {
		return host
	}
	return req.Host
}

func (ac *httpAccessController) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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

func newHttpServer(handler http.Handler) exposedServer {
	svr := &http.Server{
		Handler: handler,
	}
	return &httpServer{
		svr: svr,
	}
}

type httpServer struct {
	svr *http.Server
}

func (h *httpServer) name() string {
	return "http"
}

func (h *httpServer) matcher() cmux.Matcher {
	return cmux.HTTP1()
}

func (h *httpServer) serve(listener net.Listener) error {
	return h.svr.Serve(listener)
}

func (h *httpServer) close() error {
	return h.svr.Close()
}

func newGrpcServer(svr *grpc.Server) exposedServer {
	return &grpcServer{Server: svr}
}

type grpcServer struct {
	*grpc.Server
}

func (g *grpcServer) name() string {
	return "grpc"
}

func (g *grpcServer) matcher() cmux.Matcher {
	return cmux.HTTP2()
}

func (g *grpcServer) serve(listener net.Listener) error {
	return g.Serve(listener)
}

func (g *grpcServer) close() error {
	stopped := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		g.Stop()
		<-stopped
	}
	return nil
}

type rootServer struct {
	port     int
	services []exposedServer
}

func newRootServer(port int, ss ...exposedServer) *rootServer {
	return &rootServer{
		port:     port,
		services: ss,
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
	defer listener.Close()

	mux := cmux.New(listener)
	go func() {
		defer util.Recover()
		klog.InfoS("root server start to listen", "port", gs.port)
		muxErr := normalizeServeError(mux.Serve())
		if muxErr == nil {
			klog.InfoS("root server listener closed", "port", gs.port)
		} else {
			klog.ErrorS(muxErr, "root server shutdown cause by temporary network error", "port", gs.port)
		}
	}()

	return runServers(ctx, mux, gs.services)
}

// runServers run servers concurrently and shutdown all servers if anyone is error
func runServers(ctx context.Context, mux cmux.CMux, servers []exposedServer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	for _, server := range servers {

		// ! NOTICE: `mux.Match` is not thread-safe, so it should be called serially.
		lsn := mux.Match(server.matcher())

		runner := runSubServer(ctx, lsn, server)
		group.Go(func() (err error) {
			// if any sub exposedServer exit, call `cancel` to make other sub exposedServer exit
			defer cancel()
			return runner()
		})
	}

	// block until all sub exposedServer exit and return the first error if exist
	return group.Wait()
}

func runSubServer(ctx context.Context, lsn net.Listener, server exposedServer) func() error {
	return func() (err error) {

		closed := make(chan error, 1)
		defer func() {
			_ = server.close()
			_ = lsn.Close()
			// wait until closed
			<-closed
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

		// block until exposedServer stop or context done
		return waitFor(ctx, closed)
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
