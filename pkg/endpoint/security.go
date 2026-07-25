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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

type secureExposedServer struct {
	exposedServer
}

func (s *secureExposedServer) name() string {
	return fmt.Sprintf("secure %v", s.exposedServer.name())
}

func newSecureExposedServers(ss []exposedServer) []exposedServer {
	ret := make([]exposedServer, 0, len(ss))
	for _, s := range ss {
		ret = append(ret, &secureExposedServer{exposedServer: s})
	}
	return ret
}

type secureServer struct {
	internalServers []exposedServer
	conf            *SecurityConfig
	identities      *transportidentity.Registry
}

func newSecureServer(conf *SecurityConfig, identities *transportidentity.Registry, ss ...exposedServer) exposedServer {
	return &secureServer{
		conf:            conf,
		identities:      identities,
		internalServers: ss,
	}
}

func (t *secureServer) name() string {
	return "tls"
}

func (t *secureServer) matchWriters() []cmux.MatchWriter {
	return matchersToMatchWriters(cmux.TLS())
}

func (t *secureServer) serve(listener net.Listener) (err error) {

	tlsConf := t.conf.getServerTLSConfig()
	tlsListener := &identityTLSListener{Listener: listener, config: tlsConf, identities: t.identities}
	mux := cmux.New(tlsListener)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		if err != nil {
			klog.ErrorS(err, "tls server shutdown")
		} else {
			klog.Info("tls server shutdown")
		}
	}()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer cancel()
		return normalizeServeError(mux.Serve())
	})

	group.Go(func() error {
		defer cancel()
		return runServers(ctx, mux, newSecureExposedServers(t.internalServers))
	})

	return group.Wait()
}

type identityTLSListener struct {
	net.Listener
	config     *tls.Config
	identities *transportidentity.Registry
}

const tlsIdentityHandshakeTimeout = 10 * time.Second

func (l *identityTLSListener) Accept() (net.Conn, error) {
	for {
		raw, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		conn := tls.Server(raw, l.config)
		if err = raw.SetDeadline(time.Now().Add(tlsIdentityHandshakeTimeout)); err == nil {
			err = conn.Handshake()
		}
		if err == nil {
			err = raw.SetDeadline(time.Time{})
		}
		if err != nil {
			// TLS authentication and malformed handshakes are connection-local.
			// Returning them as listener errors makes cmux stop the entire secure
			// endpoint, allowing one untrusted client to cause an outage.
			_ = raw.Close()
			klog.V(2).InfoS("rejected TLS connection", "err", err, "remoteAddr", raw.RemoteAddr())
			continue
		}
		unregister := l.identities.Register(conn.LocalAddr(), conn.RemoteAddr(), conn.ConnectionState())
		return &identityTLSConn{Conn: conn, unregister: unregister}, nil
	}
}

type identityTLSConn struct {
	net.Conn
	once       sync.Once
	unregister func()
}

func (c *identityTLSConn) Close() error {
	c.once.Do(c.unregister)
	return c.Conn.Close()
}

func (t *secureServer) close() error {
	var result error
	for _, server := range t.internalServers {
		err := normalizeServeError(server.close())
		if err != nil {
			klog.ErrorS(err, "tls internal server close err", "server", server.name())
			result = errors.Join(result, err)
		}
	}
	return result
}
