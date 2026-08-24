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
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

type secureExposedServer struct {
	exposedServer
}

func (s *secureExposedServer) name() string {
	return fmt.Sprintf("secure %v", s.exposedServer.name())
}

func (s *secureExposedServer) isQuiescing() bool {
	quiesced, ok := s.exposedServer.(interface{ isQuiescing() bool })
	return ok && quiesced.isQuiescing()
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

func (t *secureServer) isQuiescing() bool {
	for _, server := range t.internalServers {
		if quiesced, ok := server.(interface{ isQuiescing() bool }); ok && quiesced.isQuiescing() {
			return true
		}
	}
	return false
}

// initialReadTimeout bounds the root cmux classification that runs before this
// server's TLS listener. Without it, a client that sends no ClientHello bytes
// never reaches identityTLSListener's handshake deadline and can remain open
// forever. In dual secure/insecure mode a zero-byte connection is inherently
// unclassifiable, so the secure admission bound applies to the shared socket.
func (t *secureServer) initialReadTimeout() time.Duration {
	return tlsIdentityHandshakeTimeout
}

func (t *secureServer) matchWriters() []cmux.MatchWriter {
	return matchersToMatchWriters(cmux.TLS())
}

func (t *secureServer) serve(listener net.Listener) (err error) {

	tlsConf := t.conf.getServerTLSConfig()
	tlsListener := &identityTLSListener{
		Listener: listener, config: tlsConf, identities: t.identities,
		handshakeTimeout: tlsIdentityHandshakeTimeout,
	}
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

	return serveMuxAndServers(ctx, tlsListener, mux, newSecureExposedServers(t.internalServers), false)
}

type identityTLSListener struct {
	net.Listener
	config     *tls.Config
	identities *transportidentity.Registry
	// handshakeTimeout is injectable only for deterministic socket tests. The
	// production secure-server constructor always sets tlsIdentityHandshakeTimeout.
	handshakeTimeout time.Duration
	startOnce        sync.Once
	closeOnce        sync.Once
	closeErr         error
	done             chan struct{}
	accepted         chan identityTLSAcceptResult
	activeMu         sync.Mutex
	active           map[net.Conn]struct{}
	workerWG         sync.WaitGroup
}

const (
	tlsIdentityHandshakeTimeout = 10 * time.Second
	maxConcurrentTLSHandshakes  = 256
)

type identityTLSAcceptResult struct {
	conn net.Conn
	err  error
}

func (l *identityTLSListener) start() {
	l.startOnce.Do(func() {
		l.done = make(chan struct{})
		l.accepted = make(chan identityTLSAcceptResult)
		l.active = make(map[net.Conn]struct{})
		l.workerWG.Add(1)
		go func() {
			defer l.workerWG.Done()
			l.acceptLoop()
		}()
	})
}

func (l *identityTLSListener) acceptLoop() {
	for {
		raw, err := l.Listener.Accept()
		if err != nil {
			select {
			case l.accepted <- identityTLSAcceptResult{err: err}:
			case <-l.done:
			}
			return
		}
		l.activeMu.Lock()
		select {
		case <-l.done:
			l.activeMu.Unlock()
			_ = raw.Close()
			return
		default:
		}
		if len(l.active) >= maxConcurrentTLSHandshakes {
			l.activeMu.Unlock()
			_ = raw.Close()
			klog.V(2).InfoS("rejected TLS connection: handshake limit reached", "remoteAddr", raw.RemoteAddr())
			continue
		}
		l.active[raw] = struct{}{}
		l.workerWG.Add(1)
		l.activeMu.Unlock()
		go func(conn net.Conn) {
			defer l.workerWG.Done()
			l.handshake(conn)
		}(raw)
	}
}

func (l *identityTLSListener) handshake(raw net.Conn) {
	defer func() {
		l.activeMu.Lock()
		delete(l.active, raw)
		l.activeMu.Unlock()
	}()
	conn := tls.Server(raw, l.config)
	timeout := l.handshakeTimeout
	if timeout <= 0 {
		timeout = tlsIdentityHandshakeTimeout
	}
	err := raw.SetDeadline(time.Now().Add(timeout))
	if err == nil {
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
		return
	}
	unregister := l.identities.Register(conn.LocalAddr(), conn.RemoteAddr(), conn.ConnectionState())
	wrapped := &identityTLSConn{Conn: conn, unregister: unregister}
	select {
	case l.accepted <- identityTLSAcceptResult{conn: wrapped}:
	case <-l.done:
		_ = wrapped.Close()
	}
}

func (l *identityTLSListener) Accept() (net.Conn, error) {
	l.start()
	select {
	case <-l.done:
		return nil, net.ErrClosed
	case result := <-l.accepted:
		if result.conn != nil {
			select {
			case <-l.done:
				_ = result.conn.Close()
				return nil, net.ErrClosed
			default:
			}
		}
		return result.conn, result.err
	}
}

func (l *identityTLSListener) Close() error {
	l.start()
	l.closeOnce.Do(func() {
		close(l.done)
		l.closeErr = normalizeServeError(l.Listener.Close())
		l.activeMu.Lock()
		for raw := range l.active {
			l.closeErr = errors.Join(l.closeErr, normalizeServeError(raw.Close()))
		}
		l.activeMu.Unlock()
		l.workerWG.Wait()
	})
	return l.closeErr
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
