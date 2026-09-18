package server

import (
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// This experimental bound forces a fresh handshake even for an idle connection.
// It is not an availability SLO or an election/lease duration.
const peerCredentialMaxConnectionAge = 30 * time.Second

type credentialLifetimeConn struct {
	net.Conn
	until  time.Time
	closed atomic.Bool
	once   sync.Once
	mu     sync.Mutex
	timer  *time.Timer
	err    error
}

type credentialLifetimeSyscallConn struct {
	*credentialLifetimeConn
	syscall.Conn
}

func boundCredentialConnection(conn net.Conn, until time.Time) net.Conn {
	c := &credentialLifetimeConn{Conn: conn, until: until}
	c.mu.Lock()
	c.timer = time.AfterFunc(time.Until(until), func() { _ = c.Close() })
	c.mu.Unlock()
	if raw, ok := conn.(syscall.Conn); ok {
		return &credentialLifetimeSyscallConn{credentialLifetimeConn: c, Conn: raw}
	}
	return c
}

func (c *credentialLifetimeConn) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		c.timer.Stop()
		c.mu.Unlock()
		c.err = c.Conn.Close()
	})
	return c.err
}

func (c *credentialLifetimeConn) expired() bool {
	if c.closed.Load() {
		return true
	}
	if !time.Now().Before(c.until) {
		_ = c.Close()
		return true
	}
	return false
}

func (c *credentialLifetimeConn) Read(p []byte) (int, error) {
	if c.expired() {
		return 0, net.ErrClosed
	}
	return c.Conn.Read(p)
}

func (c *credentialLifetimeConn) Write(p []byte) (int, error) {
	if c.expired() {
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}
