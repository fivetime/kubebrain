package server

import (
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type credentialCloseCounter struct {
	net.Conn
	calls atomic.Int32
}

func (c *credentialCloseCounter) Close() error { c.calls.Add(1); return c.Conn.Close() }

func TestCredentialConnectionExpiresIdleAndBlockedReads(t *testing.T) {
	for _, mode := range []string{"idle", "blocked read", "already expired"} {
		t.Run(mode, func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			raw := &credentialCloseCounter{Conn: left}
			until := time.Now().Add(30 * time.Millisecond)
			if mode == "already expired" {
				until = time.Now().Add(-time.Second)
			}
			conn := boundCredentialConnection(raw, until)
			defer conn.Close()
			done := make(chan error, 1)
			if mode == "blocked read" {
				go func() { _, err := conn.Read(make([]byte, 1)); done <- err }()
			} else {
				go func() { _, err := right.Read(make([]byte, 1)); done <- err }()
			}
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("connection did not expire")
			}
			_, err := conn.Write([]byte("must not send"))
			require.ErrorIs(t, err, net.ErrClosed)
			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _ = conn.Close() }()
			}
			wg.Wait()
			require.Equal(t, int32(1), raw.calls.Load())
		})
	}
}

func TestCredentialConnectionEarlyCloseStopsTimer(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	raw := &credentialCloseCounter{Conn: left}
	conn := boundCredentialConnection(raw, time.Now().Add(time.Hour)).(*credentialLifetimeConn)
	require.NoError(t, conn.Close())
	require.False(t, conn.timer.Reset(time.Hour), "timer must have been stopped by Close")
	conn.timer.Stop()
	require.Equal(t, int32(1), raw.calls.Load())
}

func TestCredentialConnectionPreservesSyscallConn(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	require.NoError(t, err)
	server, err := listener.Accept()
	require.NoError(t, err)
	defer server.Close()
	conn := boundCredentialConnection(client, time.Now().Add(time.Hour))
	defer conn.Close()
	sc, ok := conn.(syscall.Conn)
	require.True(t, ok)
	raw, err := sc.SyscallConn()
	require.NoError(t, err)
	require.NoError(t, raw.Control(func(fd uintptr) { require.NotZero(t, fd) }))
}
