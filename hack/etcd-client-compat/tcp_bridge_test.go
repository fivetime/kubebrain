package compat

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type tcpBridge struct {
	listener net.Listener
	target   string

	blackholeMode atomic.Int32
	dropped       atomic.Int64
	droppedConns  atomic.Int64
	acceptedConns atomic.Int64
	closed        atomic.Bool
	mu            sync.Mutex
	conns         map[net.Conn]struct{}
	acceptWG      sync.WaitGroup
	connWG        sync.WaitGroup
}

func newTCPBridge(t *testing.T, target string) *tcpBridge {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bridge := &tcpBridge{
		listener: listener,
		target:   grpcTarget(target),
		conns:    make(map[net.Conn]struct{}),
	}
	bridge.acceptWG.Add(1)
	go bridge.accept()
	t.Cleanup(bridge.Close)
	return bridge
}

func grpcTarget(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
}

func (b *tcpBridge) Endpoint() string {
	return b.listener.Addr().String()
}

func (b *tcpBridge) Blackhole() {
	b.blackholeMode.Store(1)
}

func (b *tcpBridge) BlackholeResponses() {
	b.blackholeMode.Store(2)
}

func (b *tcpBridge) Unblackhole() {
	b.blackholeMode.Store(0)
	b.dropConnections()
}

func (b *tcpBridge) Resume() {
	b.blackholeMode.Store(0)
}

func (b *tcpBridge) DroppedBytes() int64 {
	return b.dropped.Load()
}

func (b *tcpBridge) DropConnections() {
	b.dropConnections()
}

func (b *tcpBridge) DroppedConnections() int64 {
	return b.droppedConns.Load()
}

func (b *tcpBridge) AcceptedConnections() int64 {
	return b.acceptedConns.Load()
}

func (b *tcpBridge) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	_ = b.listener.Close()
	b.acceptWG.Wait()
	b.dropConnections()
	b.connWG.Wait()
}

func (b *tcpBridge) accept() {
	defer b.acceptWG.Done()
	for {
		inbound, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.acceptedConns.Add(1)
		outbound, err := net.DialTimeout("tcp", b.target, 3*time.Second)
		if err != nil {
			_ = inbound.Close()
			continue
		}
		if b.closed.Load() {
			_ = inbound.Close()
			_ = outbound.Close()
			return
		}
		b.track(inbound)
		b.track(outbound)
		b.connWG.Add(1)
		go b.forwardPair(inbound, outbound)
	}
}

func (b *tcpBridge) forwardPair(inbound, outbound net.Conn) {
	defer b.connWG.Done()
	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		b.copy(outbound, inbound, false)
	}()
	go func() {
		defer copies.Done()
		b.copy(inbound, outbound, true)
	}()
	copies.Wait()
	_ = inbound.Close()
	_ = outbound.Close()
	b.untrack(inbound)
	b.untrack(outbound)
}

func (b *tcpBridge) copy(destination, source net.Conn, response bool) {
	buffer := make([]byte, 32*1024)
	for {
		read, err := source.Read(buffer)
		if read > 0 {
			mode := b.blackholeMode.Load()
			if mode == 1 || (mode == 2 && response) {
				b.dropped.Add(int64(read))
			} else {
				if _, writeErr := destination.Write(buffer[:read]); writeErr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (b *tcpBridge) track(connection net.Conn) {
	b.mu.Lock()
	b.conns[connection] = struct{}{}
	b.mu.Unlock()
}

func (b *tcpBridge) untrack(connection net.Conn) {
	b.mu.Lock()
	delete(b.conns, connection)
	b.mu.Unlock()
}

func (b *tcpBridge) dropConnections() {
	b.mu.Lock()
	connections := make([]net.Conn, 0, len(b.conns))
	for connection := range b.conns {
		connections = append(connections, connection)
	}
	b.mu.Unlock()
	b.droppedConns.Add(int64(len(connections)))
	for _, connection := range connections {
		_ = connection.Close()
	}
}
