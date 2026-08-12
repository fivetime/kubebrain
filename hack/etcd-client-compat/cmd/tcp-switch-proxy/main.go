package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type proxy struct {
	listener           net.Listener
	mode               atomic.Int32
	dropped            atomic.Int64
	droppedConnections atomic.Int64

	mu      sync.Mutex
	target  string
	dials   map[string]int
	conns   map[net.Conn]struct{}
	closed  bool
	accept  sync.WaitGroup
	workers sync.WaitGroup
}

type response struct {
	OK                 bool           `json:"ok"`
	Error              string         `json:"error,omitempty"`
	Endpoint           string         `json:"endpoint,omitempty"`
	Dropped            int64          `json:"droppedBytes,omitempty"`
	DroppedConnections int64          `json:"droppedConnections,omitempty"`
	Dials              map[string]int `json:"dials,omitempty"`
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "TCP listen address")
	target := flag.String("target", "", "initial backend host:port")
	flag.Parse()
	if strings.TrimSpace(*target) == "" {
		fmt.Fprintln(os.Stderr, "--target is required")
		os.Exit(2)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	p := &proxy{
		listener: listener,
		target:   strings.TrimSpace(*target),
		dials:    make(map[string]int),
		conns:    make(map[net.Conn]struct{}),
	}
	p.accept.Add(1)
	go p.acceptLoop()
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(response{OK: true, Endpoint: listener.Addr().String()})
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(line)
		var reply response
		switch {
		case line == "blackhole-responses":
			p.mode.Store(1)
			reply.OK = true
		case line == "resume":
			p.mode.Store(0)
			reply.OK = true
		case line == "drop-connections":
			p.dropConnections()
			reply.OK = true
		case line == "stats":
			reply = p.stats()
		case len(parts) == 2 && parts[0] == "target":
			p.setTarget(parts[1])
			reply.OK = true
		default:
			reply.Error = "unknown command"
		}
		if err := encoder.Encode(reply); err != nil {
			break
		}
	}
	p.close()
}

func (p *proxy) acceptLoop() {
	defer p.accept.Done()
	for {
		inbound, err := p.listener.Accept()
		if err != nil {
			return
		}
		target, open := p.currentTarget()
		if !open {
			_ = inbound.Close()
			return
		}
		outbound, err := net.DialTimeout("tcp", target, 3*time.Second)
		if err != nil {
			_ = inbound.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = inbound.Close()
			_ = outbound.Close()
			return
		}
		p.dials[target]++
		p.conns[inbound] = struct{}{}
		p.conns[outbound] = struct{}{}
		p.mu.Unlock()
		p.workers.Add(1)
		go p.forward(inbound, outbound)
	}
}

func (p *proxy) forward(inbound, outbound net.Conn) {
	defer p.workers.Done()
	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		p.copy(outbound, inbound, false)
	}()
	go func() {
		defer copies.Done()
		p.copy(inbound, outbound, true)
	}()
	copies.Wait()
	_ = inbound.Close()
	_ = outbound.Close()
	p.mu.Lock()
	delete(p.conns, inbound)
	delete(p.conns, outbound)
	p.mu.Unlock()
}

func (p *proxy) copy(destination, source net.Conn, responseDirection bool) {
	buffer := make([]byte, 32*1024)
	for {
		read, err := source.Read(buffer)
		if read > 0 {
			if responseDirection && p.mode.Load() == 1 {
				p.dropped.Add(int64(read))
			} else if _, writeErr := destination.Write(buffer[:read]); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *proxy) currentTarget() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.target, !p.closed
}

func (p *proxy) setTarget(target string) {
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
}

func (p *proxy) dropConnections() {
	p.mu.Lock()
	connections := make([]net.Conn, 0, len(p.conns))
	for connection := range p.conns {
		connections = append(connections, connection)
	}
	p.mu.Unlock()
	p.droppedConnections.Add(int64(len(connections)))
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (p *proxy) stats() response {
	p.mu.Lock()
	dials := make(map[string]int, len(p.dials))
	for target, count := range p.dials {
		dials[target] = count
	}
	p.mu.Unlock()
	return response{
		OK:                 true,
		Dropped:            p.dropped.Load(),
		DroppedConnections: p.droppedConnections.Load(),
		Dials:              dials,
	}
}

func (p *proxy) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	_ = p.listener.Close()
	p.accept.Wait()
	p.dropConnections()
	p.workers.Wait()
}
