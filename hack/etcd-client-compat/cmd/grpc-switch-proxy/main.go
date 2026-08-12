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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

type rawFrame []byte

type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(value any) ([]byte, error) {
	frame, ok := value.(*rawFrame)
	if !ok {
		return nil, fmt.Errorf("raw codec cannot marshal %T", value)
	}
	return *frame, nil
}

func (rawCodec) Unmarshal(data []byte, value any) error {
	frame, ok := value.(*rawFrame)
	if !ok {
		return fmt.Errorf("raw codec cannot unmarshal %T", value)
	}
	*frame = append((*frame)[:0], data...)
	return nil
}

type proxy struct {
	listener net.Listener
	server   *grpc.Server

	mu               sync.Mutex
	target           string
	dials            map[string]int
	blocked          bool
	release          chan struct{}
	droppedResponses int
}

type response struct {
	OK               bool           `json:"ok"`
	Error            string         `json:"error,omitempty"`
	Endpoint         string         `json:"endpoint,omitempty"`
	DroppedResponses int            `json:"droppedResponses,omitempty"`
	Dials            map[string]int `json:"dials,omitempty"`
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "gRPC listen address")
	target := flag.String("target", "", "initial upstream host:port")
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
	p := &proxy{listener: listener, target: strings.TrimSpace(*target), dials: make(map[string]int)}
	p.server = grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(p.handleUnary),
	)
	go func() {
		if serveErr := p.server.Serve(listener); serveErr != nil {
			fmt.Fprintln(os.Stderr, serveErr)
		}
	}()
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(response{OK: true, Endpoint: listener.Addr().String()})
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(line)
		var reply response
		switch {
		case line == "block-responses":
			reply.OK = p.blockResponses()
			if !reply.OK {
				reply.Error = "responses are already blocked"
			}
		case line == "fail-blocked-responses":
			reply.OK = p.failBlockedResponses()
			if !reply.OK {
				reply.Error = "responses are not blocked"
			}
		case line == "stats":
			reply = p.stats()
		case len(parts) == 2 && parts[0] == "target":
			p.setTarget(parts[1])
			reply.OK = true
		default:
			reply.Error = "unknown command"
		}
		if encodeErr := encoder.Encode(reply); encodeErr != nil {
			break
		}
	}
	p.failBlockedResponses()
	p.server.Stop()
	_ = listener.Close()
}

func (p *proxy) handleUnary(_ any, stream grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "missing gRPC method")
	}
	var request rawFrame
	if err := stream.RecvMsg(&request); err != nil {
		return err
	}
	target := p.currentTarget()
	connection, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	defer connection.Close()
	p.recordDial(target)
	var upstreamResponse rawFrame
	if err := connection.Invoke(stream.Context(), method, &request, &upstreamResponse); err != nil {
		return err
	}
	if release := p.blockResponse(); release != nil {
		select {
		case <-release:
			return status.Error(codes.Unavailable, "external L7 proxy discarded committed response")
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	return stream.SendMsg(&upstreamResponse)
}

func (p *proxy) currentTarget() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.target
}

func (p *proxy) setTarget(target string) {
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
}

func (p *proxy) recordDial(target string) {
	p.mu.Lock()
	p.dials[target]++
	p.mu.Unlock()
}

func (p *proxy) blockResponses() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.blocked {
		return false
	}
	p.blocked = true
	p.release = make(chan struct{})
	return true
}

func (p *proxy) blockResponse() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.blocked {
		return nil
	}
	p.droppedResponses++
	return p.release
}

func (p *proxy) failBlockedResponses() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.blocked {
		return false
	}
	p.blocked = false
	close(p.release)
	p.release = nil
	return true
}

func (p *proxy) stats() response {
	p.mu.Lock()
	defer p.mu.Unlock()
	dials := make(map[string]int, len(p.dials))
	for target, count := range p.dials {
		dials[target] = count
	}
	return response{OK: true, DroppedResponses: p.droppedResponses, Dials: dials}
}

var _ encoding.Codec = rawCodec{}
