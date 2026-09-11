package main

import (
	"context"
	"encoding/json"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Only transport facts are retained. Never inspect requests, responses, metadata,
// credentials or error messages: all can contain application data or tokens.
type restoredAuthRPCEvent struct {
	Method string `json:"method"`
	Phase  string `json:"phase"`
	Peer   string `json:"peer,omitempty"`
	Code   string `json:"code"`
}

type restoredAuthRPCTrace struct {
	mu     sync.Mutex
	auth   *restoredAuthRPCEvent
	recent []restoredAuthRPCEvent
}

func (r *restoredAuthRPCTrace) record(method, phase string, p *peer.Peer, err error) {
	event := restoredAuthRPCEvent{Method: method, Phase: phase, Code: status.Code(err).String()}
	if p != nil && p.Addr != nil {
		event.Peer = p.Addr.String()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if method == "/etcdserverpb.Auth/Authenticate" {
		r.auth = &event
	}
	const limit = 16
	if len(r.recent) == limit {
		copy(r.recent, r.recent[1:])
		r.recent = r.recent[:limit-1]
	}
	r.recent = append(r.recent, event)
}

func (r *restoredAuthRPCTrace) summary() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, _ := json.Marshal(struct {
		Auth   *restoredAuthRPCEvent  `json:"last_auth,omitempty"`
		Recent []restoredAuthRPCEvent `json:"recent"`
	}{r.auth, r.recent})
	return string(data)
}

func (r *restoredAuthRPCTrace) config(cfg clientv3.Config) clientv3.Config {
	// Config copies must not append into another user's shared DialOptions array.
	cfg.DialOptions = append([]grpc.DialOption(nil), cfg.DialOptions...)
	cfg.DialOptions = append(cfg.DialOptions,
		grpc.WithChainUnaryInterceptor(r.unary), grpc.WithChainStreamInterceptor(r.stream))
	return cfg
}

func (r *restoredAuthRPCTrace) unary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
	invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var p peer.Peer
	err := invoke(ctx, method, req, reply, cc, append(opts, grpc.Peer(&p))...)
	r.record(method, "unary", &p, err)
	return err
}

func (r *restoredAuthRPCTrace) stream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, open grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := open(ctx, desc, cc, method, opts...)
	if err != nil {
		r.record(method, "open", nil, err)
		return stream, err
	}
	return &restoredAuthTracedStream{ClientStream: stream, trace: r, method: method}, nil
}

type restoredAuthTracedStream struct {
	grpc.ClientStream
	trace  *restoredAuthRPCTrace
	method string
}

func (s *restoredAuthTracedStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	p, _ := peer.FromContext(s.ClientStream.Context())
	s.trace.record(s.method, "recv", p, err)
	return err
}
