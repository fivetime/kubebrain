package main

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
)

type rpcAttemptFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *rpcAttemptFakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *rpcAttemptFakeClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func TestRPCAttemptDiagnosticsCaptureRemoteAndPhases(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	clock := &rpcAttemptFakeClock{now: base}
	recorder := newRPCAttemptRecorder(8, clock.Now)
	ctx := recorder.TagRPC(context.Background(), &stats.RPCTagInfo{
		FullMethodName:      "/etcdserverpb.KV/Put",
		NameResolutionDelay: true,
	})
	recorder.HandleRPC(ctx, &stats.Begin{Client: true, BeginTime: base.Add(time.Millisecond)})
	clock.Set(base.Add(3 * time.Millisecond))
	recorder.HandleRPC(ctx, &stats.DelayedPickComplete{})
	clock.Set(base.Add(4 * time.Millisecond))
	recorder.HandleRPC(ctx, &stats.OutHeader{Client: true, RemoteAddr: &net.TCPAddr{IP: net.ParseIP("10.244.0.12"), Port: 3379}})
	clock.Set(base.Add(8 * time.Millisecond))
	recorder.HandleRPC(ctx, &stats.InHeader{Client: true})
	recorder.HandleRPC(ctx, &stats.InPayload{Client: true, Payload: &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{MemberId: 0x2a}}})
	recorder.HandleRPC(ctx, &stats.InTrailer{Client: true, Trailer: metadata.Pairs(
		proxyprotocol.CoreUnaryProxyRouteTrailer, "proxy",
		proxyprotocol.CoreUnaryProxyWaitMicrosTrailer, "17",
		proxyprotocol.CoreUnaryProxyForwardMicrosTrailer, "3900",
		proxyprotocol.CoreUnaryProxyDrainRetriesTrailer, "1",
	)})
	recorder.HandleRPC(ctx, &stats.End{Client: true, BeginTime: base.Add(time.Millisecond), EndTime: base.Add(13 * time.Millisecond)})

	require.JSONEq(t, `{
		"window_start_utc":"2023-11-14T22:13:20Z",
		"window_end_utc":"2023-11-14T22:13:20.02Z",
		"attempts":[{
			"method":"put",
			"remote":"10.244.0.12:3379",
			"serving_member_id":"2a",
			"name_resolution_delay":true,
			"transparent_retry":false,
			"operation_to_begin_us":1000,
			"begin_to_pick_us":2000,
			"begin_to_out_header_us":3000,
			"out_header_to_in_header_us":4000,
			"in_header_to_end_us":5000,
			"total_us":12000,
			"code":"OK",
			"proxy_route":"proxy",
			"proxy_wait_us":17,
			"proxy_forward_us":3900,
			"proxy_drain_retries":1
		}],
		"omitted":0,
		"ring_overwrites":0
	}`, recorder.formatEvidence(base, base.Add(20*time.Millisecond), 8))
}

func TestRPCAttemptDiagnosticsKeepRetryAttemptsAndBoundOutput(t *testing.T) {
	base := time.Unix(1_700_000_100, 0)
	clock := &rpcAttemptFakeClock{now: base}
	recorder := newRPCAttemptRecorder(8, clock.Now)
	ctx := recorder.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/etcdserverpb.KV/Range"})

	recorder.HandleRPC(ctx, &stats.Begin{Client: true, BeginTime: base.Add(time.Millisecond), IsTransparentRetryAttempt: true})
	clock.Set(base.Add(2 * time.Millisecond))
	recorder.HandleRPC(ctx, &stats.OutHeader{Client: true, RemoteAddr: &net.TCPAddr{IP: net.ParseIP("10.244.0.21"), Port: 3379}})
	recorder.HandleRPC(ctx, &stats.End{Client: true, BeginTime: base.Add(time.Millisecond), EndTime: base.Add(3 * time.Millisecond), Error: status.Error(codes.Unavailable, "connection closed")})

	recorder.HandleRPC(ctx, &stats.Begin{Client: true, BeginTime: base.Add(4 * time.Millisecond)})
	clock.Set(base.Add(5 * time.Millisecond))
	recorder.HandleRPC(ctx, &stats.OutHeader{Client: true, RemoteAddr: &net.TCPAddr{IP: net.ParseIP("10.244.0.22"), Port: 3379}})
	clock.Set(base.Add(6 * time.Millisecond))
	recorder.HandleRPC(ctx, &stats.InHeader{Client: true})
	recorder.HandleRPC(ctx, &stats.InPayload{Client: true, Payload: &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{MemberId: 0x2b}}})
	recorder.HandleRPC(ctx, &stats.End{Client: true, BeginTime: base.Add(4 * time.Millisecond), EndTime: base.Add(7 * time.Millisecond)})

	// Non-core RPCs must not consume the bounded evidence ring or expose their metadata.
	unrelated := recorder.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/etcdserverpb.Lease/LeaseGrant"})
	recorder.HandleRPC(unrelated, &stats.Begin{Client: true, BeginTime: base.Add(8 * time.Millisecond)})
	recorder.HandleRPC(unrelated, &stats.End{Client: true, BeginTime: base.Add(8 * time.Millisecond), EndTime: base.Add(9 * time.Millisecond)})

	require.JSONEq(t, `{
		"window_start_utc":"2023-11-14T22:15:00Z",
		"window_end_utc":"2023-11-14T22:15:00.01Z",
		"attempts":[{
			"method":"range",
			"remote":"10.244.0.22:3379",
			"serving_member_id":"2b",
			"name_resolution_delay":false,
			"transparent_retry":false,
			"operation_to_begin_us":4000,
			"begin_to_pick_us":null,
			"begin_to_out_header_us":1000,
			"out_header_to_in_header_us":1000,
			"in_header_to_end_us":1000,
			"total_us":3000,
			"code":"OK",
			"proxy_route":"unknown",
			"proxy_wait_us":null,
			"proxy_forward_us":null,
			"proxy_drain_retries":null
		}],
		"omitted":1,
		"ring_overwrites":0
	}`, recorder.formatEvidence(base, base.Add(10*time.Millisecond), 1))
}

func TestRPCAttemptDiagnosticsFailClosedOnInvalidRemoteAndTime(t *testing.T) {
	base := time.Unix(1_700_000_200, 0)
	clock := &rpcAttemptFakeClock{now: base.Add(-time.Second)}
	recorder := newRPCAttemptRecorder(1, clock.Now)
	ctx := recorder.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/etcdserverpb.KV/Put"})
	recorder.HandleRPC(ctx, &stats.Begin{Client: true, BeginTime: base})
	recorder.HandleRPC(ctx, &stats.OutHeader{Client: true, RemoteAddr: invalidDiagnosticAddress("forged\nremote")})
	recorder.HandleRPC(ctx, &stats.InTrailer{Client: true, Trailer: metadata.Pairs(
		proxyprotocol.CoreUnaryProxyRouteTrailer, "forged",
		proxyprotocol.CoreUnaryProxyWaitMicrosTrailer, "-1",
		proxyprotocol.CoreUnaryProxyForwardMicrosTrailer, "01",
		proxyprotocol.CoreUnaryProxyDrainRetriesTrailer, "not-a-number",
	)})
	recorder.HandleRPC(ctx, &stats.End{Client: true, BeginTime: base, EndTime: base.Add(-time.Millisecond), Error: context.DeadlineExceeded})

	require.JSONEq(t, `{
		"window_start_utc":"2023-11-14T22:16:40Z",
		"window_end_utc":"2023-11-14T22:16:41Z",
		"attempts":[{
			"method":"put",
			"remote":"unknown",
			"serving_member_id":"unknown",
			"name_resolution_delay":false,
			"transparent_retry":false,
			"operation_to_begin_us":0,
			"begin_to_pick_us":null,
			"begin_to_out_header_us":null,
			"out_header_to_in_header_us":null,
			"in_header_to_end_us":null,
			"total_us":0,
			"code":"DeadlineExceeded",
			"proxy_route":"unknown",
			"proxy_wait_us":null,
			"proxy_forward_us":null,
			"proxy_drain_retries":null
		}],
		"omitted":0,
		"ring_overwrites":0
	}`, recorder.formatEvidence(base, base.Add(time.Second), 1))
}

func TestRPCAttemptDiagnosticsReportRingOverwrite(t *testing.T) {
	base := time.Unix(1_700_000_300, 0)
	recorder := newRPCAttemptRecorder(1, func() time.Time { return base })
	for index := 0; index < 2; index++ {
		ctx := recorder.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/etcdserverpb.KV/Put"})
		begin := base.Add(time.Duration(index) * time.Millisecond)
		recorder.HandleRPC(ctx, &stats.Begin{Client: true, BeginTime: begin})
		recorder.HandleRPC(ctx, &stats.End{Client: true, BeginTime: begin, EndTime: begin.Add(time.Microsecond)})
	}
	var report rpcAttemptEvidenceReport
	require.NoError(t, json.Unmarshal([]byte(recorder.formatEvidence(base, base.Add(time.Second), 8)), &report))
	require.Len(t, report.Attempts, 1)
	require.Equal(t, 1, report.RingOverwrites)

	recorder.reset()
	require.JSONEq(t, `{"window_start_utc":"2023-11-14T22:18:20Z","window_end_utc":"2023-11-14T22:18:21Z","attempts":[],"omitted":0,"ring_overwrites":0}`,
		recorder.formatEvidence(base, base.Add(time.Second), 8))
}

func TestRPCAttemptDiagnosticsWindowSurvivesDelayedFormatting(t *testing.T) {
	start := time.Date(2026, 9, 13, 8, 0, 0, 123456789, time.FixedZone("UTC+8", 8*60*60))
	end := start.Add(8533710699 * time.Nanosecond)
	// Formatting may happen long after the operation, including after cleanup.
	recorder := newRPCAttemptRecorder(1, func() time.Time { return end.Add(time.Hour) })
	var report rpcAttemptEvidenceReport
	require.NoError(t, json.Unmarshal([]byte(recorder.formatEvidence(start, end, 0)), &report))
	require.Equal(t, "2026-09-13T00:00:00.123456789Z", report.WindowStartUTC)
	require.Equal(t, "2026-09-13T00:00:08.657167488Z", report.WindowEndUTC)
	require.Empty(t, report.Attempts)
}

type invalidDiagnosticAddress string

func (address invalidDiagnosticAddress) Network() string { return "tcp" }
func (address invalidDiagnosticAddress) String() string  { return string(address) }

type diagnosticKVServer struct {
	etcdserverpb.UnimplementedKVServer
}

func (diagnosticKVServer) Range(ctx context.Context, _ *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if err := grpc.SetTrailer(ctx, metadata.Pairs(
		proxyprotocol.CoreUnaryProxyRouteTrailer, "proxy",
		proxyprotocol.CoreUnaryProxyWaitMicrosTrailer, "23",
		proxyprotocol.CoreUnaryProxyForwardMicrosTrailer, "4567",
		proxyprotocol.CoreUnaryProxyDrainRetriesTrailer, "0",
	)); err != nil {
		return nil, err
	}
	return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: 1, MemberId: 0x2a, Revision: 1}}, nil
}

func TestRPCAttemptDiagnosticsObserveRealGRPCRemote(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, diagnosticKVServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	recorder := newRPCAttemptRecorder(8, time.Now)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + listener.Addr().String()},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithStatsHandler(recorder)},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err = client.Get(ctx, "diagnostic-key")
	cancel()
	require.NoError(t, err)
	ended := time.Now()

	var report rpcAttemptEvidenceReport
	require.NoError(t, json.Unmarshal([]byte(recorder.formatEvidence(started, ended, 8)), &report))
	require.Len(t, report.Attempts, 1)
	require.Equal(t, "range", report.Attempts[0].Method)
	require.Equal(t, listener.Addr().String(), report.Attempts[0].Remote)
	require.Equal(t, "2a", report.Attempts[0].ServingMemberID)
	require.Equal(t, "OK", report.Attempts[0].Code)
	require.NotNil(t, report.Attempts[0].BeginToOutHeaderMicros)
	require.NotNil(t, report.Attempts[0].OutHeaderToInHeaderMicros)
	require.Equal(t, "proxy", report.Attempts[0].ProxyRoute)
	require.Equal(t, int64(23), *report.Attempts[0].ProxyWaitMicros)
	require.Equal(t, int64(4567), *report.Attempts[0].ProxyForwardMicros)
	require.Equal(t, int64(0), *report.Attempts[0].ProxyDrainRetries)
}
