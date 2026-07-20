package endpoint

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	metricspkg "github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

type interceptorOrderMetrics struct {
	unaryCodes  chan codes.Code
	streamCodes chan codes.Code
}

func (m *interceptorOrderMetrics) GetGrpcServerOption() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(func(
			ctx context.Context,
			req interface{},
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (interface{}, error) {
			resp, err := handler(ctx, req)
			m.unaryCodes <- status.Code(err)
			return resp, err
		}),
		grpc.StreamInterceptor(func(
			srv interface{},
			stream grpc.ServerStream,
			info *grpc.StreamServerInfo,
			handler grpc.StreamHandler,
		) error {
			err := handler(srv, stream)
			m.streamCodes <- status.Code(err)
			return err
		}),
	}
}

func (*interceptorOrderMetrics) GetHttpHandlers() map[string]http.Handler { return nil }
func (*interceptorOrderMetrics) EmitCounter(string, interface{}, ...metricspkg.T) error {
	return nil
}
func (*interceptorOrderMetrics) EmitGauge(string, interface{}, ...metricspkg.T) error {
	return nil
}
func (*interceptorOrderMetrics) EmitHistogram(string, interface{}, ...metricspkg.T) error {
	return nil
}

type rejectingInterceptorServer struct{}

func (rejectingInterceptorServer) RegisterClient(server *grpc.Server) {
	healthpb.RegisterHealthServer(server, health.NewServer())
}
func (rejectingInterceptorServer) RegisterPeer(server *grpc.Server) {
	healthpb.RegisterHealthServer(server, health.NewServer())
}
func (rejectingInterceptorServer) ClientServerOptions() []grpc.ServerOption {
	return rejectingServerOptions()
}
func (rejectingInterceptorServer) PeerServerOptions() []grpc.ServerOption {
	return rejectingServerOptions()
}
func (rejectingInterceptorServer) GetClientHttpHandlers() map[string]http.Handler { return nil }
func (rejectingInterceptorServer) GetPeerHttpHandlers() map[string]http.Handler   { return nil }
func (rejectingInterceptorServer) GetInfoHttpHandlers() map[string]http.Handler   { return nil }
func (rejectingInterceptorServer) Close() error                                   { return nil }

func rejectingServerOptions() []grpc.ServerOption {
	rejected := status.Error(codes.ResourceExhausted, "rejected before handler")
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(
			context.Context,
			interface{},
			*grpc.UnaryServerInfo,
			grpc.UnaryHandler,
		) (interface{}, error) {
			return nil, rejected
		}),
		grpc.ChainStreamInterceptor(func(
			interface{},
			grpc.ServerStream,
			*grpc.StreamServerInfo,
			grpc.StreamHandler,
		) error {
			return rejected
		}),
	}
}

func TestGRPCMetricsObserveCallsRejectedBeforeHandlers(t *testing.T) {
	for _, test := range []struct {
		name    string
		options func(*Endpoint) []grpc.ServerOption
	}{
		{name: "client", options: (*Endpoint).clientGrpcServerOptions},
		{name: "peer", options: (*Endpoint).peerGrpcServerOptions},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := &interceptorOrderMetrics{
				unaryCodes:  make(chan codes.Code, 1),
				streamCodes: make(chan codes.Code, 1),
			}
			endpoint := &Endpoint{
				metrics:       observed,
				server:        rejectingInterceptorServer{},
				config:        &Config{},
				tlsIdentities: &transportidentity.Registry{},
			}
			server := grpc.NewServer(test.options(endpoint)...)
			healthpb.RegisterHealthServer(server, health.NewServer())
			listener := bufconn.Listen(1024 * 1024)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
			})

			conn, err := grpc.NewClient(
				"passthrough:///bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return listener.Dial()
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			client := healthpb.NewHealthClient(conn)

			_, err = client.Check(t.Context(), &healthpb.HealthCheckRequest{})
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
			requireObservedGRPCCode(t, observed.unaryCodes, codes.ResourceExhausted)

			watch, err := client.Watch(t.Context(), &healthpb.HealthCheckRequest{})
			require.NoError(t, err)
			_, err = watch.Recv()
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
			requireObservedGRPCCode(t, observed.streamCodes, codes.ResourceExhausted)
		})
	}
}

func requireObservedGRPCCode(t *testing.T, observed <-chan codes.Code, want codes.Code) {
	t.Helper()
	select {
	case got := <-observed:
		require.Equal(t, want, got)
	case <-time.After(time.Second):
		t.Fatal("metrics interceptor did not observe the RPC rejected before its handler")
	}
}
