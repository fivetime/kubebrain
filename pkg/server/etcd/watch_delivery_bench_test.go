package etcd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Local software-path baseline only: memkv, one leader, no proxy and
// no pacing. None of these transports represents cross-node TiKV/PD deployment cost.
// Put and post-Put Watch durations partition this serial client observation;
// they are not pure server RPC or network times. No performance pass threshold.
func BenchmarkClientPutWatchDelivery(b *testing.B) {
	benchmarkClientPutWatchDelivery(b, 0)
}

// Match the deployment probe's after-operation idle interval. Standard ns/op
// includes the idle time; put-ns/op and post-put-watch-ns/op exclude it. Use a
// bounded iteration count (for example -benchtime=30x) for this slower variant.
func BenchmarkClientPutWatchDeliveryAfterIdle(b *testing.B) {
	benchmarkClientPutWatchDelivery(b, 100*time.Millisecond)
}

func benchmarkClientPutWatchDelivery(b *testing.B, afterOpIdle time.Duration) {
	b.Helper()
	for _, transport := range []string{"bufconn", "tcp", "tcp-tls"} {
		b.Run(transport, func(b *testing.B) {
			server, closeServer := newTestRPCServer(b)
			b.Cleanup(closeServer)
			serverOptions := server.ClientServerOptions()
			var clientTLS *tls.Config
			if transport == "tcp-tls" {
				serverTLS, trust := watchDeliveryBenchmarkTLS(b)
				clientTLS = trust
				serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(serverTLS)))
			}
			g := grpc.NewServer(serverOptions...)
			etcdserverpb.RegisterKVServer(g, server)
			etcdserverpb.RegisterWatchServer(g, server)
			options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			if clientTLS != nil {
				options = nil // clientv3 installs credentials from Config.TLS.
			}
			var listener net.Listener
			endpoint := "bufnet"
			if transport == "bufconn" {
				buffer := bufconn.Listen(1 << 20)
				listener = buffer
				options = append(options, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return buffer.DialContext(ctx)
				}))
			} else {
				var err error
				listener, err = net.Listen("tcp", "127.0.0.1:0")
				require.NoError(b, err)
				endpoint = listener.Addr().String()
			}
			done := make(chan struct{})
			go func() { defer close(done); _ = g.Serve(listener) }()
			b.Cleanup(func() { g.Stop(); _ = listener.Close(); <-done })
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second, DialOptions: options, TLS: clientTLS})
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, client.Close()) })
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()
			const key = "/bench/watch-delivery"
			watch := client.Watch(ctx, key, clientv3.WithCreatedNotify(), clientv3.WithPrevKV())
			startup, stopStartup := context.WithTimeout(ctx, 5*time.Second)
			select {
			case response, ok := <-watch:
				require.True(b, ok)
				require.NoError(b, response.Err())
				require.True(b, response.Created)
			case <-startup.Done():
				b.Fatal(startup.Err())
			}
			stopStartup()
			var putTotal, watchTotal time.Duration
			var previousRevision int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				opCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				value := strconv.Itoa(i)
				start := time.Now()
				put, err := client.Put(opCtx, key, value)
				putDone := time.Now()
				if err != nil {
					stop()
					b.Fatal(err)
				}
				select {
				case response, ok := <-watch:
					watchDone := time.Now()
					stop()
					require.True(b, ok)
					require.NoError(b, response.Err())
					require.Len(b, response.Events, 1)
					event := response.Events[0]
					require.NotNil(b, put.Header)
					require.Equal(b, put.Header.Revision, event.Kv.ModRevision)
					require.Greater(b, event.Kv.ModRevision, previousRevision)
					require.Equal(b, value, string(event.Kv.Value))
					if i == 0 {
						require.Nil(b, event.PrevKv)
					} else {
						require.NotNil(b, event.PrevKv)
						require.Equal(b, previousRevision, event.PrevKv.ModRevision)
						require.Equal(b, strconv.Itoa(i-1), string(event.PrevKv.Value))
					}
					previousRevision = event.Kv.ModRevision
					putTotal += putDone.Sub(start)
					watchTotal += watchDone.Sub(putDone)
				case <-opCtx.Done():
					stop()
					b.Fatal(opCtx.Err())
				}
				if afterOpIdle > 0 {
					timer := time.NewTimer(afterOpIdle)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						b.Fatal(ctx.Err())
					}
				}
			}
			b.StopTimer()
			if b.N > 0 {
				b.ReportMetric(float64(putTotal.Nanoseconds())/float64(b.N), "put-ns/op")
				b.ReportMetric(float64(watchTotal.Nanoseconds())/float64(b.N), "post-put-watch-ns/op")
			}
		})
	}
}

// Ephemeral loopback trust only; certificate creation and the initial Watch
// handshake are outside the timed region. This is server TLS, not mutual TLS.
func watchDeliveryBenchmarkTLS(b *testing.B) (*tls.Config, *tls.Config) {
	b.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(b, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:        true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(b, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(b, err)
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"}
}
