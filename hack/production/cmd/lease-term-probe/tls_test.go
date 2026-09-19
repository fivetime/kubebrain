package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type countingListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

type disconnectLease struct {
	*fixture
	stop func()
}

func (s *disconnectLease) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	s.streams.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	s.stop()
	return nil
}

func TestRunOverMutualTLS(t *testing.T) {
	for _, mode := range []string{"success", "wait-expiry", "wrong-server-name", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			caFile, certFile, keyFile, serverTLS := probeTestTLS(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			counted := &countingListener{Listener: listener}
			gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
			header := &pb.ResponseHeader{ClusterId: 11, MemberId: 22, Revision: 3, RaftTerm: 4}
			fixture := &fixture{header: header,
				ttl:      &pb.LeaseTimeToLiveResponse{Header: header, ID: 33, TTL: -2, GrantedTTL: 3, Keys: [][]byte{[]byte("owned")}},
				response: &pb.LeaseKeepAliveResponse{Header: header, ID: 33, TTL: 3}}
			if mode == "wait-expiry" {
				fixture.ttlSequence = []*pb.LeaseTimeToLiveResponse{
					{Header: header, ID: 33, TTL: 1, GrantedTTL: 3, Keys: [][]byte{[]byte("owned")}}, fixture.ttl,
				}
			}
			pb.RegisterMaintenanceServer(gs, fixture)
			if mode == "disconnect" {
				pb.RegisterLeaseServer(gs, &disconnectLease{fixture: fixture, stop: gs.Stop})
			} else {
				pb.RegisterLeaseServer(gs, fixture)
			}
			go func() { _ = gs.Serve(counted) }()
			defer counted.Close()
			defer gs.Stop()
			serverName := "probe.test"
			if mode == "wrong-server-name" {
				serverName = "wrong.test"
			}
			args := []string{"--endpoint=" + listener.Addr().String(), "--cacert=" + caFile,
				"--cert=" + certFile, "--key=" + keyFile, "--tls-server-name=" + serverName,
				"--cluster-id=11", "--member-id=22", "--lease-id=33", "--leased-key=owned", "--duration=3s"}
			var output bytes.Buffer
			if mode == "wait-expiry" {
				args = append(args, "--wait-for-expiry")
			}
			err = run(context.Background(), args, &output)
			if mode == "success" || mode == "wait-expiry" {
				require.NoError(t, err)
				require.Contains(t, output.String(), `"phase":"response"`)
			} else {
				require.Error(t, err)
				require.NotContains(t, output.String(), `"phase":"response"`)
			}
			require.Equal(t, int32(1), counted.accepted.Load(), "must never establish a replacement TCP connection")
			if mode == "wait-expiry" {
				require.Equal(t, int32(2), fixture.ttlReads.Load())
			}
			wantStreams := int32(1)
			if mode == "wrong-server-name" {
				wantStreams = 0
			}
			require.Equal(t, wantStreams, fixture.streams.Load())
		})
	}
}

func probeTestTLS(t *testing.T) (string, string, string, *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "probe test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "probe test"},
		DNSNames: []string{"probe.test"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	caFile, certFile, keyFile := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	for path, data := range map[string][]byte{caFile: caPEM, certFile: certPEM, keyFile: keyPEM} {
		require.NoError(t, os.WriteFile(path, data, 0600))
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))
	return caFile, certFile, keyFile, &tls.Config{MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
}
