// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
)

type restoredSnapshotLocalTLS struct {
	server         transport.TLSInfo
	passwordClient transport.TLSInfo
}

// newRestoredSnapshotServerTLS creates a server-only identity for the isolated
// loopback restore. The source client key is never used to serve TLS: ordinary
// client certificates need neither serverAuth nor a DNS/IP SAN. The restored
// server still verifies the ORIGINAL client against its source CA, preserving
// its certificate principal without needing a production CA or server key.
// The caller owns restoreRoot and removes it, including this identity, on every
// restore outcome. No generated identity is used against the source cluster.
func newRestoredSnapshotServerTLS(restoreRoot, clientCAFile string) (*restoredSnapshotLocalTLS, error) {
	sourceCA, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read restored Snapshot source client CA: %w", err)
	}
	if !x509.NewCertPool().AppendCertsFromPEM(sourceCA) {
		return nil, errors.New("restored Snapshot source client CA contains no certificates")
	}
	dir, err := os.MkdirTemp(restoreRoot, ".loopback-tls-*")
	if err != nil {
		return nil, fmt.Errorf("create restored Snapshot TLS directory: %w", err)
	}
	server, err := writeRestoredSnapshotIdentity(dir, "server", x509.ExtKeyUsageServerAuth)
	if err != nil {
		return nil, err
	}
	// This client has no certificate username. Anonymous and password-based
	// authorization checks must never inherit the source administrator's CN.
	passwordClient, err := writeRestoredSnapshotIdentity(dir, "password-client", x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, err
	}
	clientCert, err := os.ReadFile(passwordClient.CertFile)
	if err != nil {
		return nil, fmt.Errorf("read restored Snapshot password client certificate: %w", err)
	}
	server.TrustedCAFile = filepath.Join(dir, "client-trust.pem")
	if err := os.WriteFile(server.TrustedCAFile, append(append(sourceCA, '\n'), clientCert...), 0600); err != nil {
		return nil, fmt.Errorf("write restored Snapshot client trust: %w", err)
	}
	server.ClientCertAuth = true
	server.ServerName = "localhost"
	passwordClient.TrustedCAFile = server.CertFile
	passwordClient.ServerName = server.ServerName
	return &restoredSnapshotLocalTLS{server: server, passwordClient: passwordClient}, nil
}

// Each self-signed leaf is pinned only within this restore. Neither identity
// is a CA, and neither private key can issue identities for the source cluster.
func writeRestoredSnapshotIdentity(dir, name string, usage x509.ExtKeyUsage) (transport.TLSInfo, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return transport.TLSInfo{}, fmt.Errorf("generate restored Snapshot %s key: %w", name, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return transport.TLSInfo{}, fmt.Errorf("generate restored Snapshot %s serial: %w", name, err)
	}
	serial.Add(serial, big.NewInt(1))
	now := time.Now()
	certificate := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	if usage == x509.ExtKeyUsageServerAuth {
		certificate.Subject.CommonName = "kubebrain-snapshot-restore"
		certificate.DNSNames = []string{"localhost"}
		certificate.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		return transport.TLSInfo{}, fmt.Errorf("create restored Snapshot %s certificate: %w", name, err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return transport.TLSInfo{}, fmt.Errorf("encode restored Snapshot %s key: %w", name, err)
	}
	info := transport.TLSInfo{
		CertFile: filepath.Join(dir, name+".crt"), KeyFile: filepath.Join(dir, name+".key"),
	}
	if err := os.WriteFile(info.CertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		return transport.TLSInfo{}, fmt.Errorf("write restored Snapshot %s certificate: %w", name, err)
	}
	if err := os.WriteFile(info.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		return transport.TLSInfo{}, fmt.Errorf("write restored Snapshot %s key: %w", name, err)
	}
	return info, nil
}

func (cfg restoredSnapshotConfig) clientTLSConfig() (*tls.Config, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if !cfg.tls.enabled() {
		return nil, nil
	}
	return (transport.TLSInfo{
		TrustedCAFile: cfg.localTLS.server.CertFile,
		CertFile:      cfg.tls.certFile, KeyFile: cfg.tls.keyFile,
		ServerName: cfg.localTLS.server.ServerName,
	}).ClientConfig()
}
