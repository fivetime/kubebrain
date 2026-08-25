// Command jwt-token-probe proves whether one exact etcd JWT is accepted by one endpoint.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const maxTokenBytes = 1 << 20

type options struct {
	endpoint, tokenFile, caFile, certFile, keyFile, serverName, probeKey string
	timeout                                                              time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.endpoint, "endpoint", "", "one exact etcd endpoint")
	flag.StringVar(&o.tokenFile, "token-file", "", "0600 regular file containing the exact JWT")
	flag.StringVar(&o.caFile, "cacert", "", "trusted PEM CA for a TLS endpoint")
	flag.StringVar(&o.certFile, "cert", "", "optional PEM client certificate")
	flag.StringVar(&o.keyFile, "key", "", "optional PEM client private key")
	flag.StringVar(&o.serverName, "server-name", "", "optional TLS server-name override")
	flag.StringVar(&o.probeKey, "probe-key", "/kubebrain/jwt-rotation/probe", "authorized key used for a serializable Range")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Second, "dial and RPC timeout")
	flag.Parse()
	if err := run(context.Background(), o); err != nil {
		fmt.Fprintln(os.Stderr, "JWT token probe:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, o options) error {
	if strings.TrimSpace(o.endpoint) == "" || strings.ContainsAny(o.endpoint, "\r\n\t ,") {
		return errors.New("--endpoint must be one non-empty endpoint without whitespace or commas")
	}
	if o.tokenFile == "" || o.probeKey == "" || o.timeout <= 0 || o.timeout > time.Minute {
		return errors.New("--token-file, non-empty --probe-key, and 0 < --timeout <= 1m are required")
	}
	token, err := readSecretFile(o.tokenFile, maxTokenBytes)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	token = []byte(strings.TrimSpace(string(token)))
	if len(token) == 0 || strings.ContainsAny(string(token), "\r\n\t ") {
		return errors.New("token must be non-empty and contain no whitespace")
	}
	tlsConfig, err := loadTLS(o)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{o.endpoint}, DialTimeout: o.timeout, Token: string(token), TLS: tlsConfig})
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer cli.Close()
	if _, err := cli.Get(ctx, o.probeKey, clientv3.WithSerializable()); err != nil {
		return fmt.Errorf("range %s: %w", o.endpoint, err)
	}
	return nil
}

func readSecretFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&0o077 != 0 {
		return nil, errors.New("file must be regular, non-symlink, and inaccessible to group/other")
	}
	if info.Size() <= 0 || info.Size() > limit {
		return nil, fmt.Errorf("file must contain 1..%d bytes", limit)
	}
	return os.ReadFile(path)
}

func loadTLS(o options) (*tls.Config, error) {
	if o.caFile == "" && o.certFile == "" && o.keyFile == "" && o.serverName == "" {
		return nil, nil
	}
	if o.caFile == "" || (o.certFile == "") != (o.keyFile == "") {
		return nil, errors.New("TLS requires --cacert and paired --cert/--key")
	}
	caPEM, err := readPublicFile(o.caFile, maxTokenBytes)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("CA file contains no valid certificate")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: o.serverName}
	if o.certFile != "" {
		certPEM, err := readPublicFile(o.certFile, maxTokenBytes)
		if err != nil {
			return nil, fmt.Errorf("read client certificate: %w", err)
		}
		keyPEM, err := readSecretFile(o.keyFile, maxTokenBytes)
		if err != nil {
			return nil, fmt.Errorf("read client key: %w", err)
		}
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func readPublicFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("file must be regular and non-symlink")
	}
	if info.Size() <= 0 || info.Size() > limit {
		return nil, fmt.Errorf("file must contain 1..%d bytes", limit)
	}
	return os.ReadFile(path)
}
