package etcdutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultTimeout    = 10 * time.Minute
	maxEnvTLSPEMBytes = 1 << 20
)

func EnvFirst(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func TLSConfigFromEnv() (*tls.Config, error) {
	caFile := EnvFirst("CACERT", "ETCD_CACERT", "ETCDCTL_CACERT")
	certFile := EnvFirst("CERT", "ETCD_CERT", "ETCDCTL_CERT")
	keyFile := EnvFirst("KEY", "ETCD_KEY", "ETCDCTL_KEY")
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("client TLS requires both CERT/ETCDCTL_CERT and KEY/ETCDCTL_KEY")
	}

	cert, err := loadEnvKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		caPEM, err := readEnvTLSFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA file %s", caFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func loadEnvKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	certPEM, err := readEnvTLSFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM, err := readEnvTLSFile(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

func readEnvTLSFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEnvTLSPEMBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxEnvTLSPEMBytes {
		return nil, fmt.Errorf("TLS PEM file %s exceeds %d bytes", path, maxEnvTLSPEMBytes)
	}
	return data, nil
}

func NewClientFromEnv() (*clientv3.Client, error) {
	endpoint := os.Getenv("ENDPOINT")
	tlsConfig, err := TLSConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 10 * time.Second,
		TLS:         tlsConfig,
	})
}

func TimeoutFromEnv() (time.Duration, error) {
	value := os.Getenv("TIMEOUT")
	if value == "" {
		return defaultTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("invalid TIMEOUT %q", value)
	}
	return timeout, nil
}
