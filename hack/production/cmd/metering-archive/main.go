package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
)

const (
	maxPrometheusCABytes          = 1 << 20
	maxPrometheusBearerTokenBytes = 16 << 10
)

func main() {
	var prometheusURL, tokenFile, caFile, serverName string
	var instance, executor, objectStoreID, bucket, prefix, retentionMode string
	var slotDuration, finalizationDelay, maxStaleness, retentionDuration, timeout time.Duration
	flag.StringVar(&prometheusURL, "prometheus-url", "", "Prometheus base URL")
	flag.StringVar(&tokenFile, "prometheus-bearer-token-file", "", "optional Prometheus bearer token file")
	flag.StringVar(&caFile, "prometheus-ca-file", "", "optional PEM CA file for Prometheus TLS")
	flag.StringVar(&serverName, "prometheus-server-name", "", "optional Prometheus TLS server name")
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&executor, "archive-executor", "/usr/local/bin/kubebrain-logical-object", "immutable blob Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store account identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&prefix, "object-prefix", "metering-samples", "archive object key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "sample Object Lock retention from slot end")
	flag.DurationVar(&slotDuration, "slot-duration", time.Hour, "fixed metering sample slot")
	flag.DurationVar(&finalizationDelay, "finalization-delay", 10*time.Minute, "delay before querying a completed slot")
	flag.DurationVar(&maxStaleness, "max-staleness", 5*time.Minute, "maximum accepted recording-rule sample age")
	flag.DurationVar(&timeout, "timeout", 5*time.Minute, "overall query and archive deadline")
	flag.Parse()

	if prometheusURL == "" || instance == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("prometheus-url, instance, object-store-id, bucket, and a positive timeout are required")
	}
	client, err := prometheusClient(caFile, serverName)
	if err != nil {
		log.Fatal(err)
	}
	token, err := readToken(tokenFile)
	if err != nil {
		log.Fatal(err)
	}
	collector, err := meteringarchive.NewCollectorV3(prometheusURL, client, token, maxStaleness)
	if err != nil {
		log.Fatal(err)
	}
	archiver := &meteringarchive.Archiver{
		Collector: collector, Instance: instance, Executor: executor,
		ObjectStoreID: objectStoreID, Bucket: bucket, Prefix: prefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
		SlotDuration: slotDuration, FinalizationDelay: finalizationDelay, MaxStaleness: maxStaleness,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := archiver.Process(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(output); err != nil {
		log.Fatal(err)
	}
}

func prometheusClient(caFile, serverName string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		contents, err := readBoundedFile(caFile, "prometheus CA", maxPrometheusCABytes)
		if err != nil {
			return nil, fmt.Errorf("read prometheus CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(contents) {
			return nil, errors.New("prometheus CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}, nil
}

func readToken(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	contents, err := readBoundedFile(path, "prometheus bearer token", maxPrometheusBearerTokenBytes)
	if err != nil {
		return "", fmt.Errorf("read prometheus bearer token: %w", err)
	}
	token := strings.TrimRight(string(contents), "\r\n")
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("prometheus bearer token is empty or malformed")
	}
	return token, nil
}

func readBoundedFile(path, description string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, limit)
	}
	return contents, nil
}

func init() {
	log.SetFlags(0)
}
