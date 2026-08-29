// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	slowConsumerMetricName = "watcher_hub_slow_consumer_outcome"
	maximumMetricsBytes    = 32 << 20
)

type slowConsumerOutcomes struct {
	catchUp   float64
	recovered float64
	dropped   float64
}

type slowConsumerMetricsReader struct {
	endpoint  string
	client    *http.Client
	transport *http.Transport
}

func validateInfoEndpoint(text string) error {
	if text == "" {
		return nil
	}
	parsed, err := url.Parse(text)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/metrics" {
		return fmt.Errorf("INFO_ENDPOINT must be an absolute http(s) URL ending in /metrics without credentials, query, or fragment: %q", text)
	}
	return nil
}

func infoTLSConfig(cfg config) (*tls.Config, error) {
	caFile := cfg.infoCAFile
	if caFile == "" {
		caFile = cfg.caFile
	}
	serverName := cfg.infoTLSServerName
	if serverName == "" {
		serverName = cfg.tlsServerName
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read info CA file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("info CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if cfg.infoCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.infoCertFile, cfg.infoKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load info client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func newSlowConsumerMetricsReader(cfg config) (*slowConsumerMetricsReader, error) {
	tlsConfig, err := infoTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &slowConsumerMetricsReader{
		endpoint: cfg.infoEndpoint,
		client: &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("metrics endpoint redirects are not allowed")
		}},
		transport: transport,
	}, nil
}

func (reader *slowConsumerMetricsReader) close() {
	if reader != nil && reader.transport != nil {
		reader.transport.CloseIdleConnections()
	}
}

func (reader *slowConsumerMetricsReader) fetch(ctx context.Context) (slowConsumerOutcomes, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, reader.endpoint, nil)
	if err != nil {
		return slowConsumerOutcomes{}, err
	}
	response, err := reader.client.Do(request)
	if err != nil {
		return slowConsumerOutcomes{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return slowConsumerOutcomes{}, fmt.Errorf("metrics endpoint returned %s", response.Status)
	}
	limited := &io.LimitedReader{R: response.Body, N: maximumMetricsBytes + 1}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(limited)
	if err != nil {
		return slowConsumerOutcomes{}, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	if limited.N <= 0 {
		return slowConsumerOutcomes{}, fmt.Errorf("metrics response exceeds %d bytes", maximumMetricsBytes)
	}
	family := families[slowConsumerMetricName]
	if family == nil {
		family = families[slowConsumerMetricName+"_total"]
	}
	if family == nil {
		return slowConsumerOutcomes{}, fmt.Errorf("metrics response omits %s", slowConsumerMetricName)
	}
	values := map[string]float64{}
	for _, metric := range family.Metric {
		outcome := ""
		for _, label := range metric.Label {
			if label.GetName() == "outcome" {
				outcome = label.GetValue()
			}
		}
		if outcome == "" || metric.Counter == nil {
			continue
		}
		if _, duplicate := values[outcome]; duplicate {
			return slowConsumerOutcomes{}, fmt.Errorf("metrics response has duplicate outcome %q", outcome)
		}
		value := metric.Counter.GetValue()
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || math.Trunc(value) != value {
			return slowConsumerOutcomes{}, fmt.Errorf("metrics response has invalid outcome %q counter %v", outcome, value)
		}
		values[outcome] = value
	}
	for _, outcome := range []string{"catch_up", "recovered", "dropped"} {
		if _, exists := values[outcome]; !exists {
			return slowConsumerOutcomes{}, fmt.Errorf("metrics response omits slow-consumer outcome %q", outcome)
		}
	}
	return slowConsumerOutcomes{catchUp: values["catch_up"], recovered: values["recovered"], dropped: values["dropped"]}, nil
}

func waitForSlowConsumerEntered(ctx context.Context, reader *slowConsumerMetricsReader, baseline slowConsumerOutcomes) error {
	return waitForSlowConsumerOutcomes(ctx, reader, baseline, slowConsumerOutcomes{
		catchUp: baseline.catchUp + 1, recovered: baseline.recovered, dropped: baseline.dropped,
	}, "catch_up")
}

func waitForSlowConsumerRecovered(ctx context.Context, reader *slowConsumerMetricsReader, baseline slowConsumerOutcomes) error {
	return waitForSlowConsumerOutcomes(ctx, reader, baseline, slowConsumerOutcomes{
		catchUp: baseline.catchUp + 1, recovered: baseline.recovered + 1, dropped: baseline.dropped,
	}, "recovered")
}

func waitForSlowConsumerOutcomes(ctx context.Context, reader *slowConsumerMetricsReader, baseline, expected slowConsumerOutcomes, phase string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := reader.fetch(ctx)
		if err != nil {
			return fmt.Errorf("read slow-consumer outcomes while waiting for %s: %w", phase, err)
		}
		if current.catchUp < baseline.catchUp || current.recovered < baseline.recovered || current.dropped < baseline.dropped {
			return fmt.Errorf("slow-consumer counters regressed while waiting for %s: baseline=%+v current=%+v", phase, baseline, current)
		}
		if current.dropped != baseline.dropped {
			return fmt.Errorf("slow-consumer was dropped while waiting for %s: baseline=%+v current=%+v", phase, baseline, current)
		}
		if current == expected {
			return nil
		}
		if current.catchUp > expected.catchUp || current.recovered > expected.recovered {
			return fmt.Errorf("slow-consumer outcome counters changed by more than this owned watch while waiting for %s: baseline=%+v current=%+v expected=%+v",
				phase, baseline, current, expected)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for slow-consumer %s outcomes: %w; baseline=%+v last=%+v", phase, context.Cause(ctx), baseline, current)
		case <-ticker.C:
		}
	}
}
