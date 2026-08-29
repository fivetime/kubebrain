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

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	slowConsumerMetricName    = "watcher_hub_slow_consumer_outcome"
	watchGenerationMetricName = "watch_generation_recovery"
	maximumMetricsBytes       = 32 << 20
)

type slowConsumerOutcomes struct {
	catchUp   float64
	recovered float64
	dropped   float64
}

type watchGenerationOutcomes struct {
	retry     float64
	recovered float64
	compacted float64
	failed    float64
}

type watchOutcomes struct {
	slow       slowConsumerOutcomes
	generation watchGenerationOutcomes
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

func (reader *slowConsumerMetricsReader) fetch(ctx context.Context) (watchOutcomes, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, reader.endpoint, nil)
	if err != nil {
		return watchOutcomes{}, err
	}
	response, err := reader.client.Do(request)
	if err != nil {
		return watchOutcomes{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return watchOutcomes{}, fmt.Errorf("metrics endpoint returned %s", response.Status)
	}
	limited := &io.LimitedReader{R: response.Body, N: maximumMetricsBytes + 1}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(limited)
	if err != nil {
		return watchOutcomes{}, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	if limited.N <= 0 {
		return watchOutcomes{}, fmt.Errorf("metrics response exceeds %d bytes", maximumMetricsBytes)
	}
	slow, err := parseFixedOutcomeCounters(families, slowConsumerMetricName, []string{"catch_up", "recovered", "dropped"})
	if err != nil {
		return watchOutcomes{}, err
	}
	generation, err := parseFixedOutcomeCounters(families, watchGenerationMetricName, []string{"retry", "recovered", "compacted", "failed"})
	if err != nil {
		return watchOutcomes{}, err
	}
	return watchOutcomes{
		slow: slowConsumerOutcomes{
			catchUp: slow["catch_up"], recovered: slow["recovered"], dropped: slow["dropped"],
		},
		generation: watchGenerationOutcomes{
			retry: generation["retry"], recovered: generation["recovered"],
			compacted: generation["compacted"], failed: generation["failed"],
		},
	}, nil
}

func parseFixedOutcomeCounters(families map[string]*dto.MetricFamily, name string, outcomes []string) (map[string]float64, error) {
	family := families[name]
	if family == nil {
		family = families[name+"_total"]
	}
	if family == nil {
		return nil, fmt.Errorf("metrics response omits %s", name)
	}
	allowed := make(map[string]struct{}, len(outcomes))
	for _, outcome := range outcomes {
		allowed[outcome] = struct{}{}
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
		if _, ok := allowed[outcome]; !ok {
			return nil, fmt.Errorf("metrics response has unknown %s outcome %q", name, outcome)
		}
		if _, duplicate := values[outcome]; duplicate {
			return nil, fmt.Errorf("metrics response has duplicate %s outcome %q", name, outcome)
		}
		value := metric.Counter.GetValue()
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || math.Trunc(value) != value {
			return nil, fmt.Errorf("metrics response has invalid %s outcome %q counter %v", name, outcome, value)
		}
		values[outcome] = value
	}
	for _, outcome := range outcomes {
		if _, exists := values[outcome]; !exists {
			return nil, fmt.Errorf("metrics response omits %s outcome %q", name, outcome)
		}
	}
	return values, nil
}

func waitForExpectedSlowConsumerPressure(ctx context.Context, reader *slowConsumerMetricsReader, baseline watchOutcomes, outcome string) error {
	expected, err := expectedWatchOutcomes(baseline, outcome, false)
	if err != nil {
		return err
	}
	return waitForWatchOutcomes(ctx, reader, baseline, expected, outcome+" pressure")
}

func waitForExpectedSlowConsumerCompletion(ctx context.Context, reader *slowConsumerMetricsReader, baseline watchOutcomes, outcome string) error {
	expected, err := expectedWatchOutcomes(baseline, outcome, true)
	if err != nil {
		return err
	}
	return waitForWatchOutcomes(ctx, reader, baseline, expected, outcome+" completion")
}

func expectedWatchOutcomes(baseline watchOutcomes, outcome string, completed bool) (watchOutcomes, error) {
	expected := baseline
	expected.slow.catchUp++
	switch outcome {
	case slowConsumerExpectedRecovered:
		if completed {
			expected.slow.recovered++
		}
	case slowConsumerExpectedDropped:
		expected.slow.dropped++
		if completed {
			expected.generation.recovered++
		}
	default:
		return watchOutcomes{}, fmt.Errorf("unsupported slow-consumer expected outcome %q", outcome)
	}
	return expected, nil
}

func waitForWatchOutcomes(ctx context.Context, reader *slowConsumerMetricsReader, baseline, expected watchOutcomes, phase string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := reader.fetch(ctx)
		if err != nil {
			return fmt.Errorf("read slow-consumer outcomes while waiting for %s: %w", phase, err)
		}
		if watchOutcomesRegressed(current, baseline) {
			return fmt.Errorf("slow-consumer counters regressed while waiting for %s: baseline=%+v current=%+v", phase, baseline, current)
		}
		if current == expected {
			return nil
		}
		if watchOutcomesExceeded(current, expected) {
			return fmt.Errorf("watch outcome counters changed beyond this owned watch while waiting for %s: baseline=%+v current=%+v expected=%+v",
				phase, baseline, current, expected)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for slow-consumer %s outcomes: %w; baseline=%+v last=%+v", phase, context.Cause(ctx), baseline, current)
		case <-ticker.C:
		}
	}
}

func watchOutcomesRegressed(current, floor watchOutcomes) bool {
	return current.slow.catchUp < floor.slow.catchUp || current.slow.recovered < floor.slow.recovered ||
		current.slow.dropped < floor.slow.dropped || current.generation.retry < floor.generation.retry ||
		current.generation.recovered < floor.generation.recovered || current.generation.compacted < floor.generation.compacted ||
		current.generation.failed < floor.generation.failed
}

func watchOutcomesExceeded(current, ceiling watchOutcomes) bool {
	return current.slow.catchUp > ceiling.slow.catchUp || current.slow.recovered > ceiling.slow.recovered ||
		current.slow.dropped > ceiling.slow.dropped || current.generation.retry > ceiling.generation.retry ||
		current.generation.recovered > ceiling.generation.recovered || current.generation.compacted > ceiling.generation.compacted ||
		current.generation.failed > ceiling.generation.failed
}
