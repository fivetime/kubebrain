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
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const cleanupTimeout = 15 * time.Second

// Each watcher retains one int64 revision per event until the final comparison.
// Twenty million observations bound that oracle storage to about 160 MiB while
// still allowing 25 watchers at one event/second for more than nine days.
const maximumWatchObservations int64 = 20_000_000

var runIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type config struct {
	endpoint      string
	watchers      int
	events        int
	timeout       time.Duration
	writeInterval time.Duration
	runID         string
	caFile        string
	certFile      string
	keyFile       string
	tlsServerName string
	username      string
	password      string
}

type watcherResult struct {
	id        int
	revisions []int64
	err       error
}

func main() {
	cfg, err := configFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	tlsConfig, err := loadTLSConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.endpoint},
		DialTimeout: min(10*time.Second, cfg.timeout),
		TLS:         tlsConfig,
		Username:    cfg.username,
		Password:    cfg.password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	if err := run(ctx, client, cfg); err != nil {
		log.Fatal(err)
	}
}

func configFromEnvironment() (config, error) {
	watchers, err := positiveInt("WATCHERS", 100000)
	if err != nil {
		return config{}, err
	}
	events, err := positiveInt("EVENTS", 10000000)
	if err != nil {
		return config{}, err
	}
	if int64(watchers)*int64(events) > maximumWatchObservations {
		return config{}, fmt.Errorf("WATCHERS*EVENTS must not exceed %d", maximumWatchObservations)
	}
	timeoutSeconds, err := positiveInt("TIMEOUT_SECONDS", int(^uint(0)>>1))
	if err != nil {
		return config{}, err
	}
	if int64(timeoutSeconds) > int64((time.Duration(1<<63-1))/time.Second) {
		return config{}, errors.New("TIMEOUT_SECONDS exceeds time.Duration")
	}
	writeInterval, err := time.ParseDuration(os.Getenv("WRITE_INTERVAL"))
	if err != nil || writeInterval < 0 {
		return config{}, fmt.Errorf("WRITE_INTERVAL must be a non-negative Go duration: %q", os.Getenv("WRITE_INTERVAL"))
	}
	runID := os.Getenv("RUN_ID")
	if len(runID) == 0 || len(runID) > 63 || !runIDPattern.MatchString(runID) {
		return config{}, fmt.Errorf("RUN_ID must be a 1-63 character lowercase DNS label: %q", runID)
	}
	endpoint := os.Getenv("ENDPOINT")
	if endpoint == "" {
		return config{}, errors.New("ENDPOINT is required")
	}
	certFile, keyFile := os.Getenv("ETCD_CERT_FILE"), os.Getenv("ETCD_KEY_FILE")
	if (certFile == "") != (keyFile == "") {
		return config{}, errors.New("ETCD_CERT_FILE and ETCD_KEY_FILE must be set together")
	}
	username, password := os.Getenv("ETCD_USERNAME"), os.Getenv("ETCD_PASSWORD")
	if (username == "") != (password == "") {
		return config{}, errors.New("ETCD_USERNAME and ETCD_PASSWORD must be set together")
	}
	return config{
		endpoint: endpoint, watchers: watchers, events: events,
		timeout: time.Duration(timeoutSeconds) * time.Second, writeInterval: writeInterval, runID: runID,
		caFile: os.Getenv("ETCD_CA_FILE"), certFile: certFile, keyFile: keyFile,
		tlsServerName: os.Getenv("ETCD_TLS_SERVER_NAME"), username: username, password: password,
	}, nil
}

func positiveInt(name string, maximum int) (int, error) {
	text := os.Getenv(name)
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 1 || value > int64(maximum) || strconv.FormatInt(value, 10) != text {
		return 0, fmt.Errorf("%s must be a canonical positive integer no greater than %d: %q", name, maximum, text)
	}
	return int(value), nil
}

func loadTLSConfig(cfg config) (*tls.Config, error) {
	if cfg.caFile == "" && cfg.certFile == "" && cfg.tlsServerName == "" {
		return nil, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.tlsServerName}
	if cfg.caFile != "" {
		pem, err := os.ReadFile(cfg.caFile)
		if err != nil {
			return nil, fmt.Errorf("read ETCD_CA_FILE: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("ETCD_CA_FILE contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if cfg.certFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.certFile, cfg.keyFile)
		if err != nil {
			return nil, fmt.Errorf("load etcd client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func run(ctx context.Context, client *clientv3.Client, cfg config) (retErr error) {
	prefix := "/registry/watch-soak/" + cfg.runID + "/"
	preflight, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return fmt.Errorf("preflight watch-soak prefix: %w", err)
	}
	if preflight.Count != 0 {
		return fmt.Errorf("refusing non-empty watch-soak prefix %q: count=%d", prefix, preflight.Count)
	}
	if preflight.Header == nil || preflight.Header.Revision < 0 {
		return fmt.Errorf("preflight watch-soak prefix returned invalid header: %+v", preflight.Header)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		retErr = errors.Join(retErr, cleanupPrefix(cleanupCtx, client, prefix))
	}()

	startRevision := preflight.Header.Revision + 1
	watchCtx, cancelWatches := context.WithCancel(ctx)
	defer cancelWatches()
	created := make(chan int, cfg.watchers)
	results := make(chan watcherResult, cfg.watchers)
	for id := 0; id < cfg.watchers; id++ {
		go consumeWatch(watchCtx, client, prefix, startRevision, cfg.events, id, created, results)
	}
	for count := 0; count < cfg.watchers; count++ {
		select {
		case <-created:
		case result := <-results:
			return fmt.Errorf("watcher %d failed before Created barrier: %w", result.id, result.err)
		case <-ctx.Done():
			return fmt.Errorf("wait for Created barrier %d/%d: %w", count, cfg.watchers, context.Cause(ctx))
		}
	}

	writtenRevisions := make([]int64, cfg.events)
	for index := 0; index < cfg.events; index++ {
		if index > 0 && cfg.writeInterval > 0 {
			timer := time.NewTimer(cfg.writeInterval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return fmt.Errorf("wait before event %d: %w", index, context.Cause(ctx))
			}
		}
		response, err := client.Put(ctx, expectedKey(prefix, index), expectedValue(index))
		if err != nil {
			return fmt.Errorf("put event %d: %w", index, err)
		}
		if response.Header == nil || response.Header.Revision <= 0 ||
			(index > 0 && response.Header.Revision <= writtenRevisions[index-1]) {
			return fmt.Errorf("put event %d returned invalid revision: %+v", index, response.Header)
		}
		writtenRevisions[index] = response.Header.Revision
	}

	for count := 0; count < cfg.watchers; count++ {
		select {
		case result := <-results:
			if result.err != nil {
				return fmt.Errorf("watcher %d: %w", result.id, result.err)
			}
			if err := validateObservedRevisions(result.revisions, writtenRevisions); err != nil {
				return fmt.Errorf("watcher %d: %w", result.id, err)
			}
		case <-ctx.Done():
			return fmt.Errorf("wait for watcher completion %d/%d: %w", count, cfg.watchers, context.Cause(ctx))
		}
	}
	cancelWatches()
	fmt.Printf("Watch soak completed: watchers=%d events=%d first_revision=%d last_revision=%d prefix=%s write_interval=%s\n",
		cfg.watchers, cfg.events, writtenRevisions[0], writtenRevisions[len(writtenRevisions)-1], prefix, cfg.writeInterval)
	return nil
}

func consumeWatch(ctx context.Context, client *clientv3.Client, prefix string, startRevision int64, eventCount, id int,
	created chan<- int, results chan<- watcherResult,
) {
	result := watcherResult{id: id, revisions: make([]int64, 0, eventCount)}
	watch := client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(startRevision),
		clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
	createdSeen := false
	for len(result.revisions) < eventCount {
		select {
		case <-ctx.Done():
			result.err = context.Cause(ctx)
			results <- result
			return
		case response, ok := <-watch:
			if !ok {
				result.err = errors.New("watch channel closed")
				results <- result
				return
			}
			if err := response.Err(); err != nil {
				result.err = err
				results <- result
				return
			}
			if response.Canceled {
				result.err = fmt.Errorf("watch canceled: reason=%q compact_revision=%d", response.CancelReason, response.CompactRevision)
				results <- result
				return
			}
			if response.Created && !createdSeen {
				createdSeen = true
				created <- id
			}
			for _, event := range response.Events {
				index := len(result.revisions)
				if index >= eventCount {
					result.err = fmt.Errorf("received extra event at index %d", index)
					results <- result
					return
				}
				if err := validateEvent(prefix, index, event); err != nil {
					result.err = err
					results <- result
					return
				}
				result.revisions = append(result.revisions, event.Kv.ModRevision)
			}
		}
	}
	results <- result
}

func validateEvent(prefix string, index int, event *clientv3.Event) error {
	if event == nil || event.Type != mvccpb.PUT || event.Kv == nil {
		return fmt.Errorf("event %d is not a Put with KeyValue: %+v", index, event)
	}
	if string(event.Kv.Key) != expectedKey(prefix, index) || string(event.Kv.Value) != expectedValue(index) ||
		event.Kv.CreateRevision != event.Kv.ModRevision || event.Kv.ModRevision <= 0 || event.Kv.Version != 1 || event.Kv.Lease != 0 {
		return fmt.Errorf("event %d mismatch: key=%q value=%q create_revision=%d mod_revision=%d version=%d lease=%d",
			index, event.Kv.Key, event.Kv.Value, event.Kv.CreateRevision, event.Kv.ModRevision, event.Kv.Version, event.Kv.Lease)
	}
	return nil
}

func validateObservedRevisions(observed, written []int64) error {
	if len(observed) != len(written) {
		return fmt.Errorf("event count mismatch: got=%d want=%d", len(observed), len(written))
	}
	for index := range written {
		if observed[index] != written[index] {
			return fmt.Errorf("event %d revision mismatch: got=%d want=%d", index, observed[index], written[index])
		}
	}
	return nil
}

func cleanupPrefix(ctx context.Context, client *clientv3.Client, prefix string) error {
	if _, err := client.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("delete owned watch-soak prefix %q: %w", prefix, err)
	}
	response, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return fmt.Errorf("verify owned watch-soak prefix cleanup %q: %w", prefix, err)
	}
	if response.Count != 0 {
		return fmt.Errorf("owned watch-soak prefix %q is not empty after cleanup: count=%d", prefix, response.Count)
	}
	return nil
}

func expectedKey(prefix string, index int) string {
	return fmt.Sprintf("%sevent-%08d", prefix, index)
}

func expectedValue(index int) string {
	return fmt.Sprintf("value-%08d", index)
}
