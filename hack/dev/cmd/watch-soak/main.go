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
	"bytes"
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

// Each normal or raw slow watcher retains one int64 revision per event until
// the final comparison. Twenty million observations bound that oracle storage
// to about 160 MiB while still allowing 25 watchers at one event/second for
// more than nine days.
const maximumWatchObservations int64 = 20_000_000

// Match etcd's default --max-txn-ops and stay within KubeBrain's production
// profile. Every operation is an exact-key Delete, never a broad range delete.
const cleanupDeleteOpsPerTxn = 128

var runIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type config struct {
	endpoint                    string
	watchers                    int
	events                      int
	timeout                     time.Duration
	cleanupTimeout              time.Duration
	writeInterval               time.Duration
	runID                       string
	slowConsumer                bool
	requireSlowConsumerOutcomes bool
	infoEndpoint                string
	caFile                      string
	certFile                    string
	keyFile                     string
	tlsServerName               string
	infoCAFile                  string
	infoCertFile                string
	infoKeyFile                 string
	infoTLSServerName           string
	username                    string
	password                    string
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
	slowConsumer, err := strictBool("SLOW_CONSUMER")
	if err != nil {
		return config{}, err
	}
	requireOutcomes, err := strictBool("REQUIRE_SLOW_CONSUMER_OUTCOMES")
	if err != nil {
		return config{}, err
	}
	if requireOutcomes && !slowConsumer {
		return config{}, errors.New("REQUIRE_SLOW_CONSUMER_OUTCOMES=true requires SLOW_CONSUMER=true")
	}
	observers := int64(watchers)
	if slowConsumer {
		observers++
	}
	if observers*int64(events) > maximumWatchObservations {
		return config{}, fmt.Errorf("(WATCHERS+slow-consumer)*EVENTS must not exceed %d", maximumWatchObservations)
	}
	timeoutSeconds, err := positiveInt("TIMEOUT_SECONDS", int(^uint(0)>>1))
	if err != nil {
		return config{}, err
	}
	if int64(timeoutSeconds) > int64((time.Duration(1<<63-1))/time.Second) {
		return config{}, errors.New("TIMEOUT_SECONDS exceeds time.Duration")
	}
	cleanupTimeoutSeconds, err := positiveInt("CLEANUP_TIMEOUT_SECONDS", int(^uint(0)>>1))
	if err != nil {
		return config{}, err
	}
	if int64(cleanupTimeoutSeconds) > int64((time.Duration(1<<63-1))/time.Second) {
		return config{}, errors.New("CLEANUP_TIMEOUT_SECONDS exceeds time.Duration")
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
	infoEndpoint := os.Getenv("INFO_ENDPOINT")
	if requireOutcomes && infoEndpoint == "" {
		return config{}, errors.New("INFO_ENDPOINT is required when REQUIRE_SLOW_CONSUMER_OUTCOMES=true")
	}
	if err := validateInfoEndpoint(infoEndpoint); err != nil {
		return config{}, err
	}
	infoCertFile, infoKeyFile := os.Getenv("INFO_CERT_FILE"), os.Getenv("INFO_KEY_FILE")
	if (infoCertFile == "") != (infoKeyFile == "") {
		return config{}, errors.New("INFO_CERT_FILE and INFO_KEY_FILE must be set together")
	}
	return config{
		endpoint: endpoint, watchers: watchers, events: events,
		timeout:        time.Duration(timeoutSeconds) * time.Second,
		cleanupTimeout: time.Duration(cleanupTimeoutSeconds) * time.Second,
		writeInterval:  writeInterval, runID: runID,
		slowConsumer: slowConsumer, requireSlowConsumerOutcomes: requireOutcomes, infoEndpoint: infoEndpoint,
		caFile: os.Getenv("ETCD_CA_FILE"), certFile: certFile, keyFile: keyFile,
		tlsServerName: os.Getenv("ETCD_TLS_SERVER_NAME"), username: username, password: password,
		infoCAFile: os.Getenv("INFO_CA_FILE"), infoCertFile: infoCertFile, infoKeyFile: infoKeyFile,
		infoTLSServerName: os.Getenv("INFO_TLS_SERVER_NAME"),
	}, nil
}

func strictBool(name string) (bool, error) {
	switch text := os.Getenv(name); text {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be true or false: %q", name, text)
	}
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
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.cleanupTimeout)
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

	var metricsReader *slowConsumerMetricsReader
	var baseline slowConsumerOutcomes
	if cfg.requireSlowConsumerOutcomes {
		metricsReader, err = newSlowConsumerMetricsReader(cfg)
		if err != nil {
			return fmt.Errorf("configure slow-consumer outcome reader: %w", err)
		}
		defer metricsReader.close()
		baseline, err = metricsReader.fetch(ctx)
		if err != nil {
			return fmt.Errorf("read baseline slow-consumer outcomes: %w", err)
		}
	}
	var slow *rawSlowWatch
	if cfg.slowConsumer {
		if err := requireDirectLeader(ctx, client, cfg.endpoint); err != nil {
			return err
		}
		slow, err = startRawSlowWatch(watchCtx, client, prefix, startRevision, cfg.events)
		if err != nil {
			return fmt.Errorf("start raw slow-consumer watch: %w", err)
		}
		defer slow.close()
		if err := requireDirectLeader(ctx, client, cfg.endpoint); err != nil {
			return fmt.Errorf("confirm direct leader after raw watch Created: %w", err)
		}
	}

	writtenRevisions := make([]int64, cfg.events)
	writeStarted := time.Now()
	progressEvery := watchSoakProgressEvery(cfg.events)
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
		written := index + 1
		if written%progressEvery == 0 || written == cfg.events {
			fmt.Fprintf(os.Stderr, "Watch soak write progress: written=%d total=%d revision=%d elapsed=%s\n",
				written, cfg.events, response.Header.Revision, time.Since(writeStarted).Round(time.Millisecond))
		}
	}
	if cfg.requireSlowConsumerOutcomes {
		if err := waitForSlowConsumerEntered(ctx, metricsReader, baseline); err != nil {
			return err
		}
	}
	if slow != nil {
		slowRevisions, slowErr := slow.consume()
		if slowErr != nil {
			return fmt.Errorf("raw slow-consumer watcher: %w", slowErr)
		}
		if err := validateObservedRevisions(slowRevisions, writtenRevisions); err != nil {
			return fmt.Errorf("raw slow-consumer watcher: %w", err)
		}
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
	if cfg.requireSlowConsumerOutcomes {
		if err := waitForSlowConsumerRecovered(ctx, metricsReader, baseline); err != nil {
			return err
		}
	}
	cancelWatches()
	fmt.Printf("Watch soak completed: watchers=%d slow_consumer=%t outcome_metrics=%t events=%d first_revision=%d last_revision=%d prefix=%s write_interval=%s\n",
		cfg.watchers, cfg.slowConsumer, cfg.requireSlowConsumerOutcomes, cfg.events, writtenRevisions[0],
		writtenRevisions[len(writtenRevisions)-1], prefix, cfg.writeInterval)
	return nil
}

func watchSoakProgressEvery(events int) int {
	every := events / 100
	if every < 1000 {
		every = 1000
	}
	return every
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
	for {
		response, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(cleanupDeleteOpsPerTxn))
		if err != nil {
			return fmt.Errorf("list owned watch-soak prefix %q for cleanup: %w", prefix, err)
		}
		if len(response.Kvs) == 0 {
			break
		}
		operations, err := cleanupDeleteOperations(prefix, response.Kvs)
		if err != nil {
			return err
		}
		transaction, err := client.Txn(ctx).Then(operations...).Commit()
		if err != nil {
			return fmt.Errorf("delete owned watch-soak prefix %q batch: %w", prefix, err)
		}
		if transaction == nil || transaction.Header == nil || len(transaction.Responses) != len(operations) {
			return fmt.Errorf("delete owned watch-soak prefix %q returned invalid transaction response", prefix)
		}
		for index, operation := range transaction.Responses {
			deleted := operation.GetResponseDeleteRange()
			if deleted == nil || deleted.Deleted != 1 {
				return fmt.Errorf("delete owned watch-soak prefix %q batch operation %d deleted %d keys", prefix, index, deleted.GetDeleted())
			}
		}
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

func cleanupDeleteOperations(prefix string, kvs []*mvccpb.KeyValue) ([]clientv3.Op, error) {
	if len(kvs) == 0 || len(kvs) > cleanupDeleteOpsPerTxn {
		return nil, fmt.Errorf("cleanup list for %q returned invalid batch size %d", prefix, len(kvs))
	}
	operations := make([]clientv3.Op, 0, len(kvs))
	for _, kv := range kvs {
		if kv == nil || len(kv.Key) == 0 || !bytes.HasPrefix(kv.Key, []byte(prefix)) {
			return nil, fmt.Errorf("cleanup list for %q returned out-of-prefix key %q", prefix, kv.GetKey())
		}
		operations = append(operations, clientv3.OpDelete(string(kv.Key)))
	}
	return operations, nil
}

func expectedKey(prefix string, index int) string {
	return fmt.Sprintf("%sevent-%08d", prefix, index)
}

func expectedValue(index int) string {
	return fmt.Sprintf("value-%08d", index)
}
