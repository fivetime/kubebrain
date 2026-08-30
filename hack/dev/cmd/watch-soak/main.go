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
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	grpcstats "google.golang.org/grpc/stats"
)

// Each normal or raw slow watcher retains one (event index, revision) pair per
// event until the final comparison. Twenty million observations bound the pair
// payload to about 320 MiB while still allowing 25 watchers at one event/second
// for more than nine days.
const maximumWatchObservations int64 = 20_000_000
const maximumSlowConsumers = 1000

// Match etcd's default --max-txn-ops and stay within KubeBrain's production
// profile. Every operation is an exact-key Delete, never a broad range delete.
const (
	cleanupDeleteOpsPerTxn   = 128
	cleanupDeleteConcurrency = 16
	cleanupListLimit         = cleanupDeleteOpsPerTxn * cleanupDeleteConcurrency
)

const (
	slowConsumerExpectedRecovered = "recovered"
	slowConsumerExpectedDropped   = "dropped"
	slowConsumerExpectedCompacted = "compacted"
)

var runIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type config struct {
	endpoint                    string
	writeEndpoint               string
	watchers                    int
	events                      int
	timeout                     time.Duration
	cleanupTimeout              time.Duration
	writeInterval               time.Duration
	writeConcurrency            int
	minimumTransportReconnects  int
	runID                       string
	cleanupOnly                 bool
	slowConsumer                bool
	slowConsumers               int
	requireSlowConsumerOutcomes bool
	slowConsumerExpectedOutcome string
	infoEndpoint                string
	caFile                      string
	certFile                    string
	keyFile                     string
	tlsServerName               string
	writeTLSServerName          string
	infoCAFile                  string
	infoCertFile                string
	infoKeyFile                 string
	infoTLSServerName           string
	username                    string
	password                    string
}

type watcherResult struct {
	id           int
	observations []eventObservation
	err          error
}

type eventObservation struct {
	index    int
	revision int64
}

type putEventFunc func(context.Context, string, string) (*clientv3.PutResponse, error)
type etcdClientFactory func() (*clientv3.Client, error)

type transportConnectionTracker struct {
	connections atomic.Int64
}

func (*transportConnectionTracker) TagRPC(ctx context.Context, _ *grpcstats.RPCTagInfo) context.Context {
	return ctx
}

func (*transportConnectionTracker) HandleRPC(context.Context, grpcstats.RPCStats) {}

func (*transportConnectionTracker) TagConn(ctx context.Context, _ *grpcstats.ConnTagInfo) context.Context {
	return ctx
}

func (tracker *transportConnectionTracker) HandleConn(_ context.Context, event grpcstats.ConnStats) {
	if begin, ok := event.(*grpcstats.ConnBegin); ok && begin.Client {
		tracker.connections.Add(1)
	}
}

func (tracker *transportConnectionTracker) connectionCount() int64 {
	return tracker.connections.Load()
}

func main() {
	processCtx, stop := processContext()
	defer stop()
	fmt.Fprintf(os.Stderr, "Watch soak signal handler ready: pid=%d\n", os.Getpid())

	cfg, err := configFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	tlsConfig, err := loadTLSConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	connectionTracker := &transportConnectionTracker{}
	clientFactory := func() (*clientv3.Client, error) { return newEtcdClient(cfg, tlsConfig, nil) }
	client, err := newEtcdClient(cfg, tlsConfig, connectionTracker)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	writeClient := client
	if cfg.writeEndpoint != "" {
		writeConfig, writeTLSConfig := deriveWriteClientConfig(cfg, tlsConfig)
		writeClient, err = newEtcdClient(writeConfig, writeTLSConfig, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer writeClient.Close()
	}

	if err := runWithTimeout(processCtx, cfg.timeout, func(ctx context.Context) error {
		return run(ctx, client, writeClient, cfg, clientFactory, connectionTracker)
	}); err != nil {
		log.Fatal(err)
	}
}

func deriveWriteClientConfig(cfg config, tlsConfig *tls.Config) (config, *tls.Config) {
	writeConfig := cfg
	writeConfig.endpoint = cfg.writeEndpoint
	if cfg.writeTLSServerName != "" {
		writeConfig.tlsServerName = cfg.writeTLSServerName
	}
	if tlsConfig != nil {
		writeTLSConfig := tlsConfig.Clone()
		writeTLSConfig.ServerName = writeConfig.tlsServerName
		return writeConfig, writeTLSConfig
	}
	if writeConfig.tlsServerName != "" {
		return writeConfig, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: writeConfig.tlsServerName}
	}
	return writeConfig, nil
}

func processContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Restore the default signal behavior after the first signal. The first one
	// enters run's bounded exact-prefix cleanup; a second one can still force an
	// exit if an external dependency prevents that cleanup from completing.
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}

func runWithTimeout(parent context.Context, timeout time.Duration, run func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return run(ctx)
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
	writeConcurrency, err := positiveInt("WRITE_CONCURRENCY", 1024)
	if err != nil {
		return config{}, err
	}
	minimumTransportReconnects, err := nonNegativeInt("MIN_TRANSPORT_RECONNECTS", 1_000_000)
	if err != nil {
		return config{}, err
	}
	slowConsumer, err := strictBool("SLOW_CONSUMER")
	if err != nil {
		return config{}, err
	}
	slowConsumers := 0
	if slowConsumer {
		slowConsumers = 1
	}
	if os.Getenv("SLOW_CONSUMERS") != "" {
		if !slowConsumer {
			return config{}, errors.New("SLOW_CONSUMERS requires SLOW_CONSUMER=true")
		}
		slowConsumers, err = positiveInt("SLOW_CONSUMERS", maximumSlowConsumers)
		if err != nil {
			return config{}, err
		}
	}
	requireOutcomes, err := strictBool("REQUIRE_SLOW_CONSUMER_OUTCOMES")
	if err != nil {
		return config{}, err
	}
	cleanupOnly, err := strictBool("CLEANUP_ONLY")
	if err != nil {
		return config{}, err
	}
	if requireOutcomes && !slowConsumer {
		return config{}, errors.New("REQUIRE_SLOW_CONSUMER_OUTCOMES=true requires SLOW_CONSUMER=true")
	}
	expectedOutcome := os.Getenv("SLOW_CONSUMER_EXPECTED_OUTCOME")
	if expectedOutcome != slowConsumerExpectedRecovered && expectedOutcome != slowConsumerExpectedDropped &&
		expectedOutcome != slowConsumerExpectedCompacted {
		return config{}, fmt.Errorf("SLOW_CONSUMER_EXPECTED_OUTCOME must be recovered, dropped, or compacted: %q", expectedOutcome)
	}
	if expectedOutcome != slowConsumerExpectedRecovered && (!slowConsumer || !requireOutcomes) {
		return config{}, fmt.Errorf("SLOW_CONSUMER_EXPECTED_OUTCOME=%s requires SLOW_CONSUMER=true and REQUIRE_SLOW_CONSUMER_OUTCOMES=true", expectedOutcome)
	}
	observers := int64(watchers + slowConsumers)
	if observers*int64(events) > maximumWatchObservations {
		return config{}, fmt.Errorf("(WATCHERS+SLOW_CONSUMERS)*EVENTS must not exceed %d", maximumWatchObservations)
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
	writeEndpoint, writeTLSServerName := os.Getenv("WRITE_ENDPOINT"), os.Getenv("WRITE_TLS_SERVER_NAME")
	if writeTLSServerName != "" && writeEndpoint == "" {
		return config{}, errors.New("WRITE_TLS_SERVER_NAME requires WRITE_ENDPOINT")
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
		endpoint: endpoint, writeEndpoint: writeEndpoint, watchers: watchers, events: events,
		timeout:        time.Duration(timeoutSeconds) * time.Second,
		cleanupTimeout: time.Duration(cleanupTimeoutSeconds) * time.Second,
		writeInterval:  writeInterval, writeConcurrency: writeConcurrency,
		minimumTransportReconnects: minimumTransportReconnects, runID: runID, cleanupOnly: cleanupOnly,
		slowConsumer: slowConsumer, slowConsumers: slowConsumers, requireSlowConsumerOutcomes: requireOutcomes,
		slowConsumerExpectedOutcome: expectedOutcome, infoEndpoint: infoEndpoint,
		caFile: os.Getenv("ETCD_CA_FILE"), certFile: certFile, keyFile: keyFile,
		tlsServerName: os.Getenv("ETCD_TLS_SERVER_NAME"), writeTLSServerName: writeTLSServerName,
		username: username, password: password,
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

func nonNegativeInt(name string, maximum int) (int, error) {
	text := os.Getenv(name)
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 0 || value > int64(maximum) || strconv.FormatInt(value, 10) != text {
		return 0, fmt.Errorf("%s must be a canonical non-negative integer no greater than %d: %q", name, maximum, text)
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

// newEtcdClient clones the already-loaded TLS configuration so every raw slow
// consumer owns an independent gRPC connection without reopening credential
// paths. This is required for /dev/fd process substitutions, which may only be
// consumed once and must never be copied to persistent files.
func newEtcdClient(cfg config, tlsConfig *tls.Config, connectionTracker *transportConnectionTracker) (*clientv3.Client, error) {
	var clientTLS *tls.Config
	if tlsConfig != nil {
		clientTLS = tlsConfig.Clone()
	}
	var dialOptions []grpc.DialOption
	if cfg.tlsServerName != "" {
		// The etcd resolver assigns every endpoint its own ServerName. In recent
		// gRPC versions that address-level value takes precedence over
		// tls.Config.ServerName unless the caller supplies an explicit authority.
		dialOptions = append(dialOptions, grpc.WithAuthority(cfg.tlsServerName))
	}
	if connectionTracker != nil {
		dialOptions = append(dialOptions, grpc.WithStatsHandler(connectionTracker))
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.endpoint},
		DialTimeout: min(10*time.Second, cfg.timeout),
		DialOptions: dialOptions,
		TLS:         clientTLS,
		Username:    cfg.username,
		Password:    cfg.password,
	})
}

func run(ctx context.Context, client, writeClient *clientv3.Client, cfg config, clientFactory etcdClientFactory,
	connectionTracker *transportConnectionTracker,
) (retErr error) {
	if client == nil || writeClient == nil {
		return errors.New("watch and write clients are required")
	}
	prefix := "/registry/watch-soak/" + cfg.runID + "/"
	if cfg.cleanupOnly {
		// Recovery mode deliberately does not claim a non-empty prefix through the
		// normal preflight: its sole purpose is to clean the exact RUN_ID after an
		// interrupted process could not execute its defer. It retains the same
		// bounded exact-key transactions and final CountOnly proof as normal exit.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.cleanupTimeout)
		defer cancel()
		if err := cleanupPrefix(cleanupCtx, writeClient, prefix); err != nil {
			return err
		}
		fmt.Printf("Watch soak cleanup completed: prefix=%s\n", prefix)
		return nil
	}
	if client != writeClient {
		watchStatus, err := client.Status(ctx, cfg.endpoint)
		if err != nil {
			return fmt.Errorf("read watch endpoint status: %w", err)
		}
		writeStatus, err := writeClient.Status(ctx, cfg.writeEndpoint)
		if err != nil {
			return fmt.Errorf("read write endpoint status: %w", err)
		}
		if err := validateWatchWriteStatuses(watchStatus, writeStatus); err != nil {
			return err
		}
	}
	preflight, err := writeClient.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
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
		cleanupErr := cleanupPrefix(cleanupCtx, writeClient, prefix)
		if cleanupErr == nil {
			fmt.Printf("Watch soak cleanup completed: prefix=%s\n", prefix)
		}
		retErr = errors.Join(retErr, cleanupErr)
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
	if connectionTracker == nil || connectionTracker.connectionCount() < 1 {
		return errors.New("watch Created barrier completed without an observed client transport connection")
	}
	transportConnectionBaseline := connectionTracker.connectionCount()

	var metricsReader *slowConsumerMetricsReader
	var baseline watchOutcomes
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
	slows := make([]*rawSlowWatch, 0, cfg.slowConsumers)
	if cfg.slowConsumers > 0 {
		if clientFactory == nil {
			return errors.New("raw slow consumers require an etcd client factory")
		}
		if err := requireDirectLeader(ctx, client, cfg.endpoint); err != nil {
			return err
		}
		for id := 0; id < cfg.slowConsumers; id++ {
			rawClient, clientErr := clientFactory()
			if clientErr != nil {
				return fmt.Errorf("create raw slow-consumer client %d: %w", id, clientErr)
			}
			defer rawClient.Close()
			if err := requireDirectLeader(ctx, rawClient, cfg.endpoint); err != nil {
				return fmt.Errorf("confirm direct leader for raw slow-consumer client %d: %w", id, err)
			}
			slow, startErr := startRawSlowWatch(watchCtx, rawClient, prefix, startRevision, cfg.events)
			if startErr != nil {
				return fmt.Errorf("start raw slow-consumer watch %d: %w", id, startErr)
			}
			defer slow.close()
			slows = append(slows, slow)
		}
		if err := requireDirectLeader(ctx, client, cfg.endpoint); err != nil {
			return fmt.Errorf("confirm direct leader after %d raw watch Created responses: %w", len(slows), err)
		}
	}

	writtenEvents, err := writeEvents(ctx, prefix, cfg.events, cfg.writeConcurrency, cfg.writeInterval,
		func(putCtx context.Context, key, value string) (*clientv3.PutResponse, error) {
			return writeClient.Put(putCtx, key, value)
		})
	if err != nil {
		return err
	}
	if cfg.requireSlowConsumerOutcomes {
		// The pressure barrier proves that the raw watcher filled its fan-out
		// channel and entered ring catch-up before we let it read. In dropped
		// mode the catch-up goroutine can only discover that its next revision
		// was evicted after the raw watcher drains enough of that full channel,
		// so the drop itself belongs to the completion barrier below.
		if err := waitForExpectedSlowConsumerPressure(
			ctx, metricsReader, baseline, cfg.slowConsumerExpectedOutcome, cfg.slowConsumers,
		); err != nil {
			return err
		}
	}
	compactRevision := int64(0)
	if cfg.slowConsumerExpectedOutcome == slowConsumerExpectedCompacted {
		compactRevision = writtenEvents[len(writtenEvents)-1].revision
		response, compactErr := writeClient.Compact(ctx, compactRevision)
		if compactErr = validateSlowConsumerCompactResponse(compactRevision, response, compactErr); compactErr != nil {
			return compactErr
		}
	}
	if err := consumeRawSlowWatches(slows, writtenEvents, cfg.slowConsumerExpectedOutcome); err != nil {
		return err
	}

	for count := 0; count < cfg.watchers; count++ {
		select {
		case result := <-results:
			if result.err != nil {
				return fmt.Errorf("watcher %d: %w", result.id, result.err)
			}
			if err := validateObservedEvents(result.observations, writtenEvents); err != nil {
				return fmt.Errorf("watcher %d: %w", result.id, err)
			}
		case <-ctx.Done():
			return fmt.Errorf("wait for watcher completion %d/%d: %w", count, cfg.watchers, context.Cause(ctx))
		}
	}
	if cfg.requireSlowConsumerOutcomes {
		if err := waitForExpectedSlowConsumerCompletion(
			ctx, metricsReader, baseline, cfg.slowConsumerExpectedOutcome, cfg.slowConsumers,
		); err != nil {
			return err
		}
	}
	transportConnections := connectionTracker.connectionCount()
	transportReconnects, err := validateTransportReconnects(
		cfg.minimumTransportReconnects, transportConnectionBaseline, transportConnections,
	)
	if err != nil {
		return err
	}
	cancelWatches()
	fmt.Printf("Watch soak completed: watchers=%d slow_consumer=%t slow_consumers=%d expected_slow_outcome=%s outcome_metrics=%t events=%d first_revision=%d last_revision=%d compact_revision=%d prefix=%s write_interval=%s write_concurrency=%d separate_write_endpoint=%t transport_connections=%d transport_reconnects=%d minimum_transport_reconnects=%d\n",
		cfg.watchers, cfg.slowConsumer, cfg.slowConsumers, cfg.slowConsumerExpectedOutcome, cfg.requireSlowConsumerOutcomes, cfg.events, writtenEvents[0].revision,
		writtenEvents[len(writtenEvents)-1].revision, compactRevision, prefix, cfg.writeInterval, cfg.writeConcurrency,
		client != writeClient, transportConnections, transportReconnects, cfg.minimumTransportReconnects)
	return nil
}

func validateWatchWriteStatuses(watch, write *clientv3.StatusResponse) error {
	if watch == nil || watch.Header == nil || watch.Header.ClusterId == 0 || watch.Header.MemberId == 0 {
		return fmt.Errorf("watch endpoint returned incomplete cluster identity: %+v", watch)
	}
	if write == nil || write.Header == nil || write.Header.ClusterId == 0 || write.Header.MemberId == 0 {
		return fmt.Errorf("write endpoint returned incomplete cluster identity: %+v", write)
	}
	if write.Header.ClusterId != watch.Header.ClusterId {
		return fmt.Errorf("watch/write endpoint cluster ID mismatch: watch=%d write=%d", watch.Header.ClusterId, write.Header.ClusterId)
	}
	return nil
}

func validateTransportReconnects(minimum int, baseline, total int64) (int64, error) {
	if minimum < 0 || baseline < 1 || total < baseline {
		return 0, fmt.Errorf("invalid transport connection accounting: minimum=%d baseline=%d total=%d", minimum, baseline, total)
	}
	reconnects := total - baseline
	if reconnects < int64(minimum) {
		return reconnects, fmt.Errorf("watch soak observed %d transport reconnects after Created barrier, require at least %d (baseline=%d total=%d)",
			reconnects, minimum, baseline, total)
	}
	return reconnects, nil
}

func validateSlowConsumerCompactResponse(revision int64, response *clientv3.CompactResponse, err error) error {
	if err != nil {
		return fmt.Errorf("compact slow-consumer history at revision %d: %w", revision, err)
	}
	if response == nil || response.Header == nil || response.Header.Revision < revision {
		return fmt.Errorf("compact slow-consumer history at revision %d returned invalid response: %+v", revision, response)
	}
	return nil
}

func writeEvents(ctx context.Context, prefix string, events, concurrency int, interval time.Duration, put putEventFunc) ([]eventObservation, error) {
	observations := make([]eventObservation, events)
	group, writeCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	started := time.Now()
	progressEvery := watchSoakProgressEvery(events)
	var completed atomic.Int64
	var progressMu sync.Mutex
	for index := 0; index < events; index++ {
		if index > 0 && interval > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
			case <-writeCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				if err := group.Wait(); err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("wait before event %d: %w", index, context.Cause(writeCtx))
			}
		}
		if writeCtx.Err() != nil {
			break
		}
		index := index
		group.Go(func() error {
			response, err := put(writeCtx, expectedKey(prefix, index), expectedValue(index))
			if err != nil {
				return fmt.Errorf("put event %d: %w", index, err)
			}
			if response == nil || response.Header == nil || response.Header.Revision <= 0 {
				return fmt.Errorf("put event %d returned invalid response: %+v", index, response)
			}
			observations[index] = eventObservation{index: index, revision: response.Header.Revision}
			written := completed.Add(1)
			if written%int64(progressEvery) == 0 || written == int64(events) {
				progressMu.Lock()
				fmt.Fprintf(os.Stderr, "Watch soak write progress: written=%d total=%d revision=%d elapsed=%s\n",
					written, events, response.Header.Revision, time.Since(started).Round(time.Millisecond))
				progressMu.Unlock()
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if completed.Load() != int64(events) {
		return nil, fmt.Errorf("write group completed %d/%d events", completed.Load(), events)
	}
	sort.Slice(observations, func(i, j int) bool { return observations[i].revision < observations[j].revision })
	for index, observation := range observations {
		if observation.revision <= 0 || (index > 0 && observation.revision <= observations[index-1].revision) {
			return nil, fmt.Errorf("written event revisions are not unique and increasing at position %d: previous=%d current=%d",
				index, observations[max(0, index-1)].revision, observation.revision)
		}
	}
	return observations, nil
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
	result := watcherResult{id: id, observations: make([]eventObservation, 0, eventCount)}
	watch := client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(startRevision),
		clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
	createdSeen := false
	for len(result.observations) < eventCount {
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
				position := len(result.observations)
				if position >= eventCount {
					result.err = fmt.Errorf("received extra event at position %d", position)
					results <- result
					return
				}
				observation, observeErr := observeEvent(prefix, event)
				if observeErr != nil {
					result.err = fmt.Errorf("event position %d: %w", position, observeErr)
					results <- result
					return
				}
				result.observations = append(result.observations, observation)
			}
		}
	}
	results <- result
}

func validateEvent(prefix string, index int, event *clientv3.Event) error {
	observation, err := observeEvent(prefix, event)
	if err != nil {
		return err
	}
	if observation.index != index {
		return fmt.Errorf("event index mismatch: got=%d want=%d", observation.index, index)
	}
	return nil
}

func observeEvent(prefix string, event *clientv3.Event) (eventObservation, error) {
	if event == nil || event.Type != mvccpb.PUT || event.Kv == nil {
		return eventObservation{}, fmt.Errorf("event is not a Put with KeyValue: %+v", event)
	}
	key := string(event.Kv.Key)
	const marker = "event-"
	if !strings.HasPrefix(key, prefix+marker) {
		return eventObservation{}, fmt.Errorf("event key is outside owned prefix: %q", event.Kv.Key)
	}
	indexText := strings.TrimPrefix(key, prefix+marker)
	index64, err := strconv.ParseInt(indexText, 10, 32)
	if err != nil || index64 < 0 || expectedKey(prefix, int(index64)) != key {
		return eventObservation{}, fmt.Errorf("event key has invalid canonical index: %q", event.Kv.Key)
	}
	index := int(index64)
	if string(event.Kv.Value) != expectedValue(index) ||
		event.Kv.CreateRevision != event.Kv.ModRevision || event.Kv.ModRevision <= 0 || event.Kv.Version != 1 || event.Kv.Lease != 0 {
		return eventObservation{}, fmt.Errorf("event %d mismatch: key=%q value=%q create_revision=%d mod_revision=%d version=%d lease=%d",
			index, event.Kv.Key, event.Kv.Value, event.Kv.CreateRevision, event.Kv.ModRevision, event.Kv.Version, event.Kv.Lease)
	}
	return eventObservation{index: index, revision: event.Kv.ModRevision}, nil
}

func validateObservedEvents(observed, written []eventObservation) error {
	if len(observed) != len(written) {
		return fmt.Errorf("event count mismatch: got=%d want=%d", len(observed), len(written))
	}
	for position := range written {
		if observed[position] != written[position] {
			return fmt.Errorf("event position %d mismatch: got=%+v want=%+v", position, observed[position], written[position])
		}
	}
	return nil
}

func cleanupPrefix(ctx context.Context, client *clientv3.Client, prefix string) error {
	for {
		response, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(cleanupListLimit))
		if err != nil {
			return fmt.Errorf("list owned watch-soak prefix %q for cleanup: %w", prefix, err)
		}
		if len(response.Kvs) == 0 {
			break
		}
		group, deleteCtx := errgroup.WithContext(ctx)
		group.SetLimit(cleanupDeleteConcurrency)
		for start := 0; start < len(response.Kvs); start += cleanupDeleteOpsPerTxn {
			end := min(start+cleanupDeleteOpsPerTxn, len(response.Kvs))
			batch := response.Kvs[start:end]
			group.Go(func() error {
				return deleteCleanupBatch(deleteCtx, client, prefix, batch)
			})
		}
		if err := group.Wait(); err != nil {
			return err
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

func deleteCleanupBatch(ctx context.Context, client *clientv3.Client, prefix string, kvs []*mvccpb.KeyValue) error {
	operations, err := cleanupDeleteOperations(prefix, kvs)
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
