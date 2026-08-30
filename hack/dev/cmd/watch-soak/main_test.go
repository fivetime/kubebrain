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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("ENDPOINT", "127.0.0.1:3379")
	t.Setenv("WATCHERS", "25")
	t.Setenv("EVENTS", "50")
	t.Setenv("TIMEOUT_SECONDS", "60")
	t.Setenv("CLEANUP_TIMEOUT_SECONDS", "300")
	t.Setenv("WRITE_INTERVAL", "100ms")
	t.Setenv("WRITE_CONCURRENCY", "1")
	t.Setenv("RUN_ID", "watch-soak-test")
	t.Setenv("CLEANUP_ONLY", "false")
	t.Setenv("SLOW_CONSUMER", "false")
	t.Setenv("SLOW_CONSUMERS", "")
	t.Setenv("REQUIRE_SLOW_CONSUMER_OUTCOMES", "false")
	t.Setenv("SLOW_CONSUMER_EXPECTED_OUTCOME", "recovered")
	t.Setenv("INFO_ENDPOINT", "")
}

func TestConfigFromEnvironment(t *testing.T) {
	setValidEnvironment(t)
	cfg, err := configFromEnvironment()
	require.NoError(t, err)
	require.Equal(t, 25, cfg.watchers)
	require.Equal(t, 50, cfg.events)
	require.Equal(t, "watch-soak-test", cfg.runID)
	require.Equal(t, 300*time.Second, cfg.cleanupTimeout)
	require.Equal(t, 1, cfg.writeConcurrency)
	require.False(t, cfg.cleanupOnly)
	require.False(t, cfg.slowConsumer)
	require.Zero(t, cfg.slowConsumers)
	require.Equal(t, slowConsumerExpectedRecovered, cfg.slowConsumerExpectedOutcome)
}

func TestSignalCancellationRunsDeferredCleanup(t *testing.T) {
	const helperEnvironment = "WATCH_SOAK_SIGNAL_HELPER"
	if os.Getenv(helperEnvironment) == "true" {
		ctx, stop := processContext()
		defer stop()
		err := runWithTimeout(ctx, time.Hour, func(runCtx context.Context) (retErr error) {
			defer fmt.Println("deferred cleanup executed")
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				return fmt.Errorf("send SIGTERM: %w", err)
			}
			select {
			case <-runCtx.Done():
				return context.Cause(runCtx)
			case <-time.After(5 * time.Second):
				return errors.New("SIGTERM did not cancel the watch-soak context")
			}
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("signal cancellation error = %v, want context.Canceled", err)
		}
		fmt.Println("signal cancellation observed")
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestSignalCancellationRunsDeferredCleanup$")
	command.Env = append(os.Environ(), helperEnvironment+"=true")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "deferred cleanup executed")
	require.Contains(t, string(output), "signal cancellation observed")
}

func TestNewEtcdClientHonorsExplicitTLSServerName(t *testing.T) {
	const serverName = "watch-soak.test"
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	certificateTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: serverName},
		DNSNames:              []string{serverName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate,
		privateKey.Public(), privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(certificateDER)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(certificate)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{certificateDER}, PrivateKey: privateKey}},
	})))
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveErrors)
	})

	client, err := newEtcdClient(config{
		endpoint: listener.Addr().String(), timeout: 5 * time.Second, tlsServerName: serverName,
	}, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	connection := client.ActiveConnection()
	connection.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for connection.GetState() != connectivity.Ready {
		state := connection.GetState()
		if !connection.WaitForStateChange(ctx, state) {
			t.Fatalf("client did not complete TLS handshake using explicit server name: state=%s err=%v",
				connection.GetState(), context.Cause(ctx))
		}
	}
}

func TestConfigRejectsNonCanonicalOrUnsafeInputs(t *testing.T) {
	for name, testCase := range map[string]struct {
		mutate func(*testing.T)
		want   string
	}{
		"zero watchers": {mutate: func(t *testing.T) { t.Setenv("WATCHERS", "0") }, want: "WATCHERS must be"},
		"leading zero":  {mutate: func(t *testing.T) { t.Setenv("EVENTS", "050") }, want: "EVENTS must be"},
		"observation memory bound": {mutate: func(t *testing.T) {
			t.Setenv("WATCHERS", "10000")
			t.Setenv("EVENTS", "2001")
		}, want: "(WATCHERS+SLOW_CONSUMERS)*EVENTS must not exceed"},
		"negative interval": {mutate: func(t *testing.T) { t.Setenv("WRITE_INTERVAL", "-1s") }, want: "WRITE_INTERVAL must be"},
		"zero write concurrency": {mutate: func(t *testing.T) {
			t.Setenv("WRITE_CONCURRENCY", "0")
		}, want: "WRITE_CONCURRENCY must be"},
		"excessive write concurrency": {mutate: func(t *testing.T) {
			t.Setenv("WRITE_CONCURRENCY", "1025")
		}, want: "WRITE_CONCURRENCY must be"},
		"unsafe run ID":             {mutate: func(t *testing.T) { t.Setenv("RUN_ID", "../shared") }, want: "RUN_ID must be"},
		"invalid cleanup-only flag": {mutate: func(t *testing.T) { t.Setenv("CLEANUP_ONLY", "1") }, want: "CLEANUP_ONLY must be"},
		"partial TLS identity":      {mutate: func(t *testing.T) { t.Setenv("ETCD_CERT_FILE", "client.crt") }, want: "must be set together"},
		"partial password identity": {mutate: func(t *testing.T) { t.Setenv("ETCD_USERNAME", "root") }, want: "must be set together"},
		"invalid slow flag":         {mutate: func(t *testing.T) { t.Setenv("SLOW_CONSUMER", "1") }, want: "SLOW_CONSUMER must be"},
		"slow count without safety flag": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMERS", "2")
		}, want: "SLOW_CONSUMERS requires SLOW_CONSUMER=true"},
		"noncanonical slow count": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMER", "true")
			t.Setenv("SLOW_CONSUMERS", "02")
		}, want: "SLOW_CONSUMERS must be"},
		"excessive slow count": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMER", "true")
			t.Setenv("SLOW_CONSUMERS", "1001")
		}, want: "SLOW_CONSUMERS must be"},
		"invalid expected outcome": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMER_EXPECTED_OUTCOME", "drop")
		}, want: "must be recovered, dropped, or compacted"},
		"dropped outcome requires slow metrics": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMER_EXPECTED_OUTCOME", "dropped")
		}, want: "requires SLOW_CONSUMER=true and REQUIRE_SLOW_CONSUMER_OUTCOMES=true"},
		"compacted outcome requires slow metrics": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMER_EXPECTED_OUTCOME", "compacted")
		}, want: "requires SLOW_CONSUMER=true and REQUIRE_SLOW_CONSUMER_OUTCOMES=true"},
		"outcomes require slow": {mutate: func(t *testing.T) {
			t.Setenv("REQUIRE_SLOW_CONSUMER_OUTCOMES", "true")
		}, want: "requires SLOW_CONSUMER=true"},
		"outcomes require info": {mutate: func(t *testing.T) {
			t.Setenv("SLOW_CONSUMER", "true")
			t.Setenv("REQUIRE_SLOW_CONSUMER_OUTCOMES", "true")
		}, want: "INFO_ENDPOINT is required"},
		"unsafe info URL": {mutate: func(t *testing.T) {
			t.Setenv("INFO_ENDPOINT", "https://user:pass@example.invalid/metrics")
		}, want: "INFO_ENDPOINT must be"},
		"partial info identity": {mutate: func(t *testing.T) { t.Setenv("INFO_CERT_FILE", "client.crt") }, want: "must be set together"},
		"noncanonical cleanup timeout": {mutate: func(t *testing.T) {
			t.Setenv("CLEANUP_TIMEOUT_SECONDS", "0300")
		}, want: "CLEANUP_TIMEOUT_SECONDS must be"},
	} {
		t.Run(name, func(t *testing.T) {
			setValidEnvironment(t)
			testCase.mutate(t)
			_, err := configFromEnvironment()
			require.ErrorContains(t, err, testCase.want)
		})
	}
}

func TestConfigAllowsExplicitCleanupOnlyRecovery(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("CLEANUP_ONLY", "true")
	cfg, err := configFromEnvironment()
	require.NoError(t, err)
	require.True(t, cfg.cleanupOnly)
}

func TestConfigSlowConsumerCountsTowardObservationBound(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("WATCHERS", "1")
	t.Setenv("EVENTS", "10000000")
	t.Setenv("SLOW_CONSUMER", "true")
	cfg, err := configFromEnvironment()
	require.NoError(t, err)
	require.True(t, cfg.slowConsumer)
	require.Equal(t, 1, cfg.slowConsumers)

	t.Setenv("WATCHERS", "2")
	_, err = configFromEnvironment()
	require.ErrorContains(t, err, "(WATCHERS+SLOW_CONSUMERS)*EVENTS")
}

func TestConfigMultipleSlowConsumersCountTowardObservationBound(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("WATCHERS", "1")
	t.Setenv("EVENTS", "5000000")
	t.Setenv("SLOW_CONSUMER", "true")
	t.Setenv("SLOW_CONSUMERS", "3")
	cfg, err := configFromEnvironment()
	require.NoError(t, err)
	require.Equal(t, 3, cfg.slowConsumers)

	t.Setenv("SLOW_CONSUMERS", "4")
	_, err = configFromEnvironment()
	require.ErrorContains(t, err, "(WATCHERS+SLOW_CONSUMERS)*EVENTS")
}

func TestConfigAllowsExplicitDroppedSlowConsumerOutcome(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("SLOW_CONSUMER", "true")
	t.Setenv("REQUIRE_SLOW_CONSUMER_OUTCOMES", "true")
	t.Setenv("SLOW_CONSUMER_EXPECTED_OUTCOME", "dropped")
	t.Setenv("INFO_ENDPOINT", "https://127.0.0.1:18082/metrics")
	cfg, err := configFromEnvironment()
	require.NoError(t, err)
	require.Equal(t, slowConsumerExpectedDropped, cfg.slowConsumerExpectedOutcome)
}

func TestConfigAllowsExplicitCompactedSlowConsumerOutcome(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("SLOW_CONSUMER", "true")
	t.Setenv("REQUIRE_SLOW_CONSUMER_OUTCOMES", "true")
	t.Setenv("SLOW_CONSUMER_EXPECTED_OUTCOME", "compacted")
	t.Setenv("INFO_ENDPOINT", "https://127.0.0.1:18082/metrics")
	cfg, err := configFromEnvironment()
	require.NoError(t, err)
	require.Equal(t, slowConsumerExpectedCompacted, cfg.slowConsumerExpectedOutcome)
}

func TestValidateSlowConsumerCompactResponse(t *testing.T) {
	require.NoError(t, validateSlowConsumerCompactResponse(9, &clientv3.CompactResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 10},
	}, nil))
	require.ErrorContains(t, validateSlowConsumerCompactResponse(9, nil, nil), "invalid response")
	require.ErrorContains(t, validateSlowConsumerCompactResponse(9, &clientv3.CompactResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 8},
	}, nil), "invalid response")
	require.ErrorContains(t, validateSlowConsumerCompactResponse(9, nil, errors.New("injected compact failure")),
		"injected compact failure")
}

func TestValidateEventRequiresExactIdentityAndEtcdCreateShape(t *testing.T) {
	prefix := "/registry/watch-soak/test/"
	event := &clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{
		Key: []byte(expectedKey(prefix, 3)), Value: []byte(expectedValue(3)),
		CreateRevision: 9, ModRevision: 9, Version: 1,
	}}
	require.NoError(t, validateEvent(prefix, 3, event))

	copy := proto.Clone(event.Kv).(*mvccpb.KeyValue)
	copy.Value = []byte("duplicate-or-wrong")
	require.ErrorContains(t, validateEvent(prefix, 3, &clientv3.Event{Type: mvccpb.PUT, Kv: copy}), "event 3 mismatch")
	copy = proto.Clone(event.Kv).(*mvccpb.KeyValue)
	copy.ModRevision++
	require.ErrorContains(t, validateEvent(prefix, 3, &clientv3.Event{Type: mvccpb.PUT, Kv: copy}), "event 3 mismatch")
	require.ErrorContains(t, validateEvent(prefix, 3, &clientv3.Event{Type: mvccpb.DELETE, Kv: event.Kv}), "not a Put")
}

func TestValidateObservedEventsRejectsLossDuplicationAndReordering(t *testing.T) {
	written := []eventObservation{{index: 2, revision: 7}, {index: 0, revision: 9}, {index: 1, revision: 12}}
	require.NoError(t, validateObservedEvents(written, written))
	require.ErrorContains(t, validateObservedEvents(written[:2], written), "event count mismatch")
	require.ErrorContains(t, validateObservedEvents(
		[]eventObservation{{index: 2, revision: 7}, {index: 2, revision: 7}, {index: 1, revision: 12}}, written,
	), "event position 1 mismatch")
	require.ErrorContains(t, validateObservedEvents(
		[]eventObservation{{index: 2, revision: 7}, {index: 1, revision: 12}, {index: 0, revision: 9}}, written,
	), "event position 1 mismatch")
}

func TestWriteEventsSortsConcurrentResultsByCommittedRevision(t *testing.T) {
	prefix := "/registry/watch-soak/concurrent/"
	observed, err := writeEvents(context.Background(), prefix, 8, 4, 0, func(_ context.Context, key, value string) (*clientv3.PutResponse, error) {
		indexText := strings.TrimPrefix(key, prefix+"event-")
		index, parseErr := strconv.Atoi(indexText)
		if parseErr != nil {
			return nil, fmt.Errorf("parse event index %q: %w", indexText, parseErr)
		}
		if value != expectedValue(index) {
			return nil, fmt.Errorf("event %d value mismatch: got=%q want=%q", index, value, expectedValue(index))
		}
		return &clientv3.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: int64(100 - index)}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, []eventObservation{
		{index: 7, revision: 93}, {index: 6, revision: 94}, {index: 5, revision: 95}, {index: 4, revision: 96},
		{index: 3, revision: 97}, {index: 2, revision: 98}, {index: 1, revision: 99}, {index: 0, revision: 100},
	}, observed)
}

func TestWriteEventsCancelsOutstandingWritesAfterFirstFailure(t *testing.T) {
	started := make(chan struct{}, 8)
	_, err := writeEvents(context.Background(), "/registry/watch-soak/failure/", 8, 2, 0,
		func(ctx context.Context, key, _ string) (*clientv3.PutResponse, error) {
			started <- struct{}{}
			if key == expectedKey("/registry/watch-soak/failure/", 0) {
				return nil, errors.New("injected write failure")
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
				return &clientv3.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
			}
		})
	require.ErrorContains(t, err, "injected write failure")
	require.NotEmpty(t, started, "the bounded group must start at least one companion write")
}

func TestWriteEventsRejectsDuplicateCommittedRevision(t *testing.T) {
	_, err := writeEvents(context.Background(), "/registry/watch-soak/duplicate/", 2, 2, 0,
		func(context.Context, string, string) (*clientv3.PutResponse, error) {
			return &clientv3.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 9}}, nil
		})
	require.ErrorContains(t, err, "revisions are not unique and increasing")
}

func TestWatchSoakProgressIsBoundedAndUseful(t *testing.T) {
	require.Equal(t, 1000, watchSoakProgressEvery(50))
	require.Equal(t, 1000, watchSoakProgressEvery(12000))
	require.Equal(t, 8000, watchSoakProgressEvery(800000))
}

func TestCleanupDeleteOperationsAreExactAndPrefixBound(t *testing.T) {
	prefix := "/registry/watch-soak/owned/"
	kvs := make([]*mvccpb.KeyValue, 128)
	for index := range kvs {
		kvs[index] = &mvccpb.KeyValue{Key: []byte(expectedKey(prefix, index))}
	}
	operations, err := cleanupDeleteOperations(prefix, kvs)
	require.NoError(t, err)
	require.Len(t, operations, 128)
	for index, operation := range operations {
		require.True(t, operation.IsDelete())
		require.Equal(t, expectedKey(prefix, index), string(operation.KeyBytes()))
		require.Empty(t, operation.RangeBytes(), "cleanup must issue exact-key deletes")
	}

	_, err = cleanupDeleteOperations(prefix, []*mvccpb.KeyValue{{Key: []byte("/registry/watch-soak/other/event")}})
	require.ErrorContains(t, err, "out-of-prefix")
	_, err = cleanupDeleteOperations(prefix, []*mvccpb.KeyValue{nil})
	require.ErrorContains(t, err, "out-of-prefix")
	_, err = cleanupDeleteOperations(prefix, nil)
	require.ErrorContains(t, err, "invalid batch size")
	tooMany := append(kvs, &mvccpb.KeyValue{Key: []byte(expectedKey(prefix, len(kvs)))})
	_, err = cleanupDeleteOperations(prefix, tooMany)
	require.ErrorContains(t, err, "invalid batch size")
}

func TestWrapperRequiresExplicitMutationApprovalAndUsesRepositoryCommand(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "watch-soak.sh"))
	require.NoError(t, err)
	text := string(script)
	require.Contains(t, text, "ALLOW_MUTATING_WATCH_SOAK=true")
	require.Contains(t, text, "go run ./hack/dev/cmd/watch-soak")
	require.Contains(t, text, `CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-300}"`)
	require.Contains(t, text, `WRITE_CONCURRENCY="${WRITE_CONCURRENCY:-1}"`)
	require.Contains(t, text, `WRITE_CONCURRENCY="$WRITE_CONCURRENCY"`)
	require.Contains(t, text, `CLEANUP_ONLY="${CLEANUP_ONLY:-false}"`)
	require.Contains(t, text, `CLEANUP_ONLY="$CLEANUP_ONLY"`)
	require.Contains(t, text, `SLOW_CONSUMER="${SLOW_CONSUMER:-false}"`)
	require.Contains(t, text, `SLOW_CONSUMERS="${SLOW_CONSUMERS:-}"`)
	require.Contains(t, text, `SLOW_CONSUMERS="$SLOW_CONSUMERS"`)
	require.Contains(t, text, `REQUIRE_SLOW_CONSUMER_OUTCOMES="${REQUIRE_SLOW_CONSUMER_OUTCOMES:-false}"`)
	require.Contains(t, text, `SLOW_CONSUMER_EXPECTED_OUTCOME="${SLOW_CONSUMER_EXPECTED_OUTCOME:-recovered}"`)
	require.Contains(t, text, `SLOW_CONSUMER_EXPECTED_OUTCOME="$SLOW_CONSUMER_EXPECTED_OUTCOME"`)
	require.NotContains(t, text, "go get")
	require.NotContains(t, text, "rm -rf")
	verify, err := os.ReadFile(filepath.Join("..", "..", "verify.sh"))
	require.NoError(t, err)
	require.Contains(t, string(verify), "ALLOW_MUTATING_WATCH_SOAK=true")
}

func TestWrapperRejectsMutationWithoutApprovalBeforeRunningGo(t *testing.T) {
	script := filepath.Join("..", "..", "watch-soak.sh")
	command := exec.Command("bash", script)
	command.Env = append(os.Environ(),
		"ALLOW_MUTATING_WATCH_SOAK=false",
		"RUN_ID=unauthorized-watch-soak",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "refusing shared-endpoint watch writes")
	require.NotContains(t, strings.ToLower(string(output)), "missing required command: go")
}
