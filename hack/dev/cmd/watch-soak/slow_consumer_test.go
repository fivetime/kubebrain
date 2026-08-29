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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func TestValidateDirectLeaderStatus(t *testing.T) {
	valid := &clientv3.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9}, Leader: 9,
	}
	require.NoError(t, validateDirectLeaderStatus(valid))
	require.ErrorContains(t, validateDirectLeaderStatus(nil), "incomplete leader identity")
	nonLeader := &clientv3.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9}, Leader: 10,
	}
	require.ErrorContains(t, validateDirectLeaderStatus(nonLeader), "must directly serve the leader")
}

type scriptedRawWatchStream struct {
	grpc.ClientStream
	responses []*etcdserverpb.WatchResponse
}

type blockingRawWatchStream struct {
	grpc.ClientStream
	done <-chan struct{}
}

func (stream *blockingRawWatchStream) Send(*etcdserverpb.WatchRequest) error { return nil }
func (stream *blockingRawWatchStream) CloseSend() error                      { return nil }

func (stream *blockingRawWatchStream) Recv() (*etcdserverpb.WatchResponse, error) {
	<-stream.done
	return nil, context.Canceled
}

func (stream *scriptedRawWatchStream) Send(*etcdserverpb.WatchRequest) error { return nil }
func (stream *scriptedRawWatchStream) CloseSend() error                      { return nil }

func (stream *scriptedRawWatchStream) Recv() (*etcdserverpb.WatchResponse, error) {
	if len(stream.responses) == 0 {
		return nil, io.EOF
	}
	response := stream.responses[0]
	stream.responses = stream.responses[1:]
	return response, nil
}

func rawResponse(revision int64, events ...*mvccpb.Event) *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9, Revision: revision},
		WatchId: slowConsumerWatchID, Events: events,
	}
}

func TestRawSlowWatchConsumesExactEventsAcrossProgressAndBatches(t *testing.T) {
	prefix := "/registry/watch-soak/slow/"
	event := func(index int, revision int64) *mvccpb.Event {
		return &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{
			Key: []byte(expectedKey(prefix, index)), Value: []byte(expectedValue(index)),
			CreateRevision: revision, ModRevision: revision, Version: 1,
		}}
	}
	watch := &rawSlowWatch{
		stream: &scriptedRawWatchStream{responses: []*etcdserverpb.WatchResponse{
			rawResponse(6), rawResponse(8, event(0, 7), event(1, 8)), rawResponse(9, event(2, 9)),
		}},
		prefix: prefix, eventCount: 3, clusterID: 7, memberID: 9,
	}
	observations, err := watch.consume()
	require.NoError(t, err)
	require.Equal(t, []eventObservation{{index: 0, revision: 7}, {index: 1, revision: 8}, {index: 2, revision: 9}}, observations)
}

func TestMultipleRawSlowWatchesConsumeAndValidateConcurrently(t *testing.T) {
	prefix := "/registry/watch-soak/multiple-slow/"
	written := []eventObservation{{index: 0, revision: 7}, {index: 1, revision: 8}}
	events := func() []*etcdserverpb.WatchResponse {
		return []*etcdserverpb.WatchResponse{
			rawResponse(6),
			rawResponse(8,
				&mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(expectedKey(prefix, 0)), Value: []byte(expectedValue(0)), CreateRevision: 7, ModRevision: 7, Version: 1}},
				&mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(expectedKey(prefix, 1)), Value: []byte(expectedValue(1)), CreateRevision: 8, ModRevision: 8, Version: 1}},
			),
		}
	}
	watches := []*rawSlowWatch{
		{stream: &scriptedRawWatchStream{responses: events()}, prefix: prefix, eventCount: 2, clusterID: 7, memberID: 9},
		{stream: &scriptedRawWatchStream{responses: events()}, prefix: prefix, eventCount: 2, clusterID: 7, memberID: 9},
		{stream: &scriptedRawWatchStream{responses: events()}, prefix: prefix, eventCount: 2, clusterID: 7, memberID: 9},
	}
	require.NoError(t, consumeRawSlowWatches(watches, written))
}

func TestMultipleRawSlowWatchesCancelAndJoinPeersAfterFailure(t *testing.T) {
	done := make(chan struct{})
	watches := []*rawSlowWatch{
		{
			stream: &scriptedRawWatchStream{responses: []*etcdserverpb.WatchResponse{{
				Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 10, Revision: 8}, WatchId: slowConsumerWatchID,
			}}},
			prefix: "/registry/watch-soak/failing-slow/", eventCount: 1, clusterID: 7, memberID: 9,
		},
		{
			stream: &blockingRawWatchStream{done: done}, cancel: func() { close(done) },
			prefix: "/registry/watch-soak/failing-slow/", eventCount: 1, clusterID: 7, memberID: 9,
		},
	}
	err := consumeRawSlowWatches(watches, []eventObservation{{index: 0, revision: 8}})
	require.ErrorContains(t, err, "raw slow-consumer watcher 0")
	require.ErrorContains(t, err, "identity mismatch")
	select {
	case <-done:
	default:
		t.Fatal("peer raw watcher was not canceled before join returned")
	}
}

func TestRawSlowWatchRejectsIdentityDriftAndDuplicateCreated(t *testing.T) {
	for name, response := range map[string]*etcdserverpb.WatchResponse{
		"member drift":      {Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 10, Revision: 8}, WatchId: slowConsumerWatchID},
		"duplicate created": {Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9, Revision: 8}, WatchId: slowConsumerWatchID, Created: true},
	} {
		t.Run(name, func(t *testing.T) {
			watch := &rawSlowWatch{stream: &scriptedRawWatchStream{responses: []*etcdserverpb.WatchResponse{response}},
				prefix: "/registry/watch-soak/slow/", eventCount: 1, clusterID: 7, memberID: 9}
			_, err := watch.consume()
			require.Error(t, err)
		})
	}
}

func metricsText(catchUp, recovered, dropped int) string {
	return metricsTextWithGeneration(catchUp, recovered, dropped, 0, 0, 0, 0)
}

func metricsTextWithGeneration(catchUp, recovered, dropped, retry, generationRecovered, compacted, failed int) string {
	return strings.NewReplacer(
		"CATCH", fmt.Sprint(catchUp), "SLOW_RECOVERED", fmt.Sprint(recovered), "DROPPED", fmt.Sprint(dropped),
		"RETRY", fmt.Sprint(retry), "GEN_RECOVERED", fmt.Sprint(generationRecovered),
		"COMPACTED", fmt.Sprint(compacted), "FAILED", fmt.Sprint(failed),
	).Replace(`# HELP watcher_hub_slow_consumer_outcome_total slow outcomes
# TYPE watcher_hub_slow_consumer_outcome_total counter
watcher_hub_slow_consumer_outcome_total{cluster="default",outcome="catch_up"} CATCH
watcher_hub_slow_consumer_outcome_total{cluster="default",outcome="recovered"} SLOW_RECOVERED
watcher_hub_slow_consumer_outcome_total{cluster="default",outcome="dropped"} DROPPED
# HELP watch_generation_recovery_total generation outcomes
# TYPE watch_generation_recovery_total counter
watch_generation_recovery_total{cluster="default",outcome="retry"} RETRY
watch_generation_recovery_total{cluster="default",outcome="recovered"} GEN_RECOVERED
watch_generation_recovery_total{cluster="default",outcome="compacted"} COMPACTED
watch_generation_recovery_total{cluster="default",outcome="failed"} FAILED
`)
}

func TestMetricsReaderParsesAllFixedOutcomes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, metricsTextWithGeneration(3, 2, 1, 4, 5, 6, 7))
	}))
	defer server.Close()
	cfg := config{infoEndpoint: server.URL + "/metrics"}
	reader, err := newSlowConsumerMetricsReader(cfg)
	require.NoError(t, err)
	defer reader.close()
	outcomes, err := reader.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, watchOutcomes{
		slow:       slowConsumerOutcomes{catchUp: 3, recovered: 2, dropped: 1},
		generation: watchGenerationOutcomes{retry: 4, recovered: 5, compacted: 6, failed: 7},
	}, outcomes)
}

func TestMetricsReaderRejectsMissingAndDuplicateOutcomes(t *testing.T) {
	for name, body := range map[string]string{
		"missing": `# TYPE watcher_hub_slow_consumer_outcome_total counter
watcher_hub_slow_consumer_outcome_total{outcome="catch_up"} 1
`,
		"missing generation": strings.Split(metricsText(1, 0, 0), "# HELP watch_generation_recovery_total")[0],
		"duplicate": metricsText(1, 0, 0) + `watcher_hub_slow_consumer_outcome_total{cluster="other",outcome="catch_up"} 1
`,
		"unknown generation": metricsText(1, 0, 0) + `watch_generation_recovery_total{outcome="unknown"} 1
`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(response, body) }))
			defer server.Close()
			reader, err := newSlowConsumerMetricsReader(config{infoEndpoint: server.URL + "/metrics"})
			require.NoError(t, err)
			defer reader.close()
			_, err = reader.fetch(context.Background())
			require.Error(t, err)
		})
	}
}

func TestMetricsReaderRejectsInvalidCounterAndRedirect(t *testing.T) {
	invalid := strings.Replace(metricsText(1, 0, 0), `outcome="catch_up"} 1`, `outcome="catch_up"} NaN`, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirect" {
			http.Redirect(response, request, "/metrics", http.StatusFound)
			return
		}
		_, _ = io.WriteString(response, invalid)
	}))
	defer server.Close()

	reader, err := newSlowConsumerMetricsReader(config{infoEndpoint: server.URL + "/metrics"})
	require.NoError(t, err)
	_, err = reader.fetch(context.Background())
	require.ErrorContains(t, err, "invalid "+slowConsumerMetricName+" outcome")
	reader.close()

	reader, err = newSlowConsumerMetricsReader(config{infoEndpoint: server.URL + "/redirect"})
	require.NoError(t, err)
	_, err = reader.fetch(context.Background())
	require.ErrorContains(t, err, "redirects are not allowed")
	reader.close()
}

func TestExpectedWatchOutcomesDistinguishRingRecoveryAndEviction(t *testing.T) {
	baseline := watchOutcomes{
		slow:       slowConsumerOutcomes{catchUp: 3, recovered: 2, dropped: 1},
		generation: watchGenerationOutcomes{retry: 4, recovered: 5, compacted: 6, failed: 7},
	}

	recoveredEntered, err := expectedWatchOutcomes(baseline, slowConsumerExpectedRecovered, 3, false)
	require.NoError(t, err)
	require.Equal(t, watchOutcomes{
		slow:       slowConsumerOutcomes{catchUp: 6, recovered: 2, dropped: 1},
		generation: baseline.generation,
	}, recoveredEntered)
	recoveredComplete, err := expectedWatchOutcomes(baseline, slowConsumerExpectedRecovered, 3, true)
	require.NoError(t, err)
	require.Equal(t, watchOutcomes{
		slow:       slowConsumerOutcomes{catchUp: 6, recovered: 5, dropped: 1},
		generation: baseline.generation,
	}, recoveredComplete)

	droppedEntered, err := expectedWatchOutcomes(baseline, slowConsumerExpectedDropped, 3, false)
	require.NoError(t, err)
	require.Equal(t, watchOutcomes{
		slow:       slowConsumerOutcomes{catchUp: 6, recovered: 2, dropped: 1},
		generation: baseline.generation,
	}, droppedEntered)
	droppedComplete, err := expectedWatchOutcomes(baseline, slowConsumerExpectedDropped, 3, true)
	require.NoError(t, err)
	require.Equal(t, watchOutcomes{
		slow: slowConsumerOutcomes{catchUp: 6, recovered: 2, dropped: 4},
		generation: watchGenerationOutcomes{
			retry: 4, recovered: 8, compacted: 6, failed: 7,
		},
	}, droppedComplete)

	compacted := droppedComplete
	compacted.generation.compacted++
	require.True(t, watchOutcomesExceeded(compacted, droppedComplete),
		"a compacted terminal must fail an oracle expecting transparent generation recovery")
	_, err = expectedWatchOutcomes(baseline, "unknown", 3, true)
	require.ErrorContains(t, err, "unsupported")
	_, err = expectedWatchOutcomes(baseline, slowConsumerExpectedRecovered, 0, true)
	require.ErrorContains(t, err, "positive consumer count")
}

func TestDroppedOutcomeWaitsForExactGenerationRecovery(t *testing.T) {
	baseline := watchOutcomes{
		slow:       slowConsumerOutcomes{catchUp: 3, recovered: 2, dropped: 1},
		generation: watchGenerationOutcomes{retry: 4, recovered: 5, compacted: 6, failed: 7},
	}
	var body atomic.Value
	body.Store(metricsTextWithGeneration(4, 2, 1, 4, 5, 6, 7))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, body.Load().(string))
	}))
	defer server.Close()
	reader, err := newSlowConsumerMetricsReader(config{infoEndpoint: server.URL + "/metrics"})
	require.NoError(t, err)
	defer reader.close()

	require.NoError(t, waitForExpectedSlowConsumerPressure(
		context.Background(), reader, baseline, slowConsumerExpectedDropped, 1,
	))
	body.Store(metricsTextWithGeneration(4, 2, 2, 4, 6, 6, 7))
	require.NoError(t, waitForExpectedSlowConsumerCompletion(
		context.Background(), reader, baseline, slowConsumerExpectedDropped, 1,
	))

	body.Store(metricsTextWithGeneration(4, 2, 2, 4, 6, 7, 7))
	current, err := reader.fetch(context.Background())
	require.NoError(t, err)
	expected, err := expectedWatchOutcomes(baseline, slowConsumerExpectedDropped, 1, true)
	require.NoError(t, err)
	require.True(t, watchOutcomesExceeded(current, expected),
		"compaction must not satisfy the transparent recovery oracle")
}
