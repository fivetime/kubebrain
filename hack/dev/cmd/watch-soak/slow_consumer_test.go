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

func (stream *scriptedRawWatchStream) Send(*etcdserverpb.WatchRequest) error { return nil }

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
	revisions, err := watch.consume()
	require.NoError(t, err)
	require.Equal(t, []int64{7, 8, 9}, revisions)
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
	return strings.NewReplacer("CATCH", fmt.Sprint(catchUp), "RECOVERED", fmt.Sprint(recovered), "DROPPED", fmt.Sprint(dropped)).Replace(`# HELP watcher_hub_slow_consumer_outcome_total slow outcomes
# TYPE watcher_hub_slow_consumer_outcome_total counter
watcher_hub_slow_consumer_outcome_total{cluster="default",outcome="catch_up"} CATCH
watcher_hub_slow_consumer_outcome_total{cluster="default",outcome="recovered"} RECOVERED
watcher_hub_slow_consumer_outcome_total{cluster="default",outcome="dropped"} DROPPED
`)
}

func TestMetricsReaderParsesAllFixedOutcomes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, metricsText(3, 2, 1))
	}))
	defer server.Close()
	cfg := config{infoEndpoint: server.URL + "/metrics"}
	reader, err := newSlowConsumerMetricsReader(cfg)
	require.NoError(t, err)
	defer reader.close()
	outcomes, err := reader.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, slowConsumerOutcomes{catchUp: 3, recovered: 2, dropped: 1}, outcomes)
}

func TestMetricsReaderRejectsMissingAndDuplicateOutcomes(t *testing.T) {
	for name, body := range map[string]string{
		"missing": `# TYPE watcher_hub_slow_consumer_outcome_total counter
watcher_hub_slow_consumer_outcome_total{outcome="catch_up"} 1
`,
		"duplicate": metricsText(1, 0, 0) + `watcher_hub_slow_consumer_outcome_total{cluster="other",outcome="catch_up"} 1
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
	require.ErrorContains(t, err, "invalid outcome")
	reader.close()

	reader, err = newSlowConsumerMetricsReader(config{infoEndpoint: server.URL + "/redirect"})
	require.NoError(t, err)
	_, err = reader.fetch(context.Background())
	require.ErrorContains(t, err, "redirects are not allowed")
	reader.close()
}
