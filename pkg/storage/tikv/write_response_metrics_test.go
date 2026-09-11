package tikv

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestWriteResponseMetricsPresenceAndOutcomes(t *testing.T) {
	withWrite := &kvrpcpb.ExecDetailsV2{WriteDetail: &kvrpcpb.WriteDetail{PersistLogNanos: 2e6}}
	for _, tc := range []struct {
		name, outcome, presence string
		response                *kvrpcpb.CommitResponse
		err                     error
	}{
		{"transport", "transport_error", "absent", nil, errors.New("private transport text")},
		{"missing", "response_missing", "absent", nil, nil},
		{"no_details", "success", "absent", &kvrpcpb.CommitResponse{}, nil},
		{"exec_only", "success", "exec_only", &kvrpcpb.CommitResponse{ExecDetailsV2: &kvrpcpb.ExecDetailsV2{}}, nil},
		{"write", "success", "write", &kvrpcpb.CommitResponse{ExecDetailsV2: withWrite}, nil},
		{"region", "region_error", "write", &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{}, ExecDetailsV2: withWrite}, nil},
		{"key", "key_error", "write", &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}, ExecDetailsV2: withWrite}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := newWriteResponseMetrics(reg)
			var response *tikvrpc.Response
			if tc.response != nil {
				response = &tikvrpc.Response{Resp: tc.response}
			}
			stub := &protocolResponseStub{response: response, err: tc.err}
			c := &writeResponseClient{Client: stub, metrics: m}
			got, err := c.SendRequest(context.Background(), "private-address", tikvrpc.NewRequest(tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{}), time.Second)
			require.True(t, got == response && err == tc.err, "no response/error wrapping or replacement")
			require.Equal(t, 1, stub.calls, "no retry")
			families, err := reg.Gather()
			require.NoError(t, err)
			stages := 0
			for _, family := range families {
				for _, metric := range family.Metric {
					labels := map[string]string{}
					for _, label := range metric.Label {
						labels[label.GetName()] = label.GetValue()
					}
					if family.GetName() == "kubebrain_tikv_write_response_total" {
						require.Equal(t, map[string]string{"method": "commit", "outcome": tc.outcome, "details": tc.presence}, labels)
						require.Equal(t, float64(1), metric.Counter.GetValue())
					} else {
						stages++
						require.EqualValues(t, 1, metric.Histogram.GetSampleCount())
						if labels["stage"] == "persist_log" {
							require.Equal(t, 0.002, metric.Histogram.GetSampleSum())
						}
					}
				}
			}
			want := 0
			if tc.outcome == "success" && tc.presence == "write" {
				want = 10
			}
			require.Equal(t, want, stages, "absent or failed details must never become zero duration samples")
		})
	}
}

func TestWriteResponseMetricsConcurrentAndNonWrite(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newWriteResponseMetrics(reg)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.observe("prewrite", &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{ExecDetailsV2: &kvrpcpb.ExecDetailsV2{WriteDetail: &kvrpcpb.WriteDetail{}}}}, nil, time.Millisecond)
		}()
	}
	wg.Wait()
	stub := &protocolResponseStub{}
	c := &writeResponseClient{Client: stub, metrics: m}
	_, err := c.SendRequest(t.Context(), "unused", tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}), time.Second)
	require.NoError(t, err)
	require.Equal(t, 1, stub.calls)
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		for _, metric := range f.Metric {
			if metric.Histogram != nil {
				require.EqualValues(t, 20, metric.Histogram.GetSampleCount())
			}
			if metric.Counter != nil {
				require.Equal(t, float64(20), metric.Counter.GetValue())
			}
		}
	}
}
