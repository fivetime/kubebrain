package tikv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type primaryTrackingClientFunc struct {
	clienttikv.Client
	send func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error)
}

func (c *primaryTrackingClientFunc) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	return c.send(ctx, addr, req, timeout)
}

func TestPrimaryTrackingClientPreservesTransportAndRegistersBeforeSend(t *testing.T) {
	for _, failure := range []bool{false, true} {
		tracker := &primaryWriteTracker{}
		ctx := context.WithValue(t.Context(), primaryWriteTrackerKey{}, tracker)
		var wantErr error
		if failure {
			wantErr = errors.New("unavailable")
		}
		prewrite := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 7, PrimaryLock: []byte("p")})
		commit := tikvrpc.NewRequest(tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{StartVersion: 7, Keys: [][]byte{[]byte("p")}})
		response := &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}
		calls := 0
		stub := &primaryTrackingClientFunc{send: func(got context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			calls++
			require.Same(t, ctx, got)
			require.Equal(t, "unchanged-address", addr)
			require.Equal(t, time.Second, timeout)
			tracker.mu.Lock()
			require.EqualValues(t, 7, tracker.start, "identity must be registered before delivery")
			tracker.mu.Unlock()
			if calls == 1 {
				require.Same(t, prewrite, req)
			} else {
				require.Same(t, commit, req)
			}
			return response, wantErr
		}}
		client := &writeResponseClient{Client: stub, metrics: newWriteResponseMetrics(prometheus.NewRegistry())}
		for _, req := range []*tikvrpc.Request{prewrite, commit} {
			got, err := client.SendRequest(ctx, "unchanged-address", req, time.Second)
			require.Same(t, response, got)
			require.Equal(t, wantErr, err)
		}
		require.Equal(t, 2, calls, "instrumentation must never retry")
		if failure {
			require.Zero(t, tracker.finish().SuccessfulRPCs)
		} else {
			require.EqualValues(t, 1, tracker.finish().SuccessfulRPCs)
		}
	}
}
