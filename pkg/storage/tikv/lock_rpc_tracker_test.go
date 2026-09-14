package tikv

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestLockRPCTrackerScopeAndClosure(t *testing.T) {
	prepare, commit := &lockRPCTracker{}, &lockRPCTracker{}
	prepare.observe(tikvrpc.CmdCheckTxnStatus, time.Millisecond, nil)
	prepare.observe(tikvrpc.CmdResolveLock, 2*time.Millisecond, errors.New("private"))
	prepare.observe(tikvrpc.CmdGet, time.Second, nil)
	prepare.observe(tikvrpc.CmdResolveLock, -1, nil)
	want := storage.LockRPCObservation{
		CheckTxnStatus: storage.LockRPCSample{Requests: 1, Duration: time.Millisecond},
		ResolveLock:    storage.LockRPCSample{Requests: 1, TransportErrors: 1, Duration: 2 * time.Millisecond},
	}
	require.Equal(t, want, prepare.finish())
	prepare.observe(tikvrpc.CmdResolveLock, time.Second, nil)
	commit.observe(tikvrpc.CmdResolveLock, 3*time.Millisecond, nil)
	require.Equal(t, want, prepare.finish(), "late preparation is excluded, not moved to commit")
	require.Equal(t, storage.LockRPCSample{Requests: 1, Duration: 3 * time.Millisecond}, commit.finish().ResolveLock)
}

func TestLockRPCTrackerConcurrentClose(t *testing.T) {
	tracker := &lockRPCTracker{}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); tracker.observe(tikvrpc.CmdResolveLock, time.Millisecond, nil) }()
	}
	frozen := tracker.finish()
	wg.Wait()
	require.Equal(t, frozen, tracker.finish())
}

func TestLockRPCTransportIdentityAndUnmarkedExclusion(t *testing.T) {
	for _, method := range []tikvrpc.CmdType{tikvrpc.CmdCheckTxnStatus, tikvrpc.CmdResolveLock} {
		for _, transportErr := range []error{nil, errors.New("private transport error")} {
			response := &tikvrpc.Response{Resp: &kvrpcpb.ResolveLockResponse{}}
			stub := &protocolResponseStub{response: response, err: transportErr}
			client := &writeResponseClient{Client: stub}
			tracker := &lockRPCTracker{}
			ctx := context.WithValue(context.Background(), lockRPCTrackerKey{}, tracker)
			request := &tikvrpc.Request{Type: method}
			got, err := client.SendRequest(context.Background(), "unused", request, time.Second)
			require.Same(t, response, got)
			require.True(t, err == transportErr)
			got, err = client.SendRequest(ctx, "unused", request, time.Second)
			require.Same(t, response, got)
			require.True(t, err == transportErr)
			observed := tracker.finish()
			sample := observed.ResolveLock
			if method == tikvrpc.CmdCheckTxnStatus {
				sample = observed.CheckTxnStatus
			}
			require.EqualValues(t, 1, sample.Requests)
			if transportErr != nil {
				require.EqualValues(t, 1, sample.TransportErrors)
			} else {
				require.Zero(t, sample.TransportErrors)
			}
			_, _ = client.SendRequest(ctx, "unused", request, time.Second)
			require.Equal(t, observed, tracker.finish())
			require.Equal(t, 3, stub.calls, "no retries and no dropped forwarding")
		}
	}
}

func TestPrewriteRPCTransportIdentityAndUnmarkedExclusion(t *testing.T) {
	for _, transportErr := range []error{nil, errors.New("private transport error")} {
		response := &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
		stub := &protocolResponseStub{response: response, err: transportErr}
		client := &writeResponseClient{Client: stub, metrics: newWriteResponseMetrics(prometheus.NewRegistry())}
		tracker := &primaryWriteTracker{}
		ctx := context.WithValue(context.Background(), primaryWriteTrackerKey{}, tracker)
		request := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 123, PrimaryLock: []byte("primary")})
		got, err := client.SendRequest(context.Background(), "unused", request, time.Second)
		require.Same(t, response, got)
		require.True(t, err == transportErr)
		require.Zero(t, tracker.prewriteSnapshot().Requests)
		got, err = client.SendRequest(ctx, "unused", request, time.Second)
		require.Same(t, response, got)
		require.True(t, err == transportErr)
		tracker.finish()
		frozen := tracker.prewriteSnapshot()
		require.EqualValues(t, 1, frozen.Requests)
		if transportErr != nil {
			require.EqualValues(t, 1, frozen.TransportErrors)
		} else {
			require.Zero(t, frozen.TransportErrors)
		}
		_, _ = client.SendRequest(ctx, "unused", request, time.Second)
		require.Equal(t, frozen, tracker.prewriteSnapshot())
		require.Equal(t, 3, stub.calls)
	}
}
