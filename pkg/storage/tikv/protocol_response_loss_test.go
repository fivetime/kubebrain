package tikv

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type protocolCommitMarker struct{}

type protocolLossStats struct {
	Attempts, Drops   int
	StartTS, CommitTS uint64
	Changed           bool
}

// Wraps only this test process's client. By default it drops a real successful
// 1PC response; beforeDelivery instead interrupts one marked 1PC before sending.
// No server or network configuration is changed.
type protocolResponseLoss struct {
	clienttikv.Client
	beforeDelivery bool
	// Optional cancellation is scoped to the marked caller at the selected
	// fault point. It prevents retry confirmation without a global failpoint.
	cancelAfterLoss context.CancelFunc
	mu              sync.Mutex
	stats           protocolLossStats
}

func (c *protocolResponseLoss) snapshot() protocolLossStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *protocolResponseLoss) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	marked := ctx.Value(protocolCommitMarker{}) == true && req.Type == tikvrpc.CmdPrewrite
	if marked {
		c.mu.Lock()
		c.stats.Attempts++
		if c.stats.StartTS == 0 {
			c.stats.StartTS = req.Prewrite().StartVersion
		}
		c.stats.Changed = c.stats.Changed || c.stats.StartTS != req.Prewrite().StartVersion
		if c.beforeDelivery && req.Prewrite().TryOnePc && c.stats.Drops == 0 {
			c.stats.Drops++
			if c.cancelAfterLoss != nil {
				c.cancelAfterLoss()
			}
			c.mu.Unlock()
			return nil, errors.New("injected response loss before 1PC delivery")
		}
		c.mu.Unlock()
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if !marked || err != nil || response == nil {
		return response, err
	}
	r, ok := response.Resp.(*kvrpcpb.PrewriteResponse)
	if !ok || r.RegionError != nil || len(r.Errors) != 0 || r.OnePcCommitTs == 0 {
		return response, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stats.CommitTS == 0 {
		c.stats.CommitTS = r.OnePcCommitTs
	}
	c.stats.Changed = c.stats.Changed || c.stats.CommitTS != r.OnePcCommitTs
	if c.stats.Drops == 0 {
		c.stats.Drops++
		if c.cancelAfterLoss != nil {
			c.cancelAfterLoss()
		}
		return nil, errors.New("injected loss of successful real 1PC response")
	}
	return response, err
}

type protocolResponseStub struct {
	clienttikv.Client
	response *tikvrpc.Response
	err      error
	calls    int
}

func (s *protocolResponseStub) SendRequest(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
	s.calls++
	return s.response, s.err
}

func TestProtocolBeforeDeliveryCancellationScope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), protocolCommitMarker{}, true))
	defer cancel()
	stub := &protocolResponseStub{response: &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}}
	loss := &protocolResponseLoss{Client: stub, beforeDelivery: true, cancelAfterLoss: cancel}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 10, TryOnePc: true})
	_, err := loss.SendRequest(context.Background(), "unused", req, time.Second)
	require.NoError(t, err)
	require.NoError(t, ctx.Err())
	require.Equal(t, 1, stub.calls)
	req.Prewrite().TryOnePc = false
	_, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	require.NoError(t, ctx.Err(), "ordinary 2PC must not consume the fault")
	require.Equal(t, 2, stub.calls)
	req.Prewrite().TryOnePc = true
	_, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.ErrorContains(t, err, "before 1PC delivery")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, 2, stub.calls, "faulted prewrite must never reach the transport")
	require.Equal(t, protocolLossStats{Attempts: 2, Drops: 1, StartTS: 10}, loss.snapshot())
}

func TestProtocolResponseLossScope(t *testing.T) {
	response := &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{OnePcCommitTs: 20}}
	stub := &protocolResponseStub{response: response}
	loss := &protocolResponseLoss{Client: stub}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 10, TryOnePc: true})
	ctx := context.WithValue(context.Background(), protocolCommitMarker{}, true)
	got, err := loss.SendRequest(context.Background(), "unused", req, time.Second)
	require.NoError(t, err)
	require.Same(t, response, got)
	require.Zero(t, loss.snapshot().Drops, "unmarked background requests must not consume the fault")
	stub.err = errors.New("original transport error")
	_, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.ErrorIs(t, err, stub.err)
	require.Zero(t, loss.snapshot().Drops)
	stub.err = nil
	stub.response = &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
	_, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	require.Zero(t, loss.snapshot().Drops, "2PC prewrite is not a committed 1PC response")
	stub.response = response
	got, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.ErrorContains(t, err, "injected loss")
	require.Nil(t, got)
	got, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	require.Same(t, response, got)
	require.Equal(t, protocolLossStats{Attempts: 4, Drops: 1, StartTS: 10, CommitTS: 20}, loss.snapshot())
}

func TestRealTiKVOnePCResponseLoss(t *testing.T) {
	testRealTiKVProtocolSmoke(t, true, false)
}

func TestRealTiKVOnePCCancelAfterResponseLoss(t *testing.T) {
	testRealTiKVProtocolSmoke(t, true, true)
}

func TestProtocolResponseLossCancellationScope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), protocolCommitMarker{}, true))
	defer cancel()
	stub := &protocolResponseStub{response: &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}}
	calls := 0
	loss := &protocolResponseLoss{Client: stub, cancelAfterLoss: func() { calls++; cancel() }}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 10, TryOnePc: true})
	_, err := loss.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	require.NoError(t, ctx.Err(), "ordinary prewrite must not cancel the caller")
	stub.response = &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{OnePcCommitTs: 20}}
	_, err = loss.SendRequest(context.Background(), "unused", req, time.Second)
	require.NoError(t, err)
	require.NoError(t, ctx.Err(), "background requests must not cancel the caller")
	_, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.ErrorContains(t, err, "injected loss")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	_, err = loss.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	require.Equal(t, 1, calls, "cancellation must occur exactly once")
}
