package tikv

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// Requires explicit split consent in addition to the ordinary cluster/prefix
// guards. Run only in a disposable cluster: Region boundaries outlive keys.
func TestRealTiKVBackendRegionSplitFallback(t *testing.T) {
	testRealTiKVBackendScenario(t, "split")
}

type protocolSplitStats struct {
	Splits, EpochErrors, Attempts, Commits int
	StartTS, CommitTS                      uint64
	Changed, OnePCCommitted                bool
	Regions                                int
}

type protocolRegionSplit struct {
	clienttikv.Client
	split   func(context.Context, []byte) ([]uint64, error)
	mu      sync.Mutex
	stats   protocolSplitStats
	regions map[uint64]bool
}

func (c *protocolRegionSplit) snapshot() protocolSplitStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Regions = len(c.regions)
	return s
}

func (c *protocolRegionSplit) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	marked := ctx.Value(protocolCommitMarker{}) == true
	var splitKey []byte
	if marked && req.Type == tikvrpc.CmdPrewrite {
		p := req.Prewrite()
		c.mu.Lock()
		c.stats.Attempts++
		if c.stats.StartTS == 0 {
			c.stats.StartTS = p.StartVersion
		}
		c.stats.Changed = c.stats.Changed || c.stats.StartTS != p.StartVersion
		if c.stats.Splits == 0 && p.TryOnePc && len(p.Mutations) > 1 {
			keys := make([][]byte, len(p.Mutations))
			for i, m := range p.Mutations {
				keys[i] = bytes.Clone(m.Key)
			}
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
			splitKey = keys[len(keys)/2]
			c.stats.Splits++
		}
		c.mu.Unlock()
	}
	if splitKey != nil {
		// SplitRegions sends RPCs through this same client: never hold our lock
		// across the callback. The original prewrite retains its old epoch.
		if _, err := c.split(ctx, splitKey); err != nil {
			return nil, err
		}
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if !marked || err != nil || response == nil {
		return response, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch r := response.Resp.(type) {
	case *kvrpcpb.PrewriteResponse:
		c.stats.OnePCCommitted = c.stats.OnePCCommitted || r.OnePcCommitTs != 0
		if r.RegionError.GetEpochNotMatch() != nil {
			c.stats.EpochErrors++
		}
		if r.RegionError == nil && len(r.Errors) == 0 && !req.Prewrite().TryOnePc {
			if c.regions == nil {
				c.regions = make(map[uint64]bool)
			}
			c.regions[req.Context.RegionId] = true
		}
	case *kvrpcpb.CommitResponse:
		if r.RegionError == nil && r.Error == nil {
			c.stats.Commits++
			p := req.Commit()
			if c.stats.CommitTS == 0 {
				c.stats.CommitTS = p.CommitVersion
			}
			c.stats.Changed = c.stats.Changed || c.stats.CommitTS != p.CommitVersion || c.stats.StartTS != p.StartVersion
		}
	}
	return response, err
}
