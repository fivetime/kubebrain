package tikv

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func newPrimaryTrackerFixture() (*primaryWriteTracker, *kvrpcpb.CommitRequest, *kvrpcpb.CommitResponse) {
	t := &primaryWriteTracker{}
	t.register(&kvrpcpb.PrewriteRequest{StartVersion: 123, PrimaryLock: []byte("primary")})
	return t, &kvrpcpb.CommitRequest{StartVersion: 123, Keys: [][]byte{[]byte("primary")}},
		&kvrpcpb.CommitResponse{ExecDetailsV2: &kvrpcpb.ExecDetailsV2{WriteDetail: &kvrpcpb.WriteDetail{PersistLogNanos: 10, RaftDbSyncLogNanos: 4, CommitLogNanos: 12}}}
}

func TestPrimaryWriteTrackerRejectsUnrelatedAndFailedResponses(t *testing.T) {
	for _, name := range []string{"secondary", "transaction", "transport", "region", "key", "missing", "negative"} {
		t.Run(name, func(t *testing.T) {
			tracker, req, resp := newPrimaryTrackerFixture()
			var err error
			duration := time.Millisecond
			switch name {
			case "secondary":
				req.Keys = [][]byte{[]byte("secondary")}
			case "transaction":
				req.StartVersion++
			case "transport":
				err = errors.New("undelivered")
			case "region":
				resp.RegionError = &errorpb.Error{}
			case "key":
				resp.Error = &kvrpcpb.KeyError{}
			case "missing":
				resp = nil
			case "negative":
				duration = -1
			}
			tracker.observe(req, resp, err, duration)
			require.Equal(t, primaryWriteSample{}, tracker.finish())
		})
	}
}

func TestPrimaryWriteTrackerPreservesRawPresenceOnSlowestSuccess(t *testing.T) {
	for _, name := range []string{"absent", "exec_only", "invalid_write", "write"} {
		t.Run(name, func(t *testing.T) {
			tracker, req, resp := newPrimaryTrackerFixture()
			tracker.observe(req, resp, nil, time.Millisecond)
			switch name {
			case "absent":
				resp.ExecDetailsV2 = nil
			case "exec_only":
				resp.ExecDetailsV2.WriteDetail = nil
			case "invalid_write":
				resp.ExecDetailsV2.WriteDetail.ApplyLogNanos = math.MaxUint64
			}
			tracker.observe(req, resp, nil, 2*time.Millisecond)
			sample := tracker.finish()
			require.EqualValues(t, 2, sample.SuccessfulRPCs)
			require.Equal(t, name, sample.Details)
			require.Equal(t, 2*time.Millisecond, sample.RPC)
			if name == "write" {
				require.Equal(t, 10*time.Nanosecond, sample.PersistLog)
			} else {
				require.Zero(t, sample.PersistLog, "never reuse a faster valid sample to conceal missing/invalid slowest detail")
			}
		})
	}
}

func TestPrimaryWriteTrackerIdentityAndFreeze(t *testing.T) {
	tracker, req, resp := newPrimaryTrackerFixture()
	tracker.observe(req, resp, nil, time.Millisecond)
	first := tracker.finish()
	tracker.observe(req, resp, nil, time.Second)
	tracker.register(&kvrpcpb.PrewriteRequest{StartVersion: 999, PrimaryLock: []byte("other")})
	require.Equal(t, first, tracker.finish())
	for _, changed := range []*kvrpcpb.PrewriteRequest{
		{StartVersion: 124, PrimaryLock: []byte("primary")},
		{StartVersion: 123, PrimaryLock: []byte("other")},
	} {
		tracker, req, resp := newPrimaryTrackerFixture()
		tracker.observe(req, resp, nil, time.Millisecond)
		tracker.register(changed)
		require.Equal(t, primaryWriteSample{}, tracker.finish())
	}
}

func TestPrimaryWriteTrackerConcurrentFreeze(t *testing.T) {
	tracker, req, resp := newPrimaryTrackerFixture()
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() { defer wg.Done(); tracker.observe(req, resp, nil, time.Millisecond) }()
	}
	first := tracker.finish()
	wg.Wait()
	require.Equal(t, first, tracker.finish())
}

func TestPrimaryWriteTrackerExcludesAsyncAndCopiesIdentity(t *testing.T) {
	tracker, req, resp := newPrimaryTrackerFixture()
	tracker.register(&kvrpcpb.PrewriteRequest{StartVersion: 123, PrimaryLock: []byte("primary"), UseAsyncCommit: true})
	tracker.observe(req, resp, nil, time.Millisecond)
	require.Equal(t, primaryWriteSample{}, tracker.finish())

	tracker = &primaryWriteTracker{}
	key := []byte("primary")
	tracker.register(&kvrpcpb.PrewriteRequest{StartVersion: 123, PrimaryLock: key})
	key[0] = 'X'
	resp.ExecDetailsV2.WriteDetail = &kvrpcpb.WriteDetail{}
	tracker.observe(req, resp, nil, time.Millisecond)
	sample := tracker.finish()
	require.EqualValues(t, 1, sample.SuccessfulRPCs)
	require.Equal(t, "write", sample.Details, "present zero detail is not missing")
	require.Zero(t, sample.PersistLog)
}
