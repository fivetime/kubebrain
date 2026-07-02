// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdproxy

import (
	"context"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsForwardConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "context deadline", err: context.DeadlineExceeded, want: true},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "canceled"), want: true},
		{name: "grpc deadline", err: status.Error(codes.DeadlineExceeded, "deadline"), want: true},
		{name: "grpc unavailable", err: status.Error(codes.Unavailable, "unavailable"), want: true},
		{name: "grpc invalid argument", err: status.Error(codes.InvalidArgument, "bad request"), want: false},
		{name: "grpc out of range", err: status.Error(codes.OutOfRange, "compacted"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isForwardConnectionError(tt.err))
		})
	}
}

func TestWaitReadyReturnsUnavailableWhenLeaderConnectionIsNotReady(t *testing.T) {
	proxy := &etcdProxy{
		election: &testLeaderElection{leaderAddress: "127.0.0.1:1"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	err := proxy.waitReady(ctx)

	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Less(t, time.Since(start), 4*time.Second)
}

type testLeaderElection struct {
	leaderAddress string
	isLeader      bool
}

func (t *testLeaderElection) Campaign(context.Context) {}

func (t *testLeaderElection) GetLeaderInfo() string {
	return t.leaderAddress
}

func (t *testLeaderElection) IsLeader() bool {
	return t.isLeader
}

func (t *testLeaderElection) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: t.leaderAddress, IsLeader: t.isLeader}, nil
}

// TestNextWatchRevision pins the #63 resume-revision logic: advance to
// headerRev+1, never move backwards, and ignore a zero header (Created response)
// so a from-now watch is not rewound to the start of history on reconnect.
func TestNextWatchRevision(t *testing.T) {
	tests := []struct {
		name      string
		current   uint64
		headerRev int64
		want      uint64
	}{
		{name: "created response (rev 0) leaves from-now watch untouched", current: 0, headerRev: 0, want: 0},
		{name: "first concrete revision resolves from-now watch", current: 0, headerRev: 100, want: 101},
		{name: "advances on newer revision", current: 101, headerRev: 150, want: 151},
		{name: "does not move backwards for stale header", current: 200, headerRev: 150, want: 200},
		{name: "same revision does not advance", current: 151, headerRev: 150, want: 151},
		{name: "negative header ignored", current: 50, headerRev: -1, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, nextWatchRevision(tt.current, tt.headerRev))
		})
	}
}

// TestWatchOptionsForRangeRequestsProgressNotify guards that the proxy watch
// requests progress notifications (needed for #63) across the range variants.
func TestWatchOptionsForRangeRequestsProgressNotify(t *testing.T) {
	// base opts: WithRev + WithPrevKV + WithProgressNotify = 3
	require.Len(t, watchOptionsForRange(nil, 5), 3, "single-key watch: rev+prevkv+progress")
	require.Len(t, watchOptionsForRange([]byte{}, 5), 4, "from-key watch adds WithFromKey")
	require.Len(t, watchOptionsForRange([]byte("z"), 5), 4, "range watch adds WithRange")
}
