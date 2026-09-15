package etcd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// The watermark verifier is not a per-event storage read. Keep the ordinary
// forwarding path read-free without weakening verification of compacted errors.
func TestForwardedWatchCompactionWatermarkReadBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result etcdproxy.WatchResult
	}{
		{name: "event", result: etcdproxy.WatchResult{Revision: 20, Events: []*mvccpb.Event{{Kv: &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 20}}}}},
		{name: "created", result: etcdproxy.WatchResult{Created: true}},
		{name: "progress", result: etcdproxy.WatchResult{ProgressRevision: 20}},
		{name: "transport error", result: etcdproxy.WatchResult{Err: errors.New("transport unavailable")}},
		{name: "text is not sentinel", result: etcdproxy.WatchResult{Err: errors.New(rpctypes.ErrCompacted.Error()), CompactRevision: 19}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			w := &watcher{backend: watchCompactMetricsBackend{read: func(context.Context) (uint64, error) {
				calls++
				return 0, errors.New("unexpected storage read")
			}}}
			require.NoError(t, w.validateForwardedWatchCompactionWatermark(context.Background(), tc.result))
			require.Zero(t, calls)
		})
	}

	for _, sentinel := range []error{rpctypes.ErrCompacted, rpctypes.ErrGRPCCompacted, fmt.Errorf("wrapped: %w", rpctypes.ErrCompacted)} {
		for _, tc := range []struct {
			name    string
			durable uint64
			readErr error
			wantErr string
		}{
			{name: "equal", durable: 19},
			{name: "durable ahead", durable: 20},
			{name: "unverified future", durable: 18, wantErr: "above durable compact revision"},
			{name: "read failure", readErr: errors.New("storage unavailable"), wantErr: "could not be verified"},
		} {
			t.Run(sentinel.Error()+"/"+tc.name, func(t *testing.T) {
				calls := 0
				w := &watcher{backend: watchCompactMetricsBackend{read: func(ctx context.Context) (uint64, error) {
					calls++
					_, bounded := ctx.Deadline()
					require.True(t, bounded, "durable verification must remain bounded")
					return tc.durable, tc.readErr
				}}}
				err := w.validateForwardedWatchCompactionWatermark(context.Background(), etcdproxy.WatchResult{Err: sentinel, CompactRevision: 19})
				if tc.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.wantErr)
					if tc.readErr != nil {
						require.ErrorIs(t, err, tc.readErr)
					}
				}
				require.Equal(t, 1, calls)
			})
		}
	}
}
