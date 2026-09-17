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

package revision

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

func TestReadRevisionEndsOldFetchOnLocalPromotion(t *testing.T) {
	for _, mode := range []string{"unchanged-follower", "fresh-local-leader", "stale-local-leader"} {
		promote := mode != "unchanged-follower"
		name := mode
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			var releaseOnce sync.Once
			var requests atomic.Int32
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				fmt.Fprint(w, `{"Revision":100}`)
			}))
			defer peer.Close()
			defer unblock()
			election := &mutableLeaderElection{leaderAddress: strings.TrimPrefix(peer.URL, "http://"), term: 1}
			b := &backendStub{currentRev: 42}
			ctrl := gomock.NewController(t)
			syncer := NewRevisionSyncer(b, mock.NewMinimalMetrics(ctrl), election, nil)
			defer syncer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- syncer.SyncReadRevision(ctx) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("fetch never entered")
			}
			var queued chan error
			if promote {
				queued = make(chan error, 1)
				go func() { queued <- syncer.SyncReadRevision(ctx) }()
				rs := syncer.(*revisionSyncer)
				require.Eventually(t, func() bool {
					rs.fetchMu.Lock()
					defer rs.fetchMu.Unlock()
					return rs.next != nil
				}, time.Second, time.Millisecond)
				election.mu.Lock()
				election.isLeader = true
				election.leadingFresh = mode == "fresh-local-leader"
				election.term++
				election.mu.Unlock()
				// A new caller is admitted immediately; the older caller must not spin.
				if mode == "fresh-local-leader" {
					require.NoError(t, syncer.SyncReadRevision(ctx))
				} else {
					require.ErrorIs(t, syncer.SyncReadRevision(ctx), errLeaderChanged)
				}
			}
			unblock()
			err := <-result
			if promote {
				require.ErrorIs(t, err, errLeaderChanged, "promotion must end the old batch without exhausting the read budget")
				require.ErrorIs(t, <-queued, errLeaderChanged, "queued batch must also end without another peer fetch")
			} else {
				require.NoError(t, err)
			}
			if promote {
				require.Equal(t, uint64(42), b.currentRev, "must not install rejected peer revision")
			} else {
				require.Equal(t, uint64(100), b.currentRev)
			}
			require.Equal(t, int32(1), requests.Load())
		})
	}
}
