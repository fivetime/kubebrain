// Copyright 2022 ByteDance and/or its affiliates
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage"
	ibadger "github.com/kubewharf/kubebrain/pkg/storage/badger"
	"github.com/kubewharf/kubebrain/pkg/util/auth"
)

func newBadgerStorage(t *testing.T, ast *assert.Assertions) storage.KvStorage {
	p := path.Join(t.TempDir(), "badger")
	st, err := ibadger.NewKvStorage(ibadger.Config{Dir: p})
	if err != nil {
		ast.FailNow(err.Error())
	}
	return st
}

type testcase struct {
	name            string
	retryTimes      int
	expectError     error
	respWaitTime    time.Duration
	enableCmux      bool
	clientTlsConfig *tls.Config
	serverTlsConfig *tls.Config
}

type backendStub struct {
	currentRev uint64
}

func (b *backendStub) SetCurrentRevision(u uint64) {
	b.currentRev = u
}

func (tc *testcase) run(t *testing.T) {
	ast := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMetrics := mock.NewMinimalMetrics(ctrl)

	wg := sync.WaitGroup{}
	ts := &testRevisionServer{
		t:            t,
		ast:          ast,
		wg:           &wg,
		respWaitTime: tc.respWaitTime,
		tlsConfig:    tc.serverTlsConfig,
		enableCmux:   tc.enableCmux,
	}
	stopServer := ts.run()

	leaderElectionStub := &leader.Stub{
		ElectionInfo: leader.ElectionInfo{
			IsLeader:      false,
			LeaderAddress: ts.addr,
		},
	}

	bs := &backendStub{}
	rs := NewRevisionSyncer(bs, mockMetrics, leaderElectionStub, tc.clientTlsConfig)
	defer rs.Close()

	times := 0
	var err error
	ctx := context.Background()
	cancel := func() {}
	if tc.expectError != nil {
		ctx, cancel = context.WithTimeout(context.Background(), 1500*time.Millisecond)
	}
	defer cancel()
	for times < tc.retryTimes {
		err = rs.SyncReadRevision(ctx)
		if err != nil {

			// since the server is run in a goroutine, there may be error if the server is not ready
			if errors.Is(err, syscall.ECONNREFUSED) {
				times++
				t.Logf("retry times %d", times)
				time.Sleep(time.Millisecond * 100)
				continue
			}

			if tc.expectError == nil {
				t.Error("unexpected failed of sync revision", err)
			} else {
				ast.True(errors.Is(err, tc.expectError) || strings.Contains(err.Error(), tc.expectError.Error()))
			}
			break
		}

		if tc.expectError != nil {
			ast.Error(err, "unexpected success")
		}
		break
	}

	stopServer()
	wg.Wait()
}

func TestHttpRevisionSyncer(t *testing.T) {

	serverTlsConfig, clientTlsConfig := mustLoadCert(t)
	testcases := []testcase{
		{
			name:         "success",
			retryTimes:   10,
			respWaitTime: 0,
		},
		{
			name:         "timeout",
			retryTimes:   10,
			respWaitTime: 2 * time.Second,
			expectError:  context.DeadlineExceeded,
		},
		{
			name:            "http->std https",
			retryTimes:      10,
			expectError:     errors.New("no suitable schema to leader"),
			clientTlsConfig: nil,
			serverTlsConfig: serverTlsConfig,
		},
		{
			// #31: a TLS-configured follower must NOT downgrade to plain http; it
			// fails rather than syncing the leader revision over cleartext.
			name:            "https->std http(no downgrade: expect failure)",
			retryTimes:      10,
			expectError:     errors.New("no suitable schema to leader"),
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: nil,
		},
		{
			name:            "https->std https",
			retryTimes:      10,
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: serverTlsConfig,
		},
		{
			name:            "http->cmux https",
			retryTimes:      10,
			expectError:     errors.New("no suitable schema to leader"),
			clientTlsConfig: nil,
			serverTlsConfig: serverTlsConfig,
			enableCmux:      true,
		},
		{
			// #31: no downgrade to plain http even against a cmux listener.
			name:            "https->cmux http(no downgrade: expect failure)",
			retryTimes:      10,
			expectError:     errors.New("no suitable schema to leader"),
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: nil,
			enableCmux:      true,
		},
		{
			name:            "https->cmux https",
			retryTimes:      10,
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: serverTlsConfig,
			enableCmux:      true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t)
		})
	}
}

func getAuthPath(filename string) string {
	return filepath.Join("../../../util/auth/testdata", filename)
}
func mustLoadCert(t *testing.T) (serverTlsConfig, clientTlsConfig *tls.Config) {
	var err error
	serverTlsConfig, err = auth.GetTLSConfig(getAuthPath("server.crt"), getAuthPath("server.key"), getAuthPath("ca.crt"))

	if err != nil {
		t.Fatalf("failed to get server tls config err:%v", err)
		return nil, nil
	}

	clientTlsConfig, err = auth.GetTLSConfig(getAuthPath("server.crt"), getAuthPath("server.key"), getAuthPath("ca.crt"))

	if err != nil {
		t.Fatalf("failed to get client tls config err:%v", err)
		return nil, nil
	}
	return
}

func TestNewRevisionSyncer(t *testing.T) {
	ast := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetrics := mock.NewMinimalMetrics(ctrl)
	t.Run("conn refused", func(t *testing.T) {
		addr := unusedLocalTCPAddr(t)
		leaderElectionStub := &leader.Stub{
			ElectionInfo: leader.ElectionInfo{
				IsLeader:      false,
				LeaderAddress: addr,
			},
		}

		backendConf := backend.Config{
			Prefix:   "/test",
			Identity: "127.0.0.1:2380",
		}
		testBackend := backend.NewBackend(newBadgerStorage(t, ast), backendConf, mockMetrics)
		rs := NewRevisionSyncer(testBackend, mockMetrics, leaderElectionStub, nil)
		defer rs.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		err := rs.SyncReadRevision(ctx)
		ast.True(strings.Contains(err.Error(), "connection refused") || errors.Is(err, context.DeadlineExceeded))
	})
}

func TestRevisionSyncerRetriesLeaderChange(t *testing.T) {
	ast := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMetrics := mock.NewMinimalMetrics(ctrl)

	wg := sync.WaitGroup{}
	ts := &testRevisionServer{
		t:   t,
		ast: ast,
		wg:  &wg,
	}
	stopServer := ts.run()
	defer func() {
		stopServer()
		wg.Wait()
	}()

	leaderElection := &mutableLeaderElection{leaderAddress: unusedLocalTCPAddr(t)}
	bs := &backendStub{}
	rs := NewRevisionSyncer(bs, mockMetrics, leaderElection, nil)
	defer rs.Close()

	go func() {
		time.Sleep(100 * time.Millisecond)
		leaderElection.setLeaderAddress(ts.addr)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := rs.SyncReadRevision(ctx)
	ast.NoError(err)
	ast.Equal(uint64(1000), bs.currentRev)
}

type mutableLeaderElection struct {
	mu            sync.RWMutex
	leaderAddress string
}

func (m *mutableLeaderElection) Campaign(context.Context) {}

func (m *mutableLeaderElection) GetLeaderInfo() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leaderAddress
}

func (m *mutableLeaderElection) LeadershipTerm(context.Context) (uint64, error) {
	return 1, nil
}

func (m *mutableLeaderElection) IsLeader() bool {
	return false
}

func (m *mutableLeaderElection) EpochAndLeadingFresh() (uint64, bool) {
	return 0, false
}

func (m *mutableLeaderElection) GetElectionInfo() (leader.ElectionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return leader.ElectionInfo{LeaderAddress: m.leaderAddress}, nil
}

func (m *mutableLeaderElection) setLeaderAddress(address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderAddress = address
}

type testRevisionServer struct {
	t            *testing.T
	ast          *assert.Assertions
	wg           *sync.WaitGroup
	enableCmux   bool
	respWaitTime time.Duration
	tlsConfig    *tls.Config
	addr         string
}

func (t *testRevisionServer) run() (cancel func()) {
	mux := http.NewServeMux()

	var statusHandler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if t.respWaitTime != 0 {
			time.Sleep(t.respWaitTime)
		}
		lr := LeaderRevision{Revision: 1000}
		data, err := json.Marshal(lr)
		t.ast.NoError(err)
		_, err = w.Write(data)
		t.ast.NoError(err)
	}

	mux.Handle("/status", statusHandler)
	rawListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.t.Fatalf("failed to listen err: %v", err)
		return
	}
	t.addr = rawListener.Addr().String()
	server := http.Server{Addr: t.addr, Handler: mux, TLSConfig: t.tlsConfig}

	listener := rawListener
	if t.enableCmux {
		c := cmux.New(listener)

		if t.tlsConfig == nil {
			listener = c.Match(cmux.HTTP1())
		} else {
			listener = c.Match(cmux.TLS())
		}
		go func() {
			err := c.Serve()
			t.t.Logf("cmux exit err:%v", err)
		}()
	}

	cancel = func() {
		t.t.Logf("try to stop server addr:%s", server.Addr)
		//time.Sleep(time.Minute)
		err := server.Close()
		t.ast.NoError(err)
		if t.enableCmux {
			_ = listener.Close()
		}
		_ = rawListener.Close()
		t.t.Logf("server closed addr:%s", server.Addr)
	}

	t.wg.Add(1)
	go func() {
		defer func() {
			t.wg.Done()
			t.t.Logf("server existed addr:%s", server.Addr)
		}()

		var err error
		if server.TLSConfig != nil {
			t.t.Logf("https server run addr:%s", server.Addr)
			err = server.ServeTLS(listener, "", "")
		} else {
			t.t.Logf("http server run addr:%s", server.Addr)
			err = server.Serve(listener)
		}

		if !t.ast.True(strings.Contains(err.Error(), "http: Server closed")) {
			t.ast.Failf(err.Error(), "unexpected err")
		}
	}()

	return cancel
}

func unusedLocalTCPAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate local tcp addr: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close local tcp listener: %v", err)
	}
	return addr
}

// TestFollowerRejectsMalformedOrZeroRevision pins #42: a follower must NOT set
// its read revision to 0 when the leader's /status body is unparseable or carries
// no/zero revision. Previously json.Unmarshal's error was ignored and the syncer
// returned (0, nil), rewinding the read index to 0.
func TestFollowerRejectsMalformedOrZeroRevision(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMetrics := mock.NewMinimalMetrics(ctrl)

	bad := []struct{ name, body string }{
		{"malformed_non_json", "<html>502 Bad Gateway</html>"},
		{"empty_json_object", "{}"},
		{"explicit_zero_revision", `{"Revision":0}`},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			ast := assert.New(t)
			body := c.body
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			addr := strings.TrimPrefix(srv.URL, "http://")

			bs := &backendStub{currentRev: 42} // pre-existing read revision that must survive
			le := &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: false, LeaderAddress: addr}}
			rs := NewRevisionSyncer(bs, mockMetrics, le, nil)
			defer rs.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			err := rs.SyncReadRevision(ctx)
			ast.Error(err, "malformed/zero status must be an error, not a silent revision 0")
			ast.Equal(uint64(42), bs.currentRev, "follower read revision must not be rewound to 0")
		})
	}

	t.Run("valid_revision_still_syncs", func(t *testing.T) {
		ast := assert.New(t)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(LeaderRevision{Revision: 777})
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		bs := &backendStub{currentRev: 42}
		le := &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: false, LeaderAddress: addr}}
		rs := NewRevisionSyncer(bs, mockMetrics, le, nil)
		defer rs.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		ast.NoError(rs.SyncReadRevision(ctx))
		ast.Equal(uint64(777), bs.currentRev)
	})
}

// TestReadIndexMidFlightReaderGetsFreshFetch pins #43: a reader that arrives while
// a leader-revision fetch is already in flight must be served by a NEW fetch that
// starts after it arrived (fresh read index), not by the in-flight one. The
// /status handler returns an increasing revision per call and blocks the first
// call until the second reader has queued; the mid-flight reader must observe the
// second (higher) revision.
func TestReadIndexMidFlightReaderGetsFreshFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMetrics := mock.NewMinimalMetrics(ctrl)

	var callCount int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			<-release // hold the first fetch in flight
		}
		_ = json.NewEncoder(w).Encode(LeaderRevision{Revision: uint64(n) * 100})
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	bs := &backendStub{}
	le := &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: false, LeaderAddress: addr}}
	rs := NewRevisionSyncer(bs, mockMetrics, le, nil).(*revisionSyncer)
	defer rs.Close()

	ctx := context.Background()
	waitFor := func(cond func() bool) {
		for i := 0; i < 200; i++ {
			if cond() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("condition not met in time")
	}

	// Reader A triggers the first (blocked) fetch F1.
	var aRev uint64
	var aErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); aRev, aErr = rs.getFreshRevisionFromLeader(ctx) }()
	waitFor(func() bool { return atomic.LoadInt32(&callCount) == 1 }) // F1 in flight

	// Reader B arrives mid-flight; it must queue into the NEXT batch, not join F1.
	var bRev uint64
	var bErr error
	wg.Add(1)
	go func() { defer wg.Done(); bRev, bErr = rs.getFreshRevisionFromLeader(ctx) }()
	waitFor(func() bool { rs.fetchMu.Lock(); defer rs.fetchMu.Unlock(); return rs.next != nil })

	close(release) // let F1 finish; F2 then runs for B
	wg.Wait()

	require.NoError(t, aErr)
	require.NoError(t, bErr)
	require.Equal(t, uint64(100), aRev, "reader A gets the first fetch's revision")
	require.Equal(t, uint64(200), bRev, "mid-flight reader B must get a fresh (second) fetch, not the in-flight one")
	require.Equal(t, int32(2), atomic.LoadInt32(&callCount), "readers arriving during one fetch coalesce into exactly one next fetch")
}
