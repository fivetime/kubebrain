// Copyright 2023 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
)

type RevisionSyncer interface {
	// SyncReadRevision  fetch the latest revision from leader and set it in backend
	// if this instance is follower. otherwise, do nothing.
	SyncReadRevision(ctx context.Context) error

	// Close closes syncer
	Close() error
}

type Backend interface {
	// SetCurrentRevision sets the revision of backend
	SetCurrentRevision(uint64)
}

var (
	syncRevTimeout         = time.Second
	syncRevRetryBackoff    = 250 * time.Millisecond
	syncRevMaxRetryElapsed = 8 * time.Second
	errLeaderChanged       = errors.New("leader changed while fetching revision")
)

type revisionSyncer struct {
	// inject
	leaderElection leader.LeaderElection
	metricCli      metrics.Metrics
	backend        Backend
	enableTLS      bool

	// internal
	schema     string
	httpClient *http.Client

	// fetchMu guards the double-buffered read-index fetch state. current is the
	// in-flight leader-revision fetch; next is the batch for readers that arrived
	// while current was running and therefore must be served by a fetch started
	// after they arrived (read-index freshness, #43).
	fetchMu sync.Mutex
	current *revFetch
	next    *revFetch
}

// revFetch is one shared leader-revision fetch; done is closed when rev/err are set.
type revFetch struct {
	done chan struct{}
	rev  uint64
	err  error
}

func defaultTransportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}

func NewRevisionSyncer(backend Backend, metricCli metrics.Metrics, l leader.LeaderElection, tlsConfig *tls.Config) RevisionSyncer {
	r := &revisionSyncer{
		leaderElection: l,
		metricCli:      metricCli,
		backend:        backend,
		schema:         "http",
		enableTLS:      false,
	}

	// transport is a copy of http.DefaultTransport
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: defaultTransportDialContext(&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if tlsConfig != nil {
		r.schema = "https"
		r.enableTLS = true
		transport.TLSClientConfig = tlsConfig
	}

	r.httpClient = &http.Client{
		Transport: transport,
		Timeout:   syncRevTimeout,
	}

	return r
}

// SyncReadRevision implements RevisionSyncer
func (r *revisionSyncer) SyncReadRevision(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.leaderElection.IsLeader() {
		return nil
	}
	// only sync when not leader
	r.metricCli.EmitCounter("read.follower", 1)
	currentRevision, err := r.getFreshRevisionFromLeader(ctx)
	if err != nil {
		r.metricCli.EmitCounter("read.follower.revision_err", 1)
		klog.Errorf("sync read revision failed %v", err)
		return fmt.Errorf("get revision from leader failed: %w", err)
	}
	r.backend.SetCurrentRevision(currentRevision)
	return nil
}

// Close implements RevisionSyncer
func (r *revisionSyncer) Close() error {
	r.httpClient.CloseIdleConnections()
	return nil
}

// LeaderRevision is the data return by leader
type LeaderRevision struct {
	// Revision is the revision of leader
	Revision uint64
}

// getFreshRevisionFromLeader returns the leader's revision from a fetch that
// started at or after this call arrived, so it is safe to use as a read index.
//
// A plain singleflight would let a reader that arrives mid-flight join the
// in-flight fetch and receive a revision read BEFORE the reader arrived, missing
// writes committed in between and breaking the read-index staleness bound (#43).
// Instead this double-buffers: a reader with no fetch in flight starts one (which
// reads the leader after it arrived); a reader arriving while a fetch is running
// joins the `next` batch, which is only started once the current fetch completes
// — i.e. after the reader arrived. All readers arriving during one fetch coalesce
// into a single next fetch, so the leader is not stampeded.
func (r *revisionSyncer) getFreshRevisionFromLeader(ctx context.Context) (uint64, error) {
	r.fetchMu.Lock()
	var f *revFetch
	if r.current == nil {
		f = &revFetch{done: make(chan struct{})}
		r.current = f
		go r.runFetch(f)
	} else {
		if r.next == nil {
			r.next = &revFetch{done: make(chan struct{})}
		}
		f = r.next
	}
	r.fetchMu.Unlock()

	select {
	case <-f.done:
		return f.rev, f.err
	case <-ctx.Done():
		// This reader gives up; the shared fetch keeps running for the others.
		return 0, ctx.Err()
	}
}

// runFetch performs one shared fetch, then promotes any batch that accumulated
// while it ran to be the next in-flight fetch. Fetches therefore run strictly
// serially (at most one at a time), and each starts after every reader it serves
// arrived.
func (r *revisionSyncer) runFetch(f *revFetch) {
	// Independent timeout so a single reader's cancelled ctx does not abort the
	// fetch the other readers are waiting on; bounded by the retry budget.
	ctx, cancel := context.WithTimeout(context.Background(), syncRevMaxRetryElapsed)
	f.rev, f.err = r.getRevisionFromLeaderWithRetry(ctx)
	cancel()
	close(f.done)

	r.fetchMu.Lock()
	r.current = nil
	nxt := r.next
	r.next = nil
	if nxt != nil {
		r.current = nxt
	}
	r.fetchMu.Unlock()
	if nxt != nil {
		go r.runFetch(nxt)
	}
}

func (r *revisionSyncer) getRevisionFromLeaderWithRetry(ctx context.Context) (uint64, error) {
	start := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rev, err := r.getRevisionFromLeaderWithSchemas(ctx)
		if err == nil {
			return rev, nil
		}
		if !retryableLeaderRevisionErr(err) {
			return 0, err
		}
		if time.Since(start) >= syncRevMaxRetryElapsed {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(syncRevRetryBackoff):
		}
	}
}

func (r *revisionSyncer) getRevisionFromLeaderWithSchemas(ctx context.Context) (uint64, error) {
	// there is no guarantee about the schema of leader, so we just try one by one
	for _, schema := range r.getRetrySchemas() {
		r.schema = schema
		rev, err := r.getRevisionFromLeader(ctx)
		if err != nil {
			if possibleSchemaMismatch(err) {
				// switch schema and retry in next loop if possible
				continue
			}

			// for other errors, let the caller decide whether leader-change retry applies
			return uint64(0), err
		}

		return rev, nil
	}

	// maybe leader can be access by https only but current node is running without cert
	err := status.Errorf(codes.Unavailable, "no suitable schema to leader")
	klog.ErrorS(err, "can not get revision from leader", "leader", r.leaderElection.GetLeaderInfo())
	return uint64(0), err
}

func retryableLeaderRevisionErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, errLeaderChanged) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "status code from leader") ||
		strings.Contains(msg, "leader is not elected") ||
		// A malformed status body or a transient zero revision is the same class of
		// transient leader issue as a non-200 (leader restarting / not yet
		// initialized); retry within the elapsed budget rather than failing the
		// read immediately (#42).
		strings.Contains(msg, "unmarshal status from leader") ||
		strings.Contains(msg, "returned zero revision")
}

func possibleSchemaMismatch(err error) bool {
	if err == nil {
		// success
		return false
	}
	// schema mismatching will rise an error:
	// ┌──────────┬──────────┬───────────────────────────────────────────────┐
	// │  client  │  server  │                     error                     │
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │   http   │cmux https│ connection reset by peer (syscall.ECONNRESET) │
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │  https   │cmux http │                 EOF (io.EOF)                  │
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │   http   │std https │Client sent an HTTP request to an HTTPS server.│
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │  https   │ std http │http: server gave HTTP response to HTTPS client│
	// └──────────┴──────────┴───────────────────────────────────────────────┘
	// switch schema if possible
	if errors.Is(err, syscall.ECONNRESET) {
		klog.InfoS("conn reset", "err", err)
		return true
	}

	if errors.Is(err, io.EOF) {
		klog.InfoS("EOF", "err", err)
		return true
	}

	if isSendHttpReqToHttpsServerErr(err) {
		klog.InfoS("send http request to https server", "err", err)
		return true
	}

	if isSendHttpsReqToHttpServerErr(err) {
		klog.InfoS("send https request to http server", "err", err)
		return true
	}

	// for other error, do not retry
	return false
}

func isSendHttpReqToHttpsServerErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Client sent an HTTP request to an HTTPS server.")
}

func isSendHttpsReqToHttpServerErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http: server gave HTTP response to HTTPS client")
}

func (r *revisionSyncer) getRevisionFromLeader(ctx context.Context) (uint64, error) {
	leaderAddress := r.leaderElection.GetLeaderInfo()
	if !election.IsLeaderKnown(leaderAddress) {
		return 0, status.Errorf(codes.Unavailable, "leader is not elected")
	}
	leaderTerm := r.leaderElection.CurrentLeadershipTerm()
	r.metricCli.EmitGauge("follower.getleader", 1, metrics.Tag("leader", leaderAddress))
	startTime := time.Now()

	// todo: implement it based on grpc API
	url := fmt.Sprintf("%s://%s/status", r.schema, leaderAddress)
	klog.V(10).InfoS("get revision", "from", url)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	response, err := r.httpClient.Do(request)
	r.metricCli.EmitHistogram("member.round_trip",
		time.Since(startTime).Milliseconds(),
		metrics.Tag("leader", leaderAddress))
	if err != nil {
		r.metricCli.EmitCounter("follower.get.revision.err", 1, metrics.Tag("leader", leaderAddress))
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		r.metricCli.EmitCounter("follower.get.revision.failed", 1, metrics.Tag("leader", leaderAddress))
		//return 0, errors.Wrapf(err, "status code from leader %s is %d", leaderAddress, response.StatusCode)

		msg, _ := io.ReadAll(response.Body)
		return 0, fmt.Errorf("status code from leader %s is %d, msg is %s", leaderAddress, response.StatusCode, msg)
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}

	revision := &LeaderRevision{}
	if err := json.Unmarshal(responseBody, revision); err != nil {
		// A malformed body (LB/proxy error page, truncated response, wrong
		// content) must NOT be swallowed: the old code ignored this error and
		// returned (0, nil), so the follower set its read revision to 0 and served
		// reads against an empty/rewound index (#42).
		r.metricCli.EmitCounter("follower.get.revision.unmarshal_err", 1, metrics.Tag("leader", leaderAddress))
		return 0, fmt.Errorf("unmarshal status from leader %s failed (body=%q): %w", leaderAddress, string(responseBody), err)
	}
	if revision.Revision == 0 {
		// Valid JSON but no/zero revision (e.g. "{}" or an error object). A healthy
		// leader's revision is TSO-derived and never 0, so treat this as a bad
		// response rather than rewinding the follower's read index to 0 (#42).
		r.metricCli.EmitCounter("follower.get.revision.zero", 1, metrics.Tag("leader", leaderAddress))
		return 0, fmt.Errorf("leader %s returned zero revision (body=%q)", leaderAddress, string(responseBody))
	}
	currentLeader := r.leaderElection.GetLeaderInfo()
	currentTerm := r.leaderElection.CurrentLeadershipTerm()
	currentIsLeader := r.leaderElection.IsLeader()
	if currentIsLeader ||
		currentLeader != leaderAddress ||
		(leaderTerm != 0 && currentTerm != 0 && currentTerm != leaderTerm) {
		r.metricCli.EmitCounter("follower.get.revision.leader_changed", 1,
			metrics.Tag("leader", leaderAddress))
		return 0, fmt.Errorf(
			"%w: requested leader=%s term=%d, current leader=%s term=%d, isLeader=%t",
			errLeaderChanged, leaderAddress, leaderTerm, currentLeader, currentTerm,
			currentIsLeader,
		)
	}
	r.metricCli.EmitGauge("follower.get.revision", revision.Revision, metrics.Tag("leader", leaderAddress))
	return revision.Revision, nil
}

var (
	schemasHttpOnly  = []string{"http"}
	schemasHttpsOnly = []string{"https"}
)

func (r *revisionSyncer) getRetrySchemas() []string {
	if !r.enableTLS {
		return schemasHttpOnly
	}
	// TLS is enabled: only ever use https. Falling back to plain http would let a
	// network attacker downgrade the leader /status sync and feed this follower a
	// forged read revision, which drives what it serves reads at (#31). A leader
	// still on http during a rollout must be reached over https once upgraded,
	// not silently over cleartext.
	return schemasHttpsOnly
}
