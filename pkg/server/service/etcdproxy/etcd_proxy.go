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

package etcdproxy

import (
	"context"
	"crypto/tls"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/util"
)

type etcdProxy struct {
	election  leader.LeaderElection
	tlsConfig *tls.Config

	closed    chan struct{}
	client    *clientv3.Client
	err       error
	curLeader string
	lock      sync.RWMutex
	// updateMu serializes updateClient so at most one goroutine builds/swaps the
	// forwarding client at a time. Without it the 1s checkLeaderLoop and the RPC
	// goroutines that call updateClient via waitReady run concurrently: each
	// releases `lock` during the slow clientv3.New/checkClientConn and then does
	// e.client = client, so a later winner overwrites an earlier client without
	// closing it (leaked connection, #47) and every caller stampedes the new
	// leader with its own dial (#41). Held for the whole function; acquired before
	// `lock` so the lock order is always updateMu -> lock.
	updateMu sync.Mutex
}

func (e *etcdProxy) EtcdProxyEnabled() bool {
	return true
}

// defaultCallOption is a copy of etcd client default call option
var defaultCallOption = []grpc.CallOption{
	grpc.FailFast(false),
	grpc.MaxCallSendMsgSize(2 * 1024 * 1024),
	grpc.MaxCallRecvMsgSize(math.MaxInt32),
}

const proxyReadyWaitTimeout = 2 * time.Second

// NewEtcdProxy return an ETCD proxy for forward request to leader
func NewEtcdProxy(leaderElection leader.LeaderElection, tlsConfig *tls.Config) EtcdProxy {
	proxy := &etcdProxy{election: leaderElection, tlsConfig: tlsConfig}
	proxy.updateClient()
	go func() {
		defer util.Recover()
		proxy.checkLeaderLoop()
	}()

	return proxy
}

func (e *etcdProxy) checkLeaderLoop() {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		e.updateClient()
		<-timer.C
		timer.Reset(time.Second)
	}
}

func (e *etcdProxy) resetClient() (reset bool) {
	reset = e.client != nil

	if e.client != nil {
		_ = e.client.Close()
		e.client = nil
	}

	if e.closed != nil {
		close(e.closed)
	}
	e.closed = make(chan struct{})
	return
}

func (e *etcdProxy) updateClient() {
	// Serialize the whole build/swap: concurrent callers otherwise leak clients
	// and stampede the leader (#41/#47). Once the winner has a healthy client, the
	// queued callers fall through the hasClient()+checkConn() fast path and return
	// without redialing.
	e.updateMu.Lock()
	defer e.updateMu.Unlock()

	if e.hasClient() {
		if err := e.checkConn(); err != nil {
			e.lock.Lock()
			defer e.lock.Unlock()
			e.curLeader = ""
			e.err = err
			if e.resetClient() {
				klog.InfoS("reset client caused by checking conn", "err", e.err)
			}
			return
		}
	}

	if e.election.IsLeader() {
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.resetClient() {
			klog.InfoS("reset client caused by becoming leader")
		}
		return
	}

	curLeader := e.election.GetLeaderInfo()
	e.lock.RLock()
	sameLeader := curLeader == e.curLeader
	e.lock.RUnlock()
	if sameLeader || curLeader == "empty" || curLeader == "" {
		return
	}

	e.lock.Lock()
	oldLeader := e.curLeader
	// close prev client
	if e.resetClient() {
		klog.InfoS("reset client caused by changing leader")
	}
	e.lock.Unlock()

	klog.InfoS("try to conn to new leader", "newLeader", curLeader)
	tlsConfigs := []*tls.Config{e.tlsConfig}
	if e.tlsConfig != nil {
		tlsConfigs = []*tls.Config{e.tlsConfig, nil}
	}

	for _, tlsConfig := range tlsConfigs {
		client, err := clientv3.New(clientv3.Config{
			Endpoints: []string{curLeader},
			TLS:       tlsConfig,
		})
		if err != nil {
			klog.ErrorS(err, "failed to create new client")
			e.lock.Lock()
			e.err = status.Error(codes.Internal, err.Error())
			e.lock.Unlock()
			return
		}

		klog.InfoS("check conn to new leader", "newLeader", curLeader, "secure", tlsConfig != nil)

		err = checkClientConn(client, nil)
		if err != nil {
			_ = client.Close()
			klog.InfoS("leader connection not ready", "err", err, "newLeader", curLeader, "secure", tlsConfig != nil)
			e.lock.Lock()
			e.err = err
			e.lock.Unlock()
			if errors.Is(err, context.DeadlineExceeded) {
				// maybe tls config error, just change config and retry
				continue
			}
			break
		}

		e.lock.Lock()
		e.client = client
		e.err = nil
		e.curLeader = curLeader
		e.lock.Unlock()
		klog.InfoS("conn to new leader", "oldLeader", oldLeader, "curLeader", curLeader)
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	klog.InfoS("leader connection not ready", "err", e.err, "newLeader", curLeader)
	e.curLeader = ""
	if e.client != nil {
		_ = e.client.Close()
		e.client = nil
	}
}

func (e *etcdProxy) hasClient() bool {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.client != nil
}

func (e *etcdProxy) checkConn() error {
	e.lock.RLock()
	client := e.client
	err := e.err
	e.lock.RUnlock()
	return checkClientConn(client, err)
}

func checkClientConn(client *clientv3.Client, clientErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if client == nil {
		return clientErr
	}

	_, err := client.MemberList(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (e *etcdProxy) readyClient(ctx context.Context) (*clientv3.Client, string, <-chan struct{}, error) {
	if err := e.waitReady(ctx); err != nil {
		return nil, "", nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	if err := e.readyLocked(); err != nil {
		return nil, "", nil, err
	}
	return e.client, e.curLeader, e.closed, nil
}

func (e *etcdProxy) markForwardError(client *clientv3.Client, err error) {
	if err == nil || !isForwardConnectionError(err) {
		return
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.client != client {
		return
	}
	e.err = err
	e.curLeader = ""
	if e.resetClient() {
		klog.InfoS("reset client caused by forward error", "err", err)
	}
}

func isForwardConnectionError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true
	default:
		return false
	}
}

func getKeyFromTxn(txn *etcdserverpb.TxnRequest) (string, int64) {
	if len(txn.GetCompare()) <= 0 {
		return "", 0
	}
	return string(txn.GetCompare()[0].GetKey()), txn.GetCompare()[0].GetModRevision()
}

func (e *etcdProxy) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}

	key, rev := getKeyFromTxn(txn)
	klog.InfoS("forward txn",
		"leader", leader,
		"key", key,
		"rev", rev)
	resp, err := etcdserverpb.NewKVClient(client.ActiveConnection()).Txn(ctx, txn, defaultCallOption...)
	e.markForwardError(client, err)
	if err != nil {
		klog.InfoS("forward txn failed", "key", key, "err", err.Error())
		return nil, err
	}
	if !resp.GetSucceeded() {
		var respRev int64

		if resp.Responses != nil && len(resp.Responses) == 1 {
			getResp := (*clientv3.GetResponse)(resp.Responses[0].GetResponseRange())
			if getResp != nil && getResp.Kvs != nil && len(getResp.Kvs) == 1 {
				respRev = getResp.Kvs[0].ModRevision
			}
		}

		klog.InfoS("forward txn cas failed",
			"key", key,
			"result", resp.GetSucceeded(),
			"rev", rev,
			"respRev", resp.GetHeader().GetRevision(),
			"kvRev", respRev,
		)
	}

	return resp, err
}

func (e *etcdProxy) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward range", "leader", leader, "key", string(req.Key), "rangeEnd", string(req.RangeEnd), "revision", req.Revision)
	resp, err := etcdserverpb.NewKVClient(client.ActiveConnection()).Range(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward put", "leader", leader, "key", string(req.Key), "lease", req.Lease)
	resp, err := etcdserverpb.NewKVClient(client.ActiveConnection()).Put(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward delete range", "leader", leader, "key", string(req.Key), "rangeEnd", string(req.RangeEnd))
	resp, err := etcdserverpb.NewKVClient(client.ActiveConnection()).DeleteRange(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward compact", "leader", leader, "revision", req.Revision, "physical", req.Physical)
	resp, err := etcdserverpb.NewKVClient(client.ActiveConnection()).Compact(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease grant", "leader", leader, "id", req.ID, "ttl", req.TTL)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseGrant(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease revoke", "leader", leader, "id", req.ID)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseRevoke(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease keepalive", "leader", leader, "id", req.ID)
	// The follower forwards one keepalive message and returns one response, but
	// LeaseKeepAlive is a bidi stream. Derive a per-call cancelable context and
	// cancel it on return so the leader-side stream is fully torn down instead of
	// left half-open (CloseSend only closes the send direction, leaking the
	// receive side until the long-lived caller context ends) (#62).
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseKeepAlive(callCtx, defaultCallOption...)
	if err != nil {
		e.markForwardError(client, err)
		return nil, err
	}
	if err := stream.Send(req); err != nil {
		e.markForwardError(client, err)
		return nil, err
	}
	resp, err := stream.Recv()
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease ttl", "leader", leader, "id", req.ID, "keys", req.Keys)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseTimeToLive(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease leases", "leader", leader)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseLeases(ctx, req, defaultCallOption...)
	e.markForwardError(client, err)
	return resp, err
}

func (e *etcdProxy) Ready() error {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.readyLocked()
}

func (e *etcdProxy) readyLocked() error {
	if e.client == nil {
		return status.Errorf(codes.Unavailable, "no ready right now")
	}
	currentLeader := e.election.GetLeaderInfo()
	if currentLeader == "" || currentLeader == "empty" {
		return status.Errorf(codes.Unavailable, "leader is not elected")
	}
	if e.curLeader != currentLeader {
		return status.Errorf(codes.Unavailable, "proxy leader %q is stale, current leader %q", e.curLeader, currentLeader)
	}
	return nil
}

func (e *etcdProxy) waitReady(ctx context.Context) error {
	if err := e.Ready(); err == nil {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, proxyReadyWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if lastErr != nil {
				return status.Errorf(codes.Unavailable, "proxy is not ready after %s: %v", proxyReadyWaitTimeout, lastErr)
			}
			return status.Errorf(codes.Unavailable, "proxy is not ready after %s", proxyReadyWaitTimeout)
		default:
		}

		e.updateClient()
		if err := e.Ready(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if lastErr != nil {
				return status.Errorf(codes.Unavailable, "proxy is not ready after %s: %v", proxyReadyWaitTimeout, lastErr)
			}
			return status.Errorf(codes.Unavailable, "proxy is not ready after %s", proxyReadyWaitTimeout)
		case <-ticker.C:
		}
	}
}

func (e *etcdProxy) Watch(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan WatchResult, error) {
	if _, _, _, err := e.readyClient(ctx); err != nil {
		return nil, err
	}

	outputCh := make(chan WatchResult, 100)
	go func() {
		defer util.Recover()
		defer close(outputCh)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		watchRevision := revision
		for {
			client, leader, closed, err := e.readyClient(ctx)
			if err != nil {
				klog.InfoS("etcd proxy watch ready failed", "key", string(key), "rangeEnd", string(rangeEnd), "rev", watchRevision, "error", err)
				return
			}

			klog.InfoS("etcd proxy start watching", "leader", leader, "key", string(key), "rangeEnd", string(rangeEnd), "rev", watchRevision)
			// Always request PrevKV from the leader. The outer etcd watch server
			// still strips PrevKv when the original client did not request it.
			inputCh := client.Watch(ctx, string(key), watchOptionsForRange(rangeEnd, watchRevision)...)
			reconnect := false
			for !reconnect {
				select {
				case <-closed:
					klog.InfoS("etcd proxy watch leader changed", "key", string(key), "rangeEnd", string(rangeEnd), "rev", watchRevision)
					reconnect = true
				case <-ctx.Done():
					klog.InfoS("etcd proxy watch ctx done")
					return
				case wresp, ok := <-inputCh:
					if !ok {
						klog.InfoS("etcd proxy watch closed", "key", string(key), "rangeEnd", string(rangeEnd), "rev", watchRevision, "channel", outputCh)
						reconnect = true
						break
					}
					err := wresp.Err()
					if err != nil {
						klog.InfoS("etcd proxy watch error", "key", string(key), "rangeEnd", string(rangeEnd), "rev", watchRevision, "channel", outputCh, "error", err)
						e.markForwardError(client, err)
						if isForwardConnectionError(err) {
							reconnect = true
							break
						}
						outputCh <- WatchResult{Err: err}
						return
					}
					// Advance the resume revision to the store revision this
					// response covers, so a reconnect after a leader change resumes
					// from a concrete point instead of restarting at the new
					// leader's "current" and silently dropping the gap (#63). The
					// header revision is >= every event's ModRevision (subsuming
					// per-event advancement) and, thanks to WithProgressNotify,
					// also advances on idle progress notifications that carry no
					// events — the case where the old code left watchRevision at
					// its initial value (0 for a from-now watch). The Created
					// response carries revision 0, which nextWatchRevision ignores.
					watchRevision = nextWatchRevision(watchRevision, wresp.Header.Revision)
					events := convertEvents(wresp.Events)
					outputCh <- WatchResult{Events: events}
				}
			}

			select {
			case <-ctx.Done():
				klog.InfoS("etcd proxy watch ctx done")
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return outputCh, nil
}

func watchOptionsForRange(rangeEnd []byte, revision uint64) []clientv3.OpOption {
	// WithProgressNotify makes the leader advertise its current revision even
	// when the watched range is idle, so the proxy can advance its resume point
	// and not lose the gap on a leader-change reconnect (#63).
	opts := []clientv3.OpOption{clientv3.WithRev(int64(revision)), clientv3.WithPrevKV(), clientv3.WithProgressNotify()}
	if rangeEnd == nil {
		return opts
	}
	if len(rangeEnd) == 0 {
		return append(opts, clientv3.WithFromKey())
	}
	return append(opts, clientv3.WithRange(string(rangeEnd)))
}

// nextWatchRevision returns the resume revision after a watch response whose
// header covers store revision headerRev. It never moves backwards, and ignores
// a zero header revision (e.g. the Created response) so the proxy does not rewind
// a from-now watch to the beginning of history.
func nextWatchRevision(current uint64, headerRev int64) uint64 {
	if headerRev <= 0 {
		return current
	}
	if next := uint64(headerRev) + 1; next > current {
		return next
	}
	return current
}

func convertEvents(events []*clientv3.Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, len(events))
	for i, event := range events {
		ret[i] = (*mvccpb.Event)(event)
	}
	return ret
}
