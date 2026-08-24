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

package etcd

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/util"
)

// leaseStoragePrefix holds one small meta record per lease: leases/<id> -> {id,ttl}.
// leaseAttachPrefix holds one record per attached key: leasekeys/<userKey> -> <id>.
// Splitting the key list out of the meta record makes attach/detach an O(1) write
// instead of rewriting the whole (potentially thousands-of-keys) list per Put (#17).
// Both live under the reserved \x00kubebrain/ namespace so #15 compaction reclaims
// their superseded versions and tombstones. leasekeys/ sorts before leases/ (k<s),
// so a leases/ prefix scan never picks up attachment records.
var leaseStoragePrefix = []byte("\x00kubebrain/leases/")
var leaseAttachPrefix = []byte("\x00kubebrain/leasekeys/")
var leaseLegacyMigrationSealKey = []byte("\x00kubebrain/lease-legacy-migration-seal")
var leaseLegacyMigrationSealValue = []byte("v1")

var errLeaseDemotedDuringRenew = errors.New("lease manager demoted during renew")

var errInvalidLeaseMetadata = errors.New("lease metadata is inconsistent")

type invalidLeaseMetadataError struct {
	cause error
}

func (e *invalidLeaseMetadataError) Error() string { return e.cause.Error() }
func (e *invalidLeaseMetadataError) Unwrap() error { return e.cause }
func (e *invalidLeaseMetadataError) Is(target error) bool {
	return target == errInvalidLeaseMetadata
}

func markInvalidLeaseMetadata(err error) error {
	if err == nil || errors.Is(err, errInvalidLeaseMetadata) {
		return err
	}
	return &invalidLeaseMetadataError{cause: err}
}

func (m *leaseManager) keysForLease(id int64) []string {
	if id == 0 {
		return nil
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	lease := m.leases[id]
	if lease == nil {
		return nil
	}
	keys := make([]string, 0, len(lease.keys))
	for key := range lease.keys {
		keys = append(keys, key)
	}
	return keys
}

const leaseExpiryRetryInterval = time.Second
const leaseCheckpointInterval = 5 * time.Minute
const leaseMetadataReconciliationTimeout = 5 * time.Second
const defaultLeaseRevokeRate = 1000
const latestRestoreRevision = int64(^uint64(0) >> 1)
const maxLeaseTTL = int64(9000000000)
const minLeaseTTL = int64(2)

// leaseRecord is the per-lease meta record. Keys is written only by pre-#17
// (legacy monolithic) records and is still read on restore for a one-time
// migration; new meta records carry only ID and TTL.
type leaseRecord struct {
	ID               int64    `json:"id"`
	Incarnation      string   `json:"incarnation,omitempty"`
	TTL              int64    `json:"ttl"`
	RemainingTTL     int64    `json:"remainingTTL,omitempty"`
	DeadlineUnixNano int64    `json:"deadlineUnixNano,omitempty"`
	Keys             []string `json:"keys,omitempty"`
	LegacyStorage    bool     `json:"-"`
	LegacyKeys       []string `json:"-"`
	LegacyAttachKeys []string `json:"-"`
}

type leaseAttachmentRecord struct {
	ID          int64
	Incarnation string
}

func newLeaseIncarnation() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate lease incarnation: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func validLeaseIncarnation(value string) bool {
	if len(value) != 32 {
		return false
	}
	raw, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(raw) == value
}

type legacyLeaseMigration struct {
	id          int64
	attachKeys  []string
	cleanupKeys []string
}

func decodeLeaseRecord(raw []byte) (leaseRecord, error) {
	var record leaseRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return leaseRecord{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return leaseRecord{}, errors.New("lease metadata contains trailing JSON")
	}
	if record.ID == 0 {
		return leaseRecord{}, errors.New("lease id 0 is reserved for no lease")
	}
	if record.Incarnation != "" && !validLeaseIncarnation(record.Incarnation) {
		return leaseRecord{}, fmt.Errorf("lease incarnation %q is not canonical 128-bit lowercase hex", record.Incarnation)
	}
	if record.TTL > maxLeaseTTL {
		return leaseRecord{}, fmt.Errorf("lease ttl %d exceeds maximum %d", record.TTL, maxLeaseTTL)
	}
	if record.RemainingTTL > maxLeaseTTL {
		return leaseRecord{}, fmt.Errorf("lease remaining ttl %d exceeds maximum %d", record.RemainingTTL, maxLeaseTTL)
	}
	for index, key := range record.Keys {
		if key == "" {
			return leaseRecord{}, fmt.Errorf("lease key %d is empty", index)
		}
	}
	return record, nil
}

func leaseMetadataStorageID(key []byte) (int64, error) {
	if !bytes.HasPrefix(key, leaseStoragePrefix) {
		return 0, markInvalidLeaseMetadata(fmt.Errorf("lease metadata key %q is outside prefix %q", key, leaseStoragePrefix))
	}
	id, err := strconv.ParseInt(string(key[len(leaseStoragePrefix):]), 10, 64)
	if err != nil || !bytes.Equal(key, leaseStorageKey(id)) {
		return 0, markInvalidLeaseMetadata(fmt.Errorf("lease metadata key %q is not canonical", key))
	}
	return id, nil
}

func (m *leaseManager) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (_ *etcdserverpb.LeaseGrantResponse, retErr error) {
	m.srv.metricCli.EmitCounter("lease.grant", 1)
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh && m.srv.peers.EtcdProxyEnabled() {
		if req.ID == 0 {
			req.ID = m.nextLeaseID()
		}
		proxyCtx, cancel := withUnaryRequestTimeout(ctx)
		defer cancel()
		proxyCtx, err := m.srv.forwardWriteAuthContext(proxyCtx)
		if err != nil {
			return nil, err
		}
		proxyCtx = m.srv.forwardQuotaAdmissionMember(proxyCtx)
		response, err := m.srv.peers.LeaseGrant(proxyCtx, req)
		response, err = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCGrant, response, err)
		response, err = validateLeaseProxyResponseID(m.srv.metricCli, leaseProxyRPCGrant, req.ID, response, err)
		response, err = validateLeaseGrantProxyPayload(m.srv.metricCli, req, response, err)
		m.srv.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
	// Upstream quotaLeaseServer wraps LeaseServer, so an unavailable configured
	// quota rejects LeaseGrant before EtcdServer allocates an automatic ID or
	// enters auth/raft admission. TiKV quota tracks logical user bytes rather
	// than bbolt file overhead; reaching that logical ceiling is the equivalent
	// preflight boundary. A manually armed NOSPACE alarm remains an apply-time
	// cap below, matching applierV3Capped instead of this outer quota layer.
	// Lease records are outside KubeBrain's logical user key/value accounting,
	// so they add no estimated growth here. An already exhausted configured
	// quota is still rejected at the same outer admission boundary as etcd.
	quotaUnavailable, quotaErr := m.srv.configuredQuotaUnavailable(ctx, 0)
	if quotaErr != nil {
		return nil, mapFenceErr(quotaErr)
	}
	if quotaUnavailable {
		return nil, rpctypes.ErrGRPCNoSpace
	}
	explicitID := req.ID != 0
	if !explicitID {
		req.ID = m.nextLeaseID()
	}
	ctx, cancel := withUnaryRequestTimeout(ctx)
	defer cancel()
	if _, err := m.srv.authCallerFromContext(ctx); err != nil {
		return nil, err
	}
	if !leadingFresh {
		return nil, m.leaseLeaderUnavailable("lease grant")
	}
	// A newly elected local leader is published before it has reloaded durable
	// lease/event/checkpoint state. Keep this unary call parked during that
	// bounded startup window instead of rapidly returning Unavailable: grpc-go's
	// finite retry policy can otherwise exhaust all attempts long before the
	// caller's deadline during a PD recovery.
	readyEpoch, err := m.srv.waitLeaderReadyEpoch(ctx)
	if err != nil {
		return nil, err
	}
	epoch = readyEpoch
	if err := m.requireLeaseReady(); err != nil {
		return nil, err
	}
	defer beginEtcdApply(m.srv.metricCli, "LeaseGrant", &retErr)()
	if err := m.srv.rejectCorrupt(ctx); err != nil {
		return nil, err
	}
	_, _, noSpace, err := m.srv.backend.QuotaStatus(ctx)
	if err != nil {
		return nil, mapFenceErr(err)
	}
	if noSpace {
		return nil, rpctypes.ErrGRPCNoSpace
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	m.leaseWriteMu.RLock()
	defer m.leaseWriteMu.RUnlock()
	if req.TTL > maxLeaseTTL {
		return nil, status.Error(codes.OutOfRange, "etcdserver: too large lease TTL")
	}
	ttl := req.TTL
	if ttl < minLeaseTTL {
		ttl = minLeaseTTL
	}

	id := req.ID
	var grantGeneration uint64
	for {
		if id == 0 {
			id = m.nextLeaseID()
		}
		m.leaseMu.Lock()
		_, active := m.leases[id]
		_, pending := m.pendingLeases[id]
		if !active && !pending {
			grantGeneration = m.leaseGeneration
			m.pendingLeases[id] = grantGeneration
			if !explicitID {
				req.ID = id
			}
			m.leaseMu.Unlock()
			break
		}
		m.leaseMu.Unlock()
		if explicitID {
			return nil, status.Error(codes.FailedPrecondition, "etcdserver: lease already exists")
		}
		id = 0
	}

	incarnation, err := newLeaseIncarnation()
	if err != nil {
		m.releasePendingLease(id, grantGeneration)
		return nil, err
	}
	if err := m.srv.backend.EnsureLeaseIncarnationFormatFence(ctx); err != nil {
		m.releasePendingLease(id, grantGeneration)
		return nil, mapFenceErr(err)
	}
	grantMetadata, err := json.Marshal(leaseRecord{ID: id, Incarnation: incarnation, TTL: ttl})
	if err != nil {
		m.releasePendingLease(id, grantGeneration)
		return nil, err
	}
	if err := m.persistLeaseRecord(ctx, leaseStorageKey(id), grantMetadata, true); err != nil {
		// No Put can observe a pending lease, so there are no attached keys to
		// revoke. Exact-incarnation compensation resolves a commit-undetermined
		// InternalPut before releasing the ID reservation. A failed delete retains
		// the reservation for retry; generation, leadership, CORRUPT, and exact-value
		// guards prevent an old grant from deleting a newer leader's metadata.
		m.compensateFailedLeaseGrant(ctx, id, grantGeneration, epoch, grantMetadata)
		return nil, mapFenceErr(err)
	}

	st := &leaseState{
		id:          id,
		incarnation: incarnation,
		ttl:         ttl,
		deadline:    time.Now().Add(time.Duration(ttl) * time.Second),
		keys:        make(map[string]struct{}),
		revoked:     make(chan struct{}),
	}
	m.leaseMu.Lock()
	pendingGeneration, stillPending := m.pendingLeases[id]
	if !stillPending || pendingGeneration != grantGeneration || m.leaseGeneration != grantGeneration {
		m.leaseMu.Unlock()
		m.compensateFailedLeaseGrant(ctx, id, grantGeneration, epoch, grantMetadata)
		return nil, mapFenceErr(backend.ErrLeadershipFenced)
	}
	delete(m.pendingLeases, id)
	m.leases[id] = st
	m.leaseIncarnations.Store(id, incarnation)
	m.scheduleLeaseLocked(st)
	m.scheduleLeaseCheckpointLocked(st)
	m.leaseMu.Unlock()

	emitEtcdLeaseGrantedCounter(m.srv.metricCli, 1)
	emitEtcdLeaseTTLHistogram(m.srv.metricCli, ttl)
	return &etcdserverpb.LeaseGrantResponse{
		Header: txnHeader(int64(m.srv.backend.GetCurrentRevision())),
		ID:     id,
		TTL:    ttl,
	}, nil
}

func (m *leaseManager) releasePendingLease(id int64, generation uint64) bool {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	if current, ok := m.pendingLeases[id]; !ok || current != generation {
		return false
	}
	delete(m.pendingLeases, id)
	return true
}

func (m *leaseManager) pendingLeaseInGeneration(id int64, generation uint64) bool {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	current, ok := m.pendingLeases[id]
	return ok && current == generation && m.leaseGeneration == generation
}

func (m *leaseManager) compensateFailedLeaseGrant(requestCtx context.Context, id int64, generation, epoch uint64, expected []byte) {
	currentEpoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh || currentEpoch != epoch || !m.pendingLeaseInGeneration(id, generation) {
		m.releasePendingLease(id, generation)
		emitLeaseGrantCleanup(m.srv.metricCli, "handoff")
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), unaryRpcTimeout)
	resolved, err := m.deleteFailedLeaseGrantIfMatch(cleanupCtx, id, expected)
	cancel()
	if resolved {
		m.releasePendingLease(id, generation)
		emitLeaseGrantCleanup(m.srv.metricCli, "success")
		return
	}
	if errors.Is(err, errLeaseGrantCleanupSuperseded) {
		m.releasePendingLease(id, generation)
		emitLeaseGrantCleanup(m.srv.metricCli, "handoff")
		return
	}
	emitLeaseGrantCleanup(m.srv.metricCli, "retry")
	klog.ErrorS(err, "lease grant compensation failed; retaining ID reservation for retry", "lease", id)
	m.startWorker(func(workerCtx context.Context) {
		m.retryFailedLeaseGrantCleanup(workerCtx, id, generation, epoch, expected)
	})
}

func (m *leaseManager) retryFailedLeaseGrantCleanup(workerCtx context.Context, id int64, generation, epoch uint64, expected []byte) {
	retryDelay := 100 * time.Millisecond
	for {
		currentEpoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
		if !leadingFresh || currentEpoch != epoch || !m.pendingLeaseInGeneration(id, generation) {
			m.releasePendingLease(id, generation)
			emitLeaseGrantCleanup(m.srv.metricCli, "handoff")
			return
		}
		ctx, cancel := context.WithTimeout(workerCtx, unaryRpcTimeout)
		resolved, err := m.deleteFailedLeaseGrantIfMatch(backend.WithLeadershipEpoch(ctx, epoch), id, expected)
		cancel()
		if resolved {
			m.releasePendingLease(id, generation)
			emitLeaseGrantCleanup(m.srv.metricCli, "success")
			return
		}
		if errors.Is(err, errLeaseGrantCleanupSuperseded) {
			m.releasePendingLease(id, generation)
			emitLeaseGrantCleanup(m.srv.metricCli, "handoff")
			return
		}
		emitLeaseGrantCleanup(m.srv.metricCli, "retry")
		klog.ErrorS(err, "lease grant compensation retry failed", "lease", id, "retryAfter", retryDelay)
		timer := time.NewTimer(retryDelay)
		select {
		case <-workerCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			m.releasePendingLease(id, generation)
			emitLeaseGrantCleanup(m.srv.metricCli, "handoff")
			return
		case <-timer.C:
		}
		if retryDelay < time.Second {
			retryDelay *= 2
			if retryDelay > time.Second {
				retryDelay = time.Second
			}
		}
	}
}

var errLeaseGrantCleanupSuperseded = errors.New("lease grant cleanup metadata was superseded")

func (m *leaseManager) deleteFailedLeaseGrantIfMatch(ctx context.Context, id int64, expected []byte) (bool, error) {
	key := leaseStorageKey(id)
	err := m.srv.backend.InternalCAS(backend.WithCorruptAlarmCommitGuard(ctx), []backend.InternalCASOp{{
		Key: key, Expected: expected, ExpectedExists: true, Delete: true,
	}})
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, storage.ErrCASFailed) {
		return false, err
	}
	current, getErr := m.srv.backend.InternalGet(ctx, key)
	switch {
	case errors.Is(getErr, storage.ErrKeyNotFound):
		return true, nil
	case getErr != nil:
		return false, errors.Join(err, fmt.Errorf("inspect failed lease grant before cleanup: %w", getErr))
	case !bytes.Equal(current, expected):
		return false, errLeaseGrantCleanupSuperseded
	default:
		return false, err
	}
}

func (m *leaseManager) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (_ *etcdserverpb.LeaseRevokeResponse, retErr error) {
	m.srv.metricCli.EmitCounter("lease.revoke", 1)
	ctx, cancel := withUnaryRequestTimeout(ctx)
	defer cancel()
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh && m.srv.peers.EtcdProxyEnabled() {
		proxyCtx, err := m.srv.forwardWriteAuthContext(ctx)
		if err != nil {
			return nil, err
		}
		response, err := m.srv.peers.LeaseRevoke(proxyCtx, req)
		response, err = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCRevoke, response, err)
		m.srv.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
	caller, err := m.srv.authCallerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	ctx = withAuthWriteGuard(ctx, caller)
	if !leadingFresh {
		return nil, m.leaseLeaderUnavailable("lease revoke")
	}
	// Revoke is a durable lease/key mutation and must not race the new leader's
	// lease/event/checkpoint reload. Parking here also avoids consuming grpc-go's
	// finite retry budget on a rapid stream of leadership-fence rejections.
	epoch, err = m.srv.waitLeaderReadyEpoch(ctx)
	if err != nil {
		return nil, err
	}
	if err := m.requireLeaseReady(); err != nil {
		return nil, err
	}
	defer beginEtcdApply(m.srv.metricCli, "LeaseRevoke", &retErr)()
	if err := m.srv.rejectCorrupt(ctx); err != nil {
		return nil, err
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	m.leaseWriteMu.Lock()
	defer m.leaseWriteMu.Unlock()
	m.leaseTeardowns.Add(1)
	defer m.leaseTeardowns.Add(-1)
	for _, key := range m.keysForLease(req.ID) {
		if err = caller.require([]byte(key), nil, authpb.WRITE); err != nil {
			return nil, err
		}
	}
	rev, err := m.revokeLeaseLocked(ctx, req.ID)
	if err != nil {
		if errors.Is(err, storage.ErrUncertainResult) {
			m.scheduleUncertainLeaseRevoke(req.ID, epoch)
		}
		return nil, mapFenceErr(err)
	}
	emitEtcdLeaseRevokedCounter(m.srv.metricCli, 1)
	return &etcdserverpb.LeaseRevokeResponse{
		Header: txnHeader(int64(rev)),
	}, nil
}

var errLeaseRevokePending = status.Error(codes.Unavailable, "etcdserver: lease revoke outcome is pending")

func (m *leaseManager) scheduleUncertainLeaseRevoke(id int64, epoch uint64) {
	m.leaseMu.Lock()
	st := m.leases[id]
	if st == nil || st.revokePending {
		m.leaseMu.Unlock()
		return
	}
	st.revokePending = true
	generation := m.leaseGeneration
	m.leaseMu.Unlock()
	emitLeaseRevokeReconcile(m.srv.metricCli, "retry")
	m.startWorker(func(workerCtx context.Context) {
		m.retryUncertainLeaseRevoke(workerCtx, id, generation, epoch)
	})
}

func (m *leaseManager) retryUncertainLeaseRevoke(workerCtx context.Context, id int64, generation, epoch uint64) {
	retryDelay := 100 * time.Millisecond
	for {
		timer := time.NewTimer(retryDelay)
		select {
		case <-workerCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			emitLeaseRevokeReconcile(m.srv.metricCli, "handoff")
			return
		case <-timer.C:
		}

		currentEpoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
		m.leaseMu.Lock()
		st := m.leases[id]
		stillPending := m.leaseGeneration == generation && st != nil && st.revokePending
		m.leaseMu.Unlock()
		if !leadingFresh || currentEpoch != epoch || !stillPending {
			emitLeaseRevokeReconcile(m.srv.metricCli, "handoff")
			return
		}

		ctx, cancel := context.WithTimeout(workerCtx, unaryRpcTimeout)
		ctx = backend.WithLeadershipEpoch(ctx, epoch)
		m.leaseWriteMu.Lock()
		m.leaseTeardowns.Add(1)
		m.leaseMu.Lock()
		st = m.leases[id]
		stillPending = m.leaseGeneration == generation && st != nil && st.revokePending
		m.leaseMu.Unlock()
		var err error
		if stillPending {
			// TxnApply re-reads the durable CORRUPT generation and stages its
			// guard in the same atomic batch as every key/attachment/meta delete.
			// The RPC-entry check may now be stale, so the retry must retain this
			// backend boundary: an alarm armed after the uncertain response keeps
			// the pending lease intact until an explicit disarm.
			_, err = m.revokeLeaseLocked(ctx, id)
		}
		m.leaseTeardowns.Add(-1)
		m.leaseWriteMu.Unlock()
		cancel()
		if !stillPending {
			emitLeaseRevokeReconcile(m.srv.metricCli, "handoff")
			return
		}
		if err == nil || status.Code(err) == codes.NotFound {
			emitLeaseRevokeReconcile(m.srv.metricCli, "success")
			return
		}
		emitLeaseRevokeReconcile(m.srv.metricCli, "retry")
		klog.ErrorS(err, "lease revoke reconciliation retry failed", "lease", id, "retryAfter", retryDelay)
		if retryDelay < time.Second {
			retryDelay *= 2
			if retryDelay > time.Second {
				retryDelay = time.Second
			}
		}
	}
}

func (m *leaseManager) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	m.srv.metricCli.EmitCounter("lease.keepalive", 1)
	errC := make(chan error, 1)
	go func() {
		errC <- m.leaseKeepAlive(stream)
	}()
	select {
	case err := <-errC:
		return err
	case <-stream.Context().Done():
		return status.FromContextError(stream.Context().Err()).Err()
	}
}

func (m *leaseManager) leaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	// Auth mutations execute on the data-plane leader and invalidate this
	// process's snapshot cache. Once one message has authoritatively observed the
	// auth state in a leadership epoch, later messages in the same stream/epoch
	// can therefore authenticate from that cache. This matches etcd's in-memory
	// auth store and keeps checkpoint-free renewals independent of TiKV Region
	// leader movement. Never carry the proof across an epoch: another leader may
	// have changed auth while this replica was not authoritative.
	var authenticatedEpoch uint64
	var authenticatedInEpoch bool
	errRouteChanged := errors.New("lease keepalive route changed to local leader")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if shouldCountServerStreamFailure(stream.Context(), err) {
				emitEtcdServerStreamFailureCounter(m.srv.metricCli, "receive", "lease-keepalive", 1)
			}
			return err
		}
		// A consumed message may start on a follower and finish after this ingress
		// becomes leader. Re-evaluate the complete route without receiving another
		// message whenever the old peer-forward loop observes that transition.
		for {
			// Route an unapplied follower message before consulting this process's auth
			// snapshot. KubeBrain followers do not receive every auth mutation through a
			// Raft apply loop, so even a complete cache may be stale, and the follower can
			// remain reachable after losing every TiKV/PD path. The leader performs the
			// normal per-message authentication and attachment authorization.
			forwardRequest := func(proxyCtx context.Context) error {
				for {
					resp, keepAliveErr := m.srv.peers.LeaseKeepAlive(proxyCtx, req)
					resp, keepAliveErr = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCKeepAlive, resp, keepAliveErr)
					resp, keepAliveErr = validateLeaseProxyResponseID(m.srv.metricCli, leaseProxyRPCKeepAlive, req.ID, resp, keepAliveErr)
					resp, keepAliveErr = validateLeaseKeepAliveProxyPayload(m.srv.metricCli, resp, keepAliveErr)
					if keepAliveErr == nil {
						m.srv.observeForwardedRevision(resp.GetHeader(), nil)
						return m.sendLeaseKeepAliveResponse(stream, resp)
					}
					// The message has already been consumed from the client stream. Preserve
					// it across a transient leader handoff rather than forcing clientv3 to
					// spend its finite reconnect budget.
					if status.Code(keepAliveErr) != codes.Unavailable {
						return keepAliveErr
					}
					if _, leadingFresh := m.srv.peers.EpochAndLeadingFresh(); leadingFresh {
						return errRouteChanged
					}
					timer := time.NewTimer(100 * time.Millisecond)
					select {
					case <-proxyCtx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						return proxyCtx.Err()
					case <-timer.C:
					}
				}
			}
			if m.srv.peers.EtcdProxyEnabled() {
				_, routeLeadingFresh := m.srv.peers.EpochAndLeadingFresh()
				if !routeLeadingFresh {
					proxyCtx, forwardErr := m.srv.forwardWriteAuthContext(stream.Context())
					if forwardErr != nil {
						return forwardErr
					}
					if forwardErr = forwardRequest(proxyCtx); errors.Is(forwardErr, errRouteChanged) {
						continue
					} else if forwardErr != nil {
						return forwardErr
					}
					break
				}
			}
			// Match etcd's LeaseServer: capture the header before authorization and
			// renewal. A concurrent revoke may advance the store after a successful
			// renew; reporting that later revision would imply the lease survived a
			// revoke already visible at or before the response revision.
			responseRevision := m.srv.backend.GetCurrentRevision()
			// Match etcd's checkLeaseRenew on every message, not only when the
			// long-lived stream is opened. Renewing a lease is a write to every key
			// attached to it; otherwise a caller that only knows the lease ID can keep
			// another tenant's protected keys alive indefinitely. Rechecking per request
			// also observes role/permission changes made while the stream is open.
			var epoch uint64
			var leadingFresh bool
			var caller *authCaller
			var authErr error
			authComplete := false
			// Leadership acquisition publishes the member's auth snapshot before it
			// becomes ready, and every local auth mutation invalidates that snapshot.
			// It is therefore authoritative for the whole current epoch, including the
			// first message of the one-message substreams used by follower forwarding.
			// Without this fast path, every forwarded keepalive re-reads TiKV and loses
			// short leases when the auth-disabled data path's Region changes leader.
			if authenticatedInEpoch {
				epoch, leadingFresh = m.srv.peers.EpochAndLeadingFresh()
				if leadingFresh && authenticatedEpoch == epoch {
					caller, authErr, authComplete = m.srv.authCallerFromCachedContext(stream.Context())
				}
			} else {
				// Only an auth-disabled cached snapshot may bypass the authoritative
				// first-message read. Preserve that read and its revision fence for an
				// authenticated caller.
				cachedCaller, cachedErr, cachedComplete := m.srv.authCallerFromCachedContext(stream.Context())
				if cachedComplete && cachedErr == nil && cachedCaller == nil {
					caller, authErr, authComplete = cachedCaller, cachedErr, true
					epoch, leadingFresh = m.srv.peers.EpochAndLeadingFresh()
				}
			}
			if !authComplete {
				caller, authErr = m.srv.authCallerFromContext(stream.Context())
				// The first message retains the established auth-before-routing error
				// precedence. If a cached snapshot was invalidated while this stream
				// was open, refresh the route after the authoritative read because it
				// may have blocked across a demotion.
				if authErr == nil {
					epoch, leadingFresh = m.srv.peers.EpochAndLeadingFresh()
				}
				if authErr == nil && leadingFresh {
					authenticatedEpoch = epoch
					authenticatedInEpoch = true
				}
			}
			if authErr != nil {
				return authErr
			}
			forward := func() error {
				proxyCtx, forwardErr := m.srv.forwardAuthToken(stream.Context(), caller)
				if forwardErr != nil {
					return forwardErr
				}
				return forwardRequest(proxyCtx)
			}
			if !leadingFresh {
				if !m.srv.peers.EtcdProxyEnabled() {
					return m.leaseLeaderUnavailable("lease keepalive")
				}
				// Unlike an etcd raft follower, this replica's in-memory lessor is a
				// disposable constructor snapshot and does not receive attachment
				// applies. It can validate the token, but authorizing its stale key set
				// would create false denials after the leader detaches or rebinds keys.
				// The forwarded leader receives this verified identity and performs the
				// normal current-snapshot authorization in refreshLeaseAuthorized.
				if err := forward(); errors.Is(err, errRouteChanged) {
					continue
				} else if err != nil {
					return err
				}
				break
			}
			admittedEpoch := epoch
			// Renewal is a durable lease metadata mutation. A newly elected local
			// leader must finish reloading lease/event/checkpoint state before it
			// serves the first request on an existing keepalive stream. Parking the
			// message here also prevents a recovering client stream from rapidly
			// cycling through grpc-go's finite retry budget on startup fence errors.
			if err := m.srv.waitLeaderReady(stream.Context()); err != nil {
				return err
			}
			// waitLeaderReady may span a complete demotion and re-election of this
			// same ingress replica. The already-received stream message has not been
			// applied yet, so admit it under the ready snapshot's current epoch rather
			// than carrying the pre-wait token into the successor term and self-proxying.
			epoch, leadingFresh = m.srv.peers.EpochAndLeadingFresh()
			if !leadingFresh {
				if !m.srv.peers.EtcdProxyEnabled() {
					return m.leaseLeaderUnavailable("lease keepalive")
				}
				if err := forward(); errors.Is(err, errRouteChanged) {
					continue
				} else if err != nil {
					return err
				}
				break
			}
			// Check local readiness only after the post-wait routing decision. A
			// demotion withdraws leaseReady before the stream necessarily observes the
			// new leader; returning the local reload fence in that window would prevent
			// a proxy-capable follower from forwarding this unapplied message.
			if !m.leaseReady.Load() || m.leaseReadyEpoch.Load() != epoch {
				// A successor epoch is not locally usable until ReloadLeases publishes
				// its matching snapshot. Treat this as the old term losing leadership,
				// so proxy-capable ingress can forward instead of leaking a local reload
				// fence. If the epoch did not change, this is an ordinary startup reload.
				if epoch != admittedEpoch {
					if !m.srv.peers.EtcdProxyEnabled() {
						return m.leaseLeaderUnavailable("lease keepalive")
					}
					if err := forward(); errors.Is(err, errRouteChanged) {
						continue
					} else if err != nil {
						return err
					}
					break
				}
				return status.Error(codes.Unavailable, "etcdserver: lease state is reloading")
			}

			renewCtx := backend.WithLeadershipEpoch(stream.Context(), epoch)
			ttl, err := m.refreshLeaseAuthorized(renewCtx, caller, req.ID, epoch)
			if errors.Is(err, errLeaseDemotedDuringRenew) {
				if !m.srv.peers.EtcdProxyEnabled() {
					return m.leaseLeaderUnavailable("lease keepalive")
				}
				if err := forward(); errors.Is(err, errRouteChanged) {
					continue
				} else if err != nil {
					return err
				}
				break
			}
			if status.Code(err) == codes.NotFound {
				ttl = 0
			} else if err != nil {
				return mapFenceErr(err)
			} else {
				emitEtcdLeaseRenewedCounter(m.srv.metricCli, 1)
			}
			if err := m.sendLeaseKeepAliveResponse(stream, &etcdserverpb.LeaseKeepAliveResponse{
				Header: txnHeader(int64(responseRevision)),
				ID:     req.ID,
				TTL:    ttl,
			}); err != nil {
				return err
			}
			break
		}
	}
}

func (m *leaseManager) sendLeaseKeepAliveResponse(stream etcdserverpb.Lease_LeaseKeepAliveServer, resp *etcdserverpb.LeaseKeepAliveResponse) error {
	err := stream.Send(resp)
	if shouldCountServerStreamFailure(stream.Context(), err) {
		emitEtcdServerStreamFailureCounter(m.srv.metricCli, "send", "lease-keepalive", 1)
	}
	return err
}

func (m *leaseManager) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	m.srv.metricCli.EmitCounter("lease.ttl", 1)
	_, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh && m.srv.peers.EtcdProxyEnabled() {
		proxyCtx, err := m.srv.forwardWriteAuthContext(ctx)
		if err != nil {
			return nil, err
		}
		response, err := m.srv.peers.LeaseTimeToLive(proxyCtx, req)
		response, err = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCTimeToLive, response, err)
		response, err = validateLeaseProxyResponseID(m.srv.metricCli, leaseProxyRPCTimeToLive, req.ID, response, err)
		response, err = validateLeaseTimeToLiveProxyPayload(m.srv.metricCli, req.Keys, response, err)
		m.srv.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
	caller, err := m.srv.authCallerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var authRevision uint64
	if req.Keys {
		authRevision, err = m.srv.leaseTimeToLiveAuthRevision(ctx)
		if err != nil {
			return nil, err
		}
	}
	finish := func(response *etcdserverpb.LeaseTimeToLiveResponse, readErr error) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
		if readErr != nil || !req.Keys {
			return response, readErr
		}
		if fenceErr := m.srv.ensureLeaseTimeToLiveAuthRevision(ctx, authRevision); fenceErr != nil {
			return nil, fenceErr
		}
		return response, nil
	}
	forward := func() (*etcdserverpb.LeaseTimeToLiveResponse, error) {
		proxyCtx, forwardErr := m.srv.forwardAuthToken(ctx, caller)
		if forwardErr != nil {
			return nil, forwardErr
		}
		response, forwardErr := m.srv.peers.LeaseTimeToLive(proxyCtx, req)
		response, forwardErr = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCTimeToLive, response, forwardErr)
		response, forwardErr = validateLeaseProxyResponseID(m.srv.metricCli, leaseProxyRPCTimeToLive, req.ID, response, forwardErr)
		response, forwardErr = validateLeaseTimeToLiveProxyPayload(m.srv.metricCli, req.Keys, response, forwardErr)
		m.srv.observeForwardedRevision(response.GetHeader(), forwardErr)
		return finish(response, forwardErr)
	}
	// A follower's lease state is a stale snapshot: the leader advances deadlines
	// via keepalive and grants/revokes leases the follower never observes. Answer
	// only as the leader; otherwise proxy, or fail like the write lease RPCs so the
	// client retries against the leader instead of reading stale local state (#56).
	if err := m.requireLeaseLeader("lease time-to-live"); err != nil {
		if m.srv.peers.EtcdProxyEnabled() {
			return forward()
		}
		return nil, err
	}
	m.leaseMu.Lock()
	// Mirror etcd leaseTimeToLive's post-lookup Demoted check. Leadership may
	// change after the initial routing decision while this RPC waits for the
	// lease snapshot lock; never return that now-stale local state.
	if err := m.requireLeaseLeader("lease time-to-live"); err != nil {
		m.leaseMu.Unlock()
		if m.srv.peers.EtcdProxyEnabled() {
			return forward()
		}
		return nil, err
	}
	finishLocal := func(response *etcdserverpb.LeaseTimeToLiveResponse) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
		// Match etcd's post-lookup Lease.Demoted check. Authorization can read
		// TiKV while leaseMu is held, so leadership may change after the earlier
		// check; never publish that old leader's live or missing snapshot.
		if leaderErr := m.requireLeaseLeader("lease time-to-live"); leaderErr != nil {
			m.leaseMu.Unlock()
			if m.srv.peers.EtcdProxyEnabled() {
				return forward()
			}
			return nil, leaderErr
		}
		m.leaseMu.Unlock()
		return finish(response, nil)
	}
	// Authorize and build the response from the same lease snapshot. Otherwise a
	// concurrent Put can attach a protected key after the permission check but
	// before leaseKeys below, disclosing that key through TTL(Keys=true).
	if req.Keys {
		keys := make([]string, 0)
		if st := m.leases[req.ID]; st != nil {
			keys = make([]string, 0, len(st.keys))
			for key := range st.keys {
				keys = append(keys, key)
			}
		}
		if err := m.authorizeLeaseKeys(ctx, caller, keys, authpb.READ); err != nil {
			m.leaseMu.Unlock()
			return nil, err
		}
	}
	st, ok := m.leases[req.ID]
	if !ok {
		resp := &etcdserverpb.LeaseTimeToLiveResponse{
			Header: txnHeader(int64(m.srv.backend.GetCurrentRevision())),
			ID:     req.ID,
			TTL:    -1,
		}
		return finishLocal(resp)
	}
	if st.revokePending {
		m.leaseMu.Unlock()
		return nil, errLeaseRevokePending
	}

	resp := &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     txnHeader(int64(m.srv.backend.GetCurrentRevision())),
		ID:         req.ID,
		TTL:        remainingTTL(st),
		GrantedTTL: st.ttl,
	}
	if req.Keys {
		resp.Keys = leaseKeys(st)
	}
	return finishLocal(resp)
}

func (m *leaseManager) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	m.srv.metricCli.EmitCounter("lease.leases", 1)
	_, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh && m.srv.peers.EtcdProxyEnabled() {
		proxyCtx, err := m.srv.forwardWriteAuthContext(ctx)
		if err != nil {
			return nil, err
		}
		response, err := m.srv.peers.LeaseLeases(proxyCtx, req)
		response, err = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCLeases, response, err)
		response, err = validateLeaseLeasesProxyPayload(m.srv.metricCli, response, err)
		m.srv.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
	caller, err := m.srv.authCallerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	forward := func() (*etcdserverpb.LeaseLeasesResponse, error) {
		proxyCtx, forwardErr := m.srv.forwardAuthToken(ctx, caller)
		if forwardErr != nil {
			return nil, forwardErr
		}
		response, forwardErr := m.srv.peers.LeaseLeases(proxyCtx, req)
		response, forwardErr = validateLeaseProxyResult(m.srv.metricCli, leaseProxyRPCLeases, response, forwardErr)
		response, forwardErr = validateLeaseLeasesProxyPayload(m.srv.metricCli, response, forwardErr)
		m.srv.observeForwardedRevision(response.GetHeader(), forwardErr)
		return response, forwardErr
	}
	// See LeaseTimeToLive: a follower must not enumerate leases from its stale
	// local snapshot; proxy to the leader or fail (#56).
	if err := m.requireLeaseLeader("lease leases"); err != nil {
		if m.srv.peers.EtcdProxyEnabled() {
			return forward()
		}
		return nil, err
	}
	m.leaseMu.Lock()
	if err := m.requireLeaseLeader("lease leases"); err != nil {
		m.leaseMu.Unlock()
		if m.srv.peers.EtcdProxyEnabled() {
			return forward()
		}
		return nil, err
	}
	// Keep authorization and enumeration on one snapshot. A newly attached
	// protected key must either be included in this check or linearize after the
	// successful list response.
	keys := make([]string, 0, len(m.keyLeaseIndex))
	for key := range m.keyLeaseIndex {
		keys = append(keys, key)
	}
	if err := m.authorizeLeaseKeys(ctx, caller, keys, authpb.READ); err != nil {
		m.leaseMu.Unlock()
		return nil, err
	}
	for _, lease := range m.leases {
		if lease.revokePending {
			m.leaseMu.Unlock()
			return nil, errLeaseRevokePending
		}
	}
	leases := make([]*leaseState, 0, len(m.leases))
	for _, lease := range m.leases {
		leases = append(leases, lease)
	}
	sort.Slice(leases, func(i, j int) bool {
		if leases[i].deadline.Equal(leases[j].deadline) {
			return leases[i].id < leases[j].id
		}
		return leases[i].deadline.Before(leases[j].deadline)
	})
	resp := &etcdserverpb.LeaseLeasesResponse{
		Header: txnHeader(int64(m.srv.backend.GetCurrentRevision())),
		Leases: make([]*etcdserverpb.LeaseStatus, 0, len(leases)),
	}
	for _, lease := range leases {
		resp.Leases = append(resp.Leases, &etcdserverpb.LeaseStatus{ID: lease.id})
	}
	if err := m.requireLeaseLeader("lease leases"); err != nil {
		m.leaseMu.Unlock()
		if m.srv.peers.EtcdProxyEnabled() {
			return forward()
		}
		return nil, err
	}
	m.leaseMu.Unlock()
	return resp, nil
}

func (m *leaseManager) authorizeLeaseKeys(ctx context.Context, caller *authCaller, keys []string, permission authpb.Permission_Type) error {
	if caller == nil || caller.isRoot() {
		return nil
	}
	for _, key := range keys {
		if err := caller.require([]byte(key), nil, permission); err != nil {
			return err
		}
	}
	return m.srv.ensureAuthStoreRevisionUnchangedAfterLeaseAuthorization(ctx, caller)
}

func (m *leaseManager) ensureLeaseExists(id int64) error {
	if id == 0 {
		return nil
	}
	if err := m.requireLeaseReady(); err != nil {
		return err
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	st, ok := m.leases[id]
	if !ok {
		return leaseNotFound(id)
	}
	if st.revokePending {
		return errLeaseRevokePending
	}
	return nil
}

// putLeasedAtomic writes a put together with its per-key lease attachment record
// in ONE atomic backend batch, so a leased key is never durably present without
// its attachment record. This closes the write+attach race (review #2): the old
// path did backend.Put and THEN attachKeyToStorage as a second, error-ignored
// write, so if the attach failed the key survived with no durable binding and
// was never expired when its lease lapsed (an orphan/leak). The value op also
// carries the lease inline in its v2 envelope (review #9). prevLease is the key's
// current in-memory binding: when the put clears the lease (put.Lease == 0) on a
// previously-leased key, the now-stale attachment is deleted in the same batch.
//
// Only the common single-Put path (leased Events, masterlease endpoints) routes
// here; the rarer generic-txn paths keep best-effort binding (documented at their
// call sites), and the expiry-time guard in deleteLeasedKey neutralizes any stale
// record either way.
func (m *leaseManager) putLeasedAtomic(ctx context.Context, put *etcdserverpb.PutRequest, prevLease int64) (*etcdserverpb.PutResponse, error) {
	userKey := string(put.Key)
	ops := []backend.TxnWriteOp{{
		Key: put.Key, Value: put.Value, Lease: put.Lease,
		PrevLeaseKnown: true, PrevLease: prevLease,
	}}
	if put.Lease != 0 {
		incarnation, _ := m.leaseIncarnation(put.Lease)
		ops = append(ops, backend.TxnWriteOp{
			Internal: true,
			Key:      leaseAttachKey(userKey),
			Value:    encodeLeaseAttachmentRecord(put.Lease, incarnation),
		})
	} else {
		// Rebind to leaseless: drop the stale attachment in the same batch. A
		// delete of an absent attachment record is a no-op.
		ops = append(ops, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(userKey)})
	}
	prevKVs := make([]bool, len(ops))
	prevKVs[0] = put.PrevKv
	responses, rev, _, err := m.srv.backend.TxnApply(ctx, ops, nil, prevKVs)
	if err != nil {
		m.reconcileLeaseIndexesAfterUncertain(err, rev, ops, 1)
		return nil, err
	}
	// The durable attachment committed atomically with the value above; update
	// only the in-memory index here (no separate durable write that could fail).
	m.bindKeyIndexOnly(put.Lease, userKey)
	response := responses[0].GetResponsePut()
	response.Header = txnHeader(int64(rev))
	return response, nil
}

// bindKeyIndexOnly updates the in-memory key->lease index (and per-lease key set)
// without writing the durable attachment record — used after putLeasedAtomic,
// which already persisted the attachment atomically with the value.
func (m *leaseManager) bindKeyIndexOnly(id int64, key string) {
	m.leaseMu.Lock()
	m.bindKeyToLeaseLocked(id, key)
	m.leaseMu.Unlock()
}

func (m *leaseManager) unbindKeyIndexOnly(key string) {
	m.leaseMu.Lock()
	if id, ok := m.keyLeaseIndex[key]; ok {
		if st := m.leases[id]; st != nil {
			delete(st.keys, key)
		}
		delete(m.keyLeaseIndex, key)
	}
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	m.leaseMu.Unlock()
}

// withLeaseAttachmentOps appends internal attachment mutations after the user
// writes. The returned userCount lets response/result handling ignore those
// implementation-only ops.
func (m *leaseManager) withLeaseAttachmentOps(writes []backend.TxnWriteOp) ([]backend.TxnWriteOp, int) {
	userCount := len(writes)
	out := append([]backend.TxnWriteOp(nil), writes...)
	for i, op := range writes {
		key := string(op.Key)
		previous := m.leaseIDForKey(key)
		op.PrevLeaseKnown = true
		op.PrevLease = previous
		out[i] = op
		switch {
		case op.Delete && previous != 0:
			out = append(out, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(key)})
		case !op.Delete && op.Lease != 0:
			incarnation, _ := m.leaseIncarnation(op.Lease)
			out = append(out, backend.TxnWriteOp{Internal: true, Key: leaseAttachKey(key), Value: encodeLeaseAttachmentRecord(op.Lease, incarnation)})
		case !op.Delete && previous != 0:
			out = append(out, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(key)})
		}
	}
	return out, userCount
}

func (m *leaseManager) applyLeaseIndexes(writes []backend.TxnWriteOp, results []backend.TxnWriteResult, userCount int) {
	for i := 0; i < userCount; i++ {
		if writes[i].Delete {
			if results[i].Deleted {
				m.unbindKeyIndexOnly(string(writes[i].Key))
			}
			continue
		}
		m.bindKeyIndexOnly(writes[i].Lease, string(writes[i].Key))
	}
}

func (m *leaseManager) reconcileLeaseIndexesAfterUncertain(err error, revision uint64, writes []backend.TxnWriteOp, userCount int) {
	if !errors.Is(err, storage.ErrUncertainResult) || revision == 0 || userCount == 0 {
		return
	}
	keys := make([]string, 0, userCount)
	for i := 0; i < userCount; i++ {
		attachmentKey := leaseAttachKey(string(writes[i].Key))
		for j := userCount; j < len(writes); j++ {
			if string(writes[j].Key) == string(attachmentKey) {
				keys = append(keys, string(writes[i].Key))
				break
			}
		}
	}
	if len(keys) == 0 {
		return
	}
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		return
	}
	m.leaseMu.Lock()
	generation := m.leaseGeneration
	m.leaseMu.Unlock()
	m.startWorker(func(ctx context.Context) {
		m.reconcileLeaseIndexesAtRevision(ctx, revision, keys, epoch, generation)
	})
}

// reconcileLeaseIndexesAtRevision waits until the backend has resolved an
// uncertain transaction, then rebuilds the affected in-memory bindings from the
// durable attachment records. The exclusive lease-write lock orders this repair
// against every later attach, detach, revoke, and expiry operation.
func (m *leaseManager) reconcileLeaseIndexesAtRevision(workerCtx context.Context, revision uint64, keys []string, epoch, generation uint64) {
	for m.srv.backend.GetCurrentRevision() < revision {
		if current, leading := m.srv.peers.EpochAndLeadingFresh(); !leading || current != epoch {
			return
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-workerCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}

	retryDelay := 100 * time.Millisecond
	for {
		m.leaseWriteMu.Lock()
		if current, leading := m.srv.peers.EpochAndLeadingFresh(); !leading || current != epoch {
			m.leaseWriteMu.Unlock()
			return
		}
		bindings := make(map[string]int64, len(keys))
		retry := false
		for _, key := range keys {
			ctx, cancel := context.WithTimeout(workerCtx, unaryRpcTimeout)
			value, err := m.srv.backend.InternalGet(ctx, leaseAttachKey(key))
			cancel()
			switch {
			case err == nil:
				id, parseErr := parseLeaseAttachmentRecord(key, value)
				if parseErr != nil {
					m.leaseWriteMu.Unlock()
					m.srv.metricCli.EmitCounter("lease.uncertain_reconcile.err", 1)
					klog.ErrorS(parseErr, "invalid durable lease attachment during uncertain reconciliation",
						"key", key, "revision", revision)
					return
				}
				bindings[key] = id
			case errors.Is(err, storage.ErrKeyNotFound):
				bindings[key] = 0
			default:
				retry = true
				m.srv.metricCli.EmitCounter("lease.uncertain_reconcile.retry", 1)
				klog.ErrorS(err, "durable lease attachment unavailable during uncertain reconciliation",
					"key", key, "revision", revision, "retryAfter", retryDelay)
			}
			if retry {
				break
			}
		}
		if !retry {
			if current, leading := m.srv.peers.EpochAndLeadingFresh(); !leading || current != epoch {
				m.leaseWriteMu.Unlock()
				return
			}
			m.leaseMu.Lock()
			if m.leaseGeneration != generation {
				m.leaseMu.Unlock()
				m.leaseWriteMu.Unlock()
				return
			}
			for key, id := range bindings {
				m.bindKeyToLeaseLocked(id, key)
			}
			m.leaseMu.Unlock()
			m.leaseWriteMu.Unlock()
			m.srv.metricCli.EmitCounter("lease.uncertain_reconcile.success", 1)
			return
		}
		m.leaseWriteMu.Unlock()
		timer := time.NewTimer(retryDelay)
		select {
		case <-workerCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if retryDelay < time.Second {
			retryDelay *= 2
			if retryDelay > time.Second {
				retryDelay = time.Second
			}
		}
	}
}

func (m *leaseManager) bindKeyToLease(ctx context.Context, id int64, key string) {
	m.leaseMu.Lock()
	_, hadPrevious := m.keyLeaseIndex[key]
	m.bindKeyToLeaseLocked(id, key)
	// Read back the resulting binding under the lock to decide the durable action.
	boundTo, bound := m.keyLeaseIndex[key]
	m.leaseMu.Unlock()
	// Persist only THIS key's attachment (O(1)), never the whole key-list (#17).
	// A rebind (previous lease -> id) needs no write to the previous lease: the
	// attachment record is keyed by the user key, so the new id overwrites it.
	switch {
	case bound && boundTo == id:
		_ = m.attachKeyToStorage(ctx, id, key)
	case hadPrevious:
		// key had an attachment but is now unbound (id==0 or the lease was absent):
		// clear the stale attachment record.
		_ = m.detachKeyFromStorage(ctx, key)
	}
	// else: a plain Put of a never-leased key — no lease bookkeeping, no write.
}

func (m *leaseManager) bindKeyToLeaseLocked(id int64, key string) []*leaseState {
	// caller holds leaseMu; keep the lock-free lease count in sync (multiple
	// return paths, so update on exit). Closure so len() is read at return, not
	// captured at defer registration.
	defer func() { atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex))) }()
	var changed []*leaseState
	if previousID, ok := m.keyLeaseIndex[key]; ok {
		if previous, exists := m.leases[previousID]; exists {
			delete(previous.keys, key)
			changed = append(changed, previous)
		}
		delete(m.keyLeaseIndex, key)
	}
	if id == 0 {
		return changed
	}
	st, ok := m.leases[id]
	if !ok {
		return changed
	}
	st.keys[key] = struct{}{}
	m.keyLeaseIndex[key] = id
	changed = append(changed, st)
	return changed
}

func (m *leaseManager) unbindKeyFromLease(ctx context.Context, key string) {
	m.leaseMu.Lock()
	_, wasBound := m.keyLeaseIndex[key]
	if id, ok := m.keyLeaseIndex[key]; ok {
		if st, exists := m.leases[id]; exists {
			delete(st.keys, key)
		}
		delete(m.keyLeaseIndex, key)
	}
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	m.leaseMu.Unlock()
	if wasBound {
		// Drop just this key's attachment record (O(1)), not the whole list (#17).
		_ = m.detachKeyFromStorage(ctx, key)
	}
}

func (m *leaseManager) leaseIDForKey(key string) int64 {
	// Fast path: when no key holds a lease, skip the mutex entirely. This is the
	// common case for reads (most ranges cover leaseless keys) and keeps the read
	// hot path from serializing on leaseMu once per returned KeyValue. A key that
	// truly has a committed lease is always counted here, so this never misses one;
	// it only races a concurrent bind, for which returning 0 is acceptable (that
	// write is not ordered before this read).
	if atomic.LoadInt64(&m.leasedKeyCount) == 0 {
		return 0
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	return m.keyLeaseIndex[key]
}

func (m *leaseManager) leaseIncarnation(id int64) (string, bool) {
	value, ok := m.leaseIncarnations.Load(id)
	if !ok {
		return "", false
	}
	return value.(string), true
}

func (m *leaseManager) refreshLease(ctx context.Context, id int64) (int64, error) {
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		return 0, errLeaseDemotedDuringRenew
	}
	return m.refreshLeaseUnrestricted(ctx, id, epoch)
}

func (m *leaseManager) refreshLeaseUnrestricted(ctx context.Context, id int64, epoch uint64) (int64, error) {
	unlockCheckpoint := m.lockLeaseCheckpoint(id)
	writeLocked := m.leaseWriteMu.TryRLock()
	if !writeLocked && m.leaseTeardowns.Load() != 0 {
		if ttl, renewed := m.refreshUncheckpointedLeaseDuringUnrelatedTeardown(id, epoch); renewed {
			unlockCheckpoint()
			return ttl, nil
		}
	}
	if !writeLocked {
		// TryRLock may have failed just before a teardown owner published its
		// marker. Waiting is the safe fallback for every non-teardown writer and
		// for the lease currently being revoked or expired.
		m.leaseWriteMu.RLock()
	}
	// Serialize against revoke/expiry. In particular, an expiry callback that
	// already won the exclusive lock must finish before this renewal, while a
	// successful renewal prevents expiry from observing the old deadline.
	return m.refreshLeaseHoldingLocks(ctx, id, epoch, m.leaseWriteMu.RUnlock, m.leaseWriteMu.RLock, nil, func() {
		m.leaseWriteMu.RUnlock()
		unlockCheckpoint()
	})
}

// lockLeaseCheckpoint serializes persisted remaining-TTL transitions for one
// lease while allowing unrelated leases to renew concurrently. Stripes keep the
// lock set bounded; collisions only reduce concurrency and never correctness.
// The barrier's read side also prevents reload/withdrawal from replacing the
// complete in-memory lease generation while a transition is in flight.
func (m *leaseManager) lockLeaseCheckpoint(id int64) func() {
	m.leaseCheckpointMu.RLock()
	stripe := &m.leaseCheckpointLocks[uint64(id)%uint64(len(m.leaseCheckpointLocks))]
	stripe.Lock()
	return func() {
		stripe.Unlock()
		m.leaseCheckpointMu.RUnlock()
	}
}

// refreshUncheckpointedLeaseDuringUnrelatedTeardown is the narrow fast path
// corresponding to etcd lessor.Renew's in-memory l.refresh(0). The caller holds
// this lease's checkpoint stripe and has proved that the exclusive global writer is a lease
// teardown. A teardown of this same lease can only reach backend I/O after its
// deadline has expired (natural expiry) or may linearize after this concurrent
// renew (explicit revoke); teardown of another lease cannot affect this state.
func (m *leaseManager) refreshUncheckpointedLeaseDuringUnrelatedTeardown(id int64, epoch uint64) (int64, bool) {
	if currentEpoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh(); !leadingFresh || currentEpoch != epoch {
		return 0, false
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	st := m.leases[id]
	if st == nil || st.remainingTTL != 0 || !st.deadline.After(time.Now()) {
		return 0, false
	}
	st.deadline = time.Now().Add(time.Duration(st.ttl) * time.Second)
	m.scheduleLeaseLocked(st)
	m.scheduleLeaseCheckpointLocked(st)
	return st.ttl, true
}

func (m *leaseManager) refreshLeaseAuthorized(ctx context.Context, caller *authCaller, id int64, epoch uint64) (int64, error) {
	if caller == nil || caller.isRoot() {
		return m.refreshLeaseUnrestricted(ctx, id, epoch)
	}

	// Put/Txn use the shared side of leaseWriteMu from lease validation through
	// binding publication. Hold the exclusive side while checking every current
	// key and refreshing the deadline, so a protected attachment cannot commit
	// between authorization and renewal.
	unlockCheckpoint := m.lockLeaseCheckpoint(id)
	m.leaseWriteMu.Lock()
	if err := m.authorizeLeaseKeys(ctx, caller, m.keysForLease(id), authpb.WRITE); err != nil {
		m.leaseWriteMu.Unlock()
		unlockCheckpoint()
		return 0, err
	}
	return m.refreshLeaseHoldingLocks(ctx, id, epoch, m.leaseWriteMu.Unlock, m.leaseWriteMu.Lock, func() error {
		return m.authorizeLeaseKeys(ctx, caller, m.keysForLease(id), authpb.WRITE)
	}, func() {
		m.leaseWriteMu.Unlock()
		unlockCheckpoint()
	})
}

// refreshLeaseHoldingLocks renews a lease while the caller holds
// its checkpoint stripe and either side of leaseWriteMu. A checkpoint clear drops
// only leaseWriteMu while its guarded CAS is in flight so Revoke is never
// blocked on a slow metadata write; lockWrite reacquires the same lock mode and
// unlock releases both locks.
func (m *leaseManager) refreshLeaseHoldingLocks(
	ctx context.Context,
	id int64,
	epoch uint64,
	unlockWrite, lockWrite func(),
	afterRelock func() error,
	unlock func(),
) (int64, error) {
	// The initial routing decision precedes the renewal locks. If leadership is
	// lost while waiting for a slow checkpoint/revoke, the old leader must not
	// extend only its private in-memory deadline and report a successful renew.
	// Match etcd lessor.Renew returning ErrNotPrimary after demotion; the caller
	// then forwards to the current leader when proxying is enabled.
	if currentEpoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh(); !leadingFresh || currentEpoch != epoch {
		unlock()
		return 0, errLeaseDemotedDuringRenew
	}
	m.leaseMu.Lock()
	st, ok := m.leases[id]
	if !ok {
		m.leaseMu.Unlock()
		unlock()
		return 0, leaseNotFound(id)
	}
	if st.revokePending {
		m.leaseMu.Unlock()
		unlock()
		return 0, errLeaseRevokePending
	}
	now := time.Now()
	// Match etcd lessor.Renew: a lease whose deadline has passed cannot be
	// resurrected merely because its asynchronous revoke callback was delayed.
	// Wait for atomic key+metadata revocation before reporting not-found, so the
	// response cannot race attached keys that are still readable.
	if !st.deadline.After(now) {
		revoked := st.revoked
		m.leaseMu.Unlock()
		unlock()
		select {
		case <-revoked:
			return 0, leaseNotFound(id)
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	checkpointed := st.remainingTTL > 0
	previousRemainingTTL := st.remainingTTL
	ttl := st.ttl
	incarnation := st.incarnation
	m.leaseMu.Unlock()
	if checkpointed {
		// Match etcd lessor.Renew: clear a persisted remaining-TTL checkpoint
		// before publishing the renewed full-TTL deadline. At most one such write
		// occurs per checkpoint interval, not per keepalive. Do not hold
		// leaseWriteMu across the TiKV write: Revoke must be able to delete the
		// lease concurrently. The exact-value CAS cannot recreate metadata after
		// that delete, and the original state pointer below fences revoke/regrant
		// of the same lease ID.
		unlockWrite()
		applyStart := time.Now()
		err := m.persistLeaseCheckpointCASIncarnation(ctx, id, ttl, previousRemainingTTL, 0, incarnation)
		emitEtcdLeaseCheckpointDurations(m.srv.metricCli, time.Since(applyStart), err)
		lockWrite()
		m.leaseMu.Lock()
		current := m.leases[id]
		m.leaseMu.Unlock()
		if current != st {
			unlock()
			return 0, leaseNotFound(id)
		}
		if afterRelock != nil {
			if authErr := afterRelock(); authErr != nil {
				unlock()
				return 0, authErr
			}
		}
		// The checkpoint CAS and its authorization fence can both block on TiKV.
		// Revalidate after reacquiring the renewal lock as well; otherwise a
		// demoted manager could still publish a private deadline after this method's
		// entry fence passed.
		if currentEpoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh(); !leadingFresh || currentEpoch != epoch {
			unlock()
			return 0, errLeaseDemotedDuringRenew
		}
		if err != nil {
			unlock()
			return 0, err
		}
	}
	m.leaseMu.Lock()
	st, ok = m.leases[id]
	if !ok {
		m.leaseMu.Unlock()
		unlock()
		return 0, leaseNotFound(id)
	}
	if st.revokePending {
		m.leaseMu.Unlock()
		unlock()
		return 0, errLeaseRevokePending
	}
	now = time.Now()
	st.deadline = now.Add(time.Duration(st.ttl) * time.Second)
	st.remainingTTL = 0
	m.scheduleLeaseLocked(st)
	m.scheduleLeaseCheckpointLocked(st)
	ttl = st.ttl
	m.leaseMu.Unlock()
	unlock()
	// Mirror etcd lessor.Renew -> l.refresh(0): ordinary keepalives only bump the
	// in-memory deadline. Persistence occurs solely when clearing a periodic
	// remaining-TTL checkpoint, limiting it to at most one write per checkpoint
	// interval and keeping it outside user MVCC.
	return ttl, nil
}

// deleteLeasedKey deletes one key bound to lease id WITHOUT clobbering a value a
// concurrent writer changed out from under us (#1). It compare-deletes at the key's
// current revision, so a Put that reassigned the key to another lease or updated it
// (both mint a new revision) is not wrongly deleted. If the compare fails but the
// key is still bound to THIS same lease (a keepalive re-Put), it retries at the new
// revision; if the key was reassigned to another lease or unbound, it is left
// untouched. A genuine storage error is returned so the caller keeps the lease and
// retries rather than orphaning the surviving keys.
func (m *leaseManager) deleteLeasedKey(ctx context.Context, id int64, key string) error {
	for attempt := 0; attempt < 4; attempt++ {
		// Is the key STILL bound to this lease? A concurrent Put that reassigned it
		// to another lease (or made it leaseless) between the revoke/expire snapshot
		// and now means it is no longer ours to delete. keyLeaseIndex is the
		// authoritative current binding.
		m.leaseMu.Lock()
		boundTo, bound := m.keyLeaseIndex[key]
		m.leaseMu.Unlock()
		if !bound || boundTo != id {
			return nil // reassigned or unbound -> not ours to delete
		}
		resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil // already gone
		}
		// Guard against a stale attachment record (e.g. a best-effort detach that
		// failed, then a leader change reloaded the record) causing us to delete a
		// key that is no longer ours: the current version's inline lease (review #9)
		// is authoritative. If it names a different lease, or none, the key was
		// rebound/recreated — leave it. For legacy v1 values with no inline lease
		// the read falls back to the (rechecked-above) live index, so id matches and
		// this is a no-op — preserving pre-existing behavior for un-upgraded data.
		if resp.Kvs[0].Lease != id {
			return nil
		}
		// Compare-delete at the observed revision so a Put that changes the key
		// between this read and the delete (a keepalive re-Put, or a reassignment
		// whose value write landed but whose index update has not yet) does not get
		// its new value clobbered; a failed compare loops to re-check the binding.
		dresp, err := m.srv.backend.Delete(backend.WithPreviousLease(ctx, id), []byte(key), resp.Kvs[0].ModRevision, false)
		if err != nil {
			return err
		}
		if dresp.GetSucceeded() {
			return nil // deleted exactly the version bound to this lease
		}
		// changed under us -> loop: re-check binding (retry keepalive, skip reassign)
	}
	return nil // extremely rare: a keepalive kept racing; leave it for the next cycle
}

// deleteLeasedKeysAtomic removes every still-live key bound to id in one user
// MVCC revision. Exact mod-revision guards make the whole batch retry if any
// concurrent Put/rebind races the snapshot; attachment records are internal ops
// in the same TiKV transaction and therefore add no revision of their own.
func (m *leaseManager) deleteLeasedKeysAtomic(ctx context.Context, id int64, keys []string) (uint64, error) {
	for attempt := 0; attempt < 4; attempt++ {
		ops := make([]backend.TxnWriteOp, 0, len(keys)*2+1)
		guards := make([]backend.TxnGuard, 0, len(keys))
		for _, key := range keys {
			m.leaseMu.Lock()
			boundTo, bound := m.keyLeaseIndex[key]
			m.leaseMu.Unlock()
			if !bound || boundTo != id {
				continue
			}
			resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
			if err != nil {
				return 0, err
			}
			if len(resp.Kvs) > 0 && resp.Kvs[0].Lease == id {
				ops = append(ops, backend.TxnWriteOp{
					Delete: true, Key: []byte(key), PrevLeaseKnown: true, PrevLease: id,
				})
				guards = append(guards, backend.TxnGuard{Key: []byte(key), Revision: uint64(resp.Kvs[0].ModRevision)})
			}
			// Reclaim the durable binding even if the user key is already gone or
			// no longer carries this lease. leaseWriteMu excludes a concurrent
			// rebind while this snapshot commits.
			ops = append(ops, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(key)})
		}
		// Match etcd lessor.Revoke: lease metadata and every attached key are
		// deleted by one backend transaction. A crash can therefore observe
		// neither side or both, never resurrect an empty lease after its keys
		// were already committed deleted.
		ops = append(ops, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseStorageKey(id)})
		prevKV := make([]bool, len(ops))
		_, rev, _, err := m.srv.backend.TxnApply(ctx, ops, guards, prevKV)
		if errors.Is(err, backend.ErrTxnGuardConflict) {
			continue
		}
		if errors.Is(err, storage.ErrUncertainResult) &&
			m.reconcileLeaseRevoke(ctx, id) == nil {
			return rev, nil
		}
		return rev, err
	}
	// A persistently racing key remains attached; leave the lease intact so the
	// next expiry/revoke attempt can retry instead of orphaning it.
	return 0, status.Error(codes.Unavailable, "etcdserver: lease keys changed during revoke")
}

// reconcileLeaseRevoke resolves a lost response from the atomic revoke batch.
// Lease metadata is deleted in the same TiKV transaction as every user key and
// attachment, so its absence proves the entire revoke committed. Its presence
// proves that this caller must retain the in-memory lease and report the original
// uncertain result.
func (m *leaseManager) reconcileLeaseRevoke(ctx context.Context, id int64) error {
	reconcileCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), leaseMetadataReconciliationTimeout,
	)
	defer cancel()

	_, err := m.srv.backend.InternalGet(reconcileCtx, leaseStorageKey(id))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil
	}
	if err == nil {
		return errors.New("lease metadata remains after uncertain revoke")
	}
	return fmt.Errorf("inspect lease metadata after uncertain revoke: %w", err)
}

// revokeLeaseLocked tears down a lease while the caller holds leaseWriteMu.
func (m *leaseManager) revokeLeaseLocked(ctx context.Context, id int64) (uint64, error) {
	// Delete the attached keys BEFORE removing the lease state/record (#36): if a
	// key delete fails we must keep the lease so the keys are not orphaned (no
	// lease left to ever expire them). Only once every bound key is gone is it
	// safe to drop the lease record and its attachment records.
	keys, ok := m.leaseKeysSnapshot(id)
	if !ok {
		return 0, leaseNotFound(id)
	}
	rev, err := m.deleteLeasedKeysAtomic(ctx, id, keys)
	if err != nil {
		return 0, err
	}
	m.forgetLease(id)
	return rev, nil
}

func (m *leaseManager) expireLease(id int64) {
	m.expireLeaseWithContext(context.Background(), id)
}

func (m *leaseManager) expireLeaseWithContext(workerCtx context.Context, id int64) {
	m.leaseMu.Lock()
	generation := m.leaseGeneration
	m.leaseMu.Unlock()
	epoch, _ := m.srv.peers.EpochAndLeadingFresh()
	m.expireLeaseGenerationWithContext(workerCtx, id, generation, epoch)
}

func (m *leaseManager) expireLeaseGenerationWithContext(workerCtx context.Context, id int64, generation, expectedEpoch uint64) {
	m.leaseWriteMu.Lock()
	defer m.leaseWriteMu.Unlock()
	m.leaseTeardowns.Add(1)
	defer m.leaseTeardowns.Add(-1)

	// time.Timer.Reset cannot prevent a callback that has already started.
	// Recheck under the same operation lock used by refreshLease so a stale
	// callback never revokes a lease whose keepalive moved the deadline forward.
	m.leaseMu.Lock()
	if m.leaseGeneration != generation {
		m.leaseMu.Unlock()
		return
	}
	st, ok := m.leases[id]
	if !ok {
		m.leaseMu.Unlock()
		return
	}
	if st.deadline.After(time.Now()) {
		m.scheduleLeaseLocked(st)
		m.leaseMu.Unlock()
		return
	}
	m.leaseMu.Unlock()

	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh || epoch != expectedEpoch {
		m.retryLeaseExpiry(id, generation)
		return
	}
	// A callback may already be queued when leadership preparation withdraws the
	// old snapshot. Election ownership alone is not enough: acting before the
	// authoritative ReloadLeases for this term can revoke a lease that the
	// previous leader renewed after the snapshot. Keep retrying until the exact
	// epoch has published its fully reloaded lease state.
	if !m.leaseReady.Load() || m.leaseReadyEpoch.Load() != epoch {
		m.retryLeaseExpiry(id, generation)
		return
	}
	// Upstream expired leases are revoked through EtcdServer.LeaseRevoke, so the
	// CORRUPT applier defers both user-key deletion and lease metadata removal.
	// Keep the same boundary here: retaining the complete lease lets the normal
	// expiry retry remove it after the alarm is explicitly disarmed.
	if err := m.srv.rejectCorrupt(workerCtx); err != nil {
		m.srv.metricCli.EmitCounter("lease.expire.corrupt_deferred", 1, errClassTag(err))
		emitLeaseBackgroundFailure(m.srv.metricCli, "expire_corrupt_deferred")
		m.retryLeaseExpiry(id, generation)
		return
	}

	ctx := backend.WithLeadershipEpoch(workerCtx, epoch)
	keys, ok := m.leaseKeysSnapshot(id)
	if !ok {
		return // already removed
	}
	// Delete bound keys first; a missing key returns no error, so a retry after a
	// partial deletion is safe. On a real storage failure keep the lease and retry
	// rather than swallowing the error and orphaning the surviving keys (#36).
	if _, err := m.deleteLeasedKeysAtomic(ctx, id, keys); err != nil {
		m.srv.metricCli.EmitCounter("lease.expire.delete.err", 1)
		emitLeaseBackgroundFailure(m.srv.metricCli, "expire_delete")
		klog.ErrorS(err, "lease expiry: atomic delete of bound keys failed; keeping lease for retry", "lease", id, "keys", len(keys))
		m.retryLeaseExpiry(id, generation)
		return
	}
	// Every bound key is gone; now drop the lease record and attachment records.
	m.forgetLease(id)
	emitEtcdLeaseExpiredCounter(m.srv.metricCli, 1)
	emitEtcdLeaseRevokedCounter(m.srv.metricCli, 1)
}

// leaseKeysSnapshot returns a copy of the keys currently attached to lease id
// without removing the lease, so callers can delete the keys first and only then
// tear the lease down (#36).
func (m *leaseManager) leaseKeysSnapshot(id int64) ([]string, bool) {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	st, ok := m.leases[id]
	if !ok {
		return nil, false
	}
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
	}
	// Match etcd lessor.Revoke: every member deletes attached keys in lexical
	// order, producing a deterministic watch-event sequence and stable hashes.
	sort.Strings(keys)
	return keys, true
}

func (m *leaseManager) retryLeaseExpiry(id int64, generation uint64) {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	if m.leaseGeneration != generation {
		return
	}
	st, ok := m.leases[id]
	if !ok || st.timer == nil {
		return
	}
	st.timer.Reset(leaseExpiryRetryInterval)
}

func (m *leaseManager) forgetLease(id int64) {
	m.leaseMu.Lock()
	st, ok := m.leases[id]
	if !ok {
		m.leaseMu.Unlock()
		return
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	if st.checkpointTimer != nil {
		st.checkpointTimer.Stop()
	}
	for key := range st.keys {
		delete(m.keyLeaseIndex, key)
	}
	delete(m.leases, id)
	m.leaseIncarnations.Delete(id)
	close(st.revoked)
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	m.leaseMu.Unlock()
}

func (m *leaseManager) keysInDeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) ([]string, error) {
	if len(r.RangeEnd) == 0 {
		return []string{string(r.Key)}, nil
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		return nil, nil
	}
	listRequest := &etcdserverpb.RangeRequest{
		Key:      r.Key,
		RangeEnd: r.RangeEnd,
	}
	if m.srv.maxDeleteRangeKeys > 0 {
		listRequest.Limit = int64(m.srv.maxDeleteRangeKeys) + 1
	}
	resp, err := m.srv.backend.List(ctx, listRequest)
	if err != nil {
		return nil, err
	}
	if m.srv.maxDeleteRangeKeys > 0 && len(resp.Kvs) > int(m.srv.maxDeleteRangeKeys) {
		emitDeleteRangeAdmissionRejected(m.srv.metricCli)
		return nil, rpctypes.ErrGRPCRequestTooManyRequests
	}
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys, nil
}

// deleteRangeWithAttachments keeps user tombstones and durable lease attachment
// deletions in the same TiKV transaction. The caller holds leaseWriteMu
// exclusively, so bindings cannot change between this snapshot and commit.
func (m *leaseManager) deleteRangeWithAttachments(ctx context.Context, request *etcdserverpb.DeleteRangeRequest, keys []string) (*etcdserverpb.DeleteRangeResponse, error) {
	mutationKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		mutationKeys = append(mutationKeys, []byte(key))
	}
	var unlock func()
	var err error
	ctx, unlock, err = m.srv.backend.BeginMutation(ctx, mutationKeys...)
	if err != nil {
		return nil, err
	}
	defer unlock()

	writes := make([]backend.TxnWriteOp, 0, len(keys))
	guards := make([]backend.TxnGuard, 0, len(keys))
	for _, key := range keys {
		current, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
		if err != nil {
			return nil, err
		}
		if len(current.Kvs) == 0 {
			continue
		}
		writes = append(writes, backend.TxnWriteOp{Delete: true, Key: []byte(key)})
		guards = append(guards, backend.TxnGuard{Key: []byte(key), Revision: uint64(current.Kvs[0].ModRevision)})
	}
	if len(writes) == 0 {
		revision, err := safeBackendRevision(ctx, m.srv.backend)
		if err != nil {
			return nil, err
		}
		return &etcdserverpb.DeleteRangeResponse{Header: txnHeader(int64(revision))}, nil
	}

	// etcd applies an arbitrarily large DeleteRange through one TxnWrite and
	// therefore one MVCC revision. Keep every user tombstone and attachment
	// mutation in the same TiKV transaction; chunking here would expose partial
	// range deletion and multiple watch revisions.
	allWrites, userCount := m.withLeaseAttachmentOps(writes)
	prevKVs := make([]bool, len(allWrites))
	for i := 0; i < userCount; i++ {
		prevKVs[i] = request.PrevKv
	}
	responses, revision, results, err := m.srv.backend.TxnApply(ctx, allWrites, guards, prevKVs)
	if err != nil {
		m.reconcileLeaseIndexesAfterUncertain(err, revision, allWrites, userCount)
		return nil, err
	}
	response := &etcdserverpb.DeleteRangeResponse{Header: txnHeader(int64(revision))}
	for i := 0; i < userCount; i++ {
		deleted := responses[i].GetResponseDeleteRange()
		response.Deleted += deleted.Deleted
		response.PrevKvs = append(response.PrevKvs, deleted.PrevKvs...)
	}
	m.applyLeaseIndexes(allWrites, results, userCount)
	return response, nil
}

func remainingTTL(st *leaseState) int64 {
	// Match etcd lessor.Lease.Remaining and leaseTimeToLive: an expired lease
	// can remain visible while its asynchronous revoke is blocked (for example
	// by a CORRUPT alarm), and its public TTL continues below zero.
	return int64(time.Until(st.deadline).Seconds())
}

func leaseKeys(st *leaseState) [][]byte {
	keys := make([][]byte, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, []byte(key))
	}
	return keys
}

func leaseNotFound(id int64) error {
	return status.Error(codes.NotFound, "etcdserver: requested lease not found")
}

func (m *leaseManager) requireLeaseReady() error {
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if leadingFresh && m.leaseReady.Load() && m.leaseReadyEpoch.Load() == epoch {
		return nil
	}
	return status.Error(codes.Unavailable, "etcdserver: lease state is reloading")
}

func (m *leaseManager) requireLeaseLeader(op string) error {
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if leadingFresh {
		if m.leaseReady.Load() && m.leaseReadyEpoch.Load() == epoch {
			return nil
		}
		return status.Error(codes.Unavailable, "etcdserver: lease state is reloading")
	}
	return m.leaseLeaderUnavailable(op)
}

func (m *leaseManager) leaseLeaderUnavailable(op string) error {
	m.srv.metricCli.EmitCounter("lease.follower", 1)
	return status.Errorf(codes.Unavailable, "%s error addr is %s leader %s", op, m.srv.backend.GetResourceLock().Identity(), m.srv.peers.GetLeaderInfo())
}

func (m *leaseManager) restoreLeases(ctx context.Context) error {
	// Constructor recovery runs before this replica participates in election.
	// Pin both metadata prefixes to one TiKV TSO snapshot without pretending the
	// replica is leader. This state is disposable and never activates primary
	// workers; ReloadLeases rereads it through the fenced snapshot path on
	// promotion.
	timestamp, err := m.srv.backend.GetFollowerSnapshotTimestamp(ctx)
	if err != nil {
		return err
	}
	ctx = storage.WithSnapshotTimestamp(ctx, timestamp)
	ctx = storage.WithSnapshotIteratorFallback(ctx)
	records, attachments, _, _, err := m.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	// Load into memory only; migration writes happen on leadership (ReloadLeases),
	// since restore runs in New() before this node has necessarily won election.
	// Match etcd lessor.initAndRecover: recovery reconstructs the follower-side
	// lease index, but primary-only expiry/checkpoint work is not armed until
	// leadership promotion (ReloadLeases). Besides avoiding useless follower
	// timers, this prevents constructor-time state from ever racing the first
	// authoritative reload.
	m.applyLeaseRecordsInactive(records, attachments)
	return nil
}

func (m *leaseManager) restoreLeasesAtStartup(parent context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := m.restoreLeases(ctx); err != nil {
		// An empty or partial constructor snapshot is not authoritative. Production
		// leadership reload will reopen this gate only after a complete durable read.
		m.PrepareLeaseReload()
		emitLeaseStartupRestoreFailure(m.srv.metricCli)
		return err
	}
	return nil
}

// ReloadLeases refreshes in-memory lease state from storage. It MUST be called
// when this node acquires leadership: while a follower the node holds a stale
// snapshot (the real leader advanced deadlines via keepalive and granted leases
// this node never saw). Without a reload the new leader would expire
// still-alive leases — deleting their bound keys (e.g. masterleases) — and
// orphan leases granted after this node started.
func (m *leaseManager) ReloadLeases(ctx context.Context) error {
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		return status.Error(codes.Unavailable, "etcdserver: leadership lost during lease reload")
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	records, attachments, legacyAttachments, staleInternalAttachments, err := m.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	m.leaseCheckpointMu.Lock()
	defer m.leaseCheckpointMu.Unlock()
	m.leaseWriteMu.Lock()
	legacy := m.applyLeaseRecords(records, attachments)
	// This node is now the leader; convert any pre-#17 monolithic records to the
	// per-key attachment format so subsequent detaches are durable and the
	// monolithic key-list is not carried forward. One-time, idempotent.
	m.migrateLegacyLeases(ctx, legacy)
	for key, id := range staleInternalAttachments {
		m.reconcileOrphanAttachment(ctx, key, id)
	}
	m.cleanupOrphanLegacyAttachments(ctx, legacyAttachments)
	if err := m.sealLegacyLeaseMigrationIfClean(ctx); err != nil {
		m.srv.metricCli.EmitCounter("lease.legacy_migration_seal.err", 1)
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "seal")
		klog.ErrorS(err, "lease reload: seal legacy migration failed")
	}
	// Release before startOrphanSweeper stops and joins the previous worker: that
	// worker may already be waiting for this same exclusive reconciliation lock.
	m.leaseWriteMu.Unlock()
	currentEpoch, stillLeadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if ctx.Err() != nil || !stillLeadingFresh || currentEpoch != epoch {
		m.clearLeaseStateHoldingCheckpointLock()
		return status.Error(codes.Unavailable, "etcdserver: leadership lost during lease reload")
	}
	// Start the safety-net sweeper that reclaims leased keys whose expiry timer was
	// never (re)armed because their attachment outlived its lease meta record.
	m.startOrphanSweeper(ctx, epoch)
	m.leaseReadyEpoch.Store(epoch)
	m.leaseReady.Store(true)
	return nil
}

// loadLeaseRecords reads the per-lease meta records and the per-key attachment
// records from one storage snapshot.
func (m *leaseManager) loadLeaseRecords(ctx context.Context) ([]leaseRecord, map[string]int64, map[string]int64, map[string]int64, error) {
	if _, pinned := storage.SnapshotTimestampFromContext(ctx); !pinned {
		timestamp, err := m.srv.backend.GetSnapshotTimestamp(ctx)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		ctx = storage.WithSnapshotTimestamp(ctx, timestamp)
		ctx = storage.WithSnapshotIteratorFallback(ctx)
	}
	internalRecords, err := m.srv.backend.InternalRange(ctx, leaseStoragePrefix)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	readLegacy, err := m.shouldReadLegacyLeaseStorage(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var legacyMetaKVs []*mvccpb.KeyValue
	if readLegacy {
		resp, listErr := m.srv.backend.List(ctx, &etcdserverpb.RangeRequest{
			Key:      leaseStoragePrefix,
			RangeEnd: prefixEnd(leaseStoragePrefix),
			Revision: latestRestoreRevision,
		})
		if listErr != nil {
			return nil, nil, nil, nil, listErr
		}
		legacyMetaKVs = resp.Kvs
	}
	// Read the legacy user-MVCC keyspace during rolling upgrades. A new internal
	// record wins when both layouts contain the same lease ID.
	recordByID := make(map[int64]leaseRecord, len(legacyMetaKVs)+len(internalRecords))
	legacyRecordByID := make(map[int64]leaseRecord, len(legacyMetaKVs))
	for _, kv := range legacyMetaKVs {
		keyID, keyErr := leaseMetadataStorageID(kv.Key)
		if keyErr != nil {
			return nil, nil, nil, nil, keyErr
		}
		record, err := decodeLeaseRecord(kv.Value)
		if err != nil {
			return nil, nil, nil, nil, markInvalidLeaseMetadata(fmt.Errorf("decode legacy lease metadata for key %q: %w", string(kv.Key), err))
		}
		if record.ID != keyID {
			return nil, nil, nil, nil, markInvalidLeaseMetadata(fmt.Errorf(
				"legacy lease metadata key id %d disagrees with payload id %d", keyID, record.ID,
			))
		}
		record.LegacyStorage = true
		record.LegacyKeys = append([]string(nil), record.Keys...)
		legacyRecordByID[record.ID] = record
		recordByID[record.ID] = record
	}
	for key, value := range internalRecords {
		keyID, keyErr := leaseMetadataStorageID([]byte(key))
		if keyErr != nil {
			return nil, nil, nil, nil, keyErr
		}
		record, err := decodeLeaseRecord(value)
		if err != nil {
			return nil, nil, nil, nil, markInvalidLeaseMetadata(fmt.Errorf("decode lease metadata: %w", err))
		}
		if record.ID != keyID {
			return nil, nil, nil, nil, markInvalidLeaseMetadata(fmt.Errorf(
				"lease metadata key id %d disagrees with payload id %d", keyID, record.ID,
			))
		}
		if legacy, exists := legacyRecordByID[record.ID]; exists {
			record.LegacyStorage = true
			record.LegacyKeys = append([]string(nil), legacy.Keys...)
		}
		recordByID[record.ID] = record
	}
	records := make([]leaseRecord, 0, len(recordByID))
	for _, record := range recordByID {
		records = append(records, record)
	}

	var legacyAttachmentKVs []*mvccpb.KeyValue
	if readLegacy {
		aresp, listErr := m.srv.backend.List(ctx, &etcdserverpb.RangeRequest{
			Key:      leaseAttachPrefix,
			RangeEnd: prefixEnd(leaseAttachPrefix),
			Revision: latestRestoreRevision,
		})
		if listErr != nil {
			return nil, nil, nil, nil, listErr
		}
		legacyAttachmentKVs = aresp.Kvs
	}
	attachments := make(map[string]int64, len(legacyAttachmentKVs))
	legacyAttachments := make(map[string]int64, len(legacyAttachmentKVs))
	staleInternalAttachments := make(map[string]int64)
	legacyAttachmentKeys := make(map[int64][]string)
	for _, kv := range legacyAttachmentKVs {
		userKey := string(kv.Key[len(leaseAttachPrefix):])
		attachment, perr := parseLeaseAttachmentRecordDetails(userKey, kv.Value)
		if perr != nil {
			return nil, nil, nil, nil, perr
		}
		legacyAttachments[userKey] = attachment.ID
		owner, ownerExists := recordByID[attachment.ID]
		if ownerExists && owner.Incarnation != attachment.Incarnation {
			continue
		}
		attachments[userKey] = attachment.ID
		legacyAttachmentKeys[attachment.ID] = append(legacyAttachmentKeys[attachment.ID], userKey)
	}
	for index := range records {
		records[index].LegacyAttachKeys = append([]string(nil), legacyAttachmentKeys[records[index].ID]...)
	}
	internalAttachments, err := m.srv.backend.InternalRange(ctx, leaseAttachPrefix)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for key, value := range internalAttachments {
		userKey := key[len(leaseAttachPrefix):]
		attachment, perr := parseLeaseAttachmentRecordDetails(userKey, value)
		if perr != nil {
			return nil, nil, nil, nil, perr
		}
		owner, ownerExists := recordByID[attachment.ID]
		if ownerExists && owner.Incarnation != attachment.Incarnation {
			staleInternalAttachments[userKey] = attachment.ID
			continue
		}
		attachments[userKey] = attachment.ID
	}
	legacyOwners := make(map[string]int64)
	for _, record := range records {
		legacyKeys := record.Keys
		if len(record.LegacyKeys) != 0 {
			legacyKeys = record.LegacyKeys
		}
		for _, key := range legacyKeys {
			previous, duplicate := legacyOwners[key]
			if !duplicate || previous == record.ID {
				legacyOwners[key] = record.ID
				continue
			}
			attachmentID, resolved := attachments[key]
			_, ownerExists := recordByID[attachmentID]
			if !resolved || !ownerExists {
				first, second := previous, record.ID
				if first > second {
					first, second = second, first
				}
				return nil, nil, nil, nil, markInvalidLeaseMetadata(fmt.Errorf(
					"legacy lease key %q has conflicting owners %d and %d", key, first, second,
				))
			}
		}
	}
	return records, attachments, legacyAttachments, staleInternalAttachments, nil
}

func (m *leaseManager) shouldReadLegacyLeaseStorage(ctx context.Context) (bool, error) {
	value, err := m.srv.backend.InternalGet(ctx, leaseLegacyMigrationSealKey)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return true, nil
	case err != nil:
		return false, err
	case !bytes.Equal(value, leaseLegacyMigrationSealValue):
		return false, markInvalidLeaseMetadata(fmt.Errorf("legacy lease migration seal has invalid value %q", value))
	default:
		return false, nil
	}
}

// sealLegacyLeaseMigrationIfClean permanently retires the old user-MVCC
// metadata interpretation after every source row has been removed. Once sealed,
// byte keys under the former prefixes are ordinary etcd user keys again. The
// caller excludes online lease writes (or is still in pre-ready ReloadLeases),
// so an old source cannot appear between the empty scans and the seal write.
func (m *leaseManager) sealLegacyLeaseMigrationIfClean(ctx context.Context) error {
	readLegacy, err := m.shouldReadLegacyLeaseStorage(ctx)
	if err != nil || !readLegacy {
		return err
	}
	for _, prefix := range [][]byte{leaseStoragePrefix, leaseAttachPrefix} {
		resp, listErr := m.srv.backend.List(ctx, &etcdserverpb.RangeRequest{
			Key: prefix, RangeEnd: prefixEnd(prefix), Limit: 1, Revision: latestRestoreRevision,
		})
		if listErr != nil {
			return listErr
		}
		if len(resp.Kvs) != 0 {
			return nil
		}
	}
	return m.srv.backend.InternalPutCorruptGuarded(ctx, leaseLegacyMigrationSealKey, leaseLegacyMigrationSealValue)
}

func encodeLeaseAttachmentRecord(id int64, incarnation string) []byte {
	encodedID := strconv.FormatInt(id, 10)
	if incarnation == "" {
		return []byte(encodedID)
	}
	return []byte(encodedID + "@" + incarnation)
}

func parseLeaseAttachmentRecordDetails(userKey string, value []byte) (leaseAttachmentRecord, error) {
	if userKey == "" {
		return leaseAttachmentRecord{}, markInvalidLeaseMetadata(errors.New("lease attachment has empty user key"))
	}
	encodedID, incarnation, found := strings.Cut(string(value), "@")
	id, err := strconv.ParseInt(encodedID, 10, 64)
	if err != nil {
		return leaseAttachmentRecord{}, markInvalidLeaseMetadata(fmt.Errorf("decode lease attachment for key %q: %w", userKey, err))
	}
	if id == 0 {
		return leaseAttachmentRecord{}, markInvalidLeaseMetadata(fmt.Errorf("lease attachment for key %q references reserved id 0", userKey))
	}
	if encodedID != strconv.FormatInt(id, 10) || (found && !validLeaseIncarnation(incarnation)) {
		return leaseAttachmentRecord{}, markInvalidLeaseMetadata(fmt.Errorf("lease attachment for key %q has noncanonical value %q", userKey, value))
	}
	return leaseAttachmentRecord{ID: id, Incarnation: incarnation}, nil
}

func parseLeaseAttachmentRecord(userKey string, value []byte) (int64, error) {
	record, err := parseLeaseAttachmentRecordDetails(userKey, value)
	return record.ID, err
}

// migrateLegacyLeases rewrites pre-#17 leases (meta records that still carried an
// inline key list) into the new format: one attachment record per currently-bound
// key plus a keyless meta record. Idempotent; only runs on the leader.
func (m *leaseManager) migrateLegacyLeases(ctx context.Context, migrations []legacyLeaseMigration) {
	for _, migration := range migrations {
		id := migration.id
		m.leaseMu.Lock()
		st, ok := m.leases[id]
		var ttl int64
		var remainingTTL int64
		var incarnation string
		if ok {
			ttl = st.ttl
			remainingTTL = st.remainingTTL
			incarnation = st.incarnation
		}
		m.leaseMu.Unlock()
		if !ok {
			continue
		}
		complete := true
		for _, k := range migration.attachKeys {
			if err := m.attachKeyToStorage(ctx, id, k); err != nil {
				emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "migration")
				complete = false
				break
			}
		}
		if !complete {
			continue
		}
		if err := m.persistMigratedLeaseMetaIncarnation(ctx, id, ttl, remainingTTL, incarnation); err != nil {
			emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "migration")
			continue
		}
		// Keep the monolithic legacy lease row as a durable retry marker until all
		// legacy attachment rows are retired. If any cleanup is fenced or fails,
		// the next leadership reload rediscovers the source even though canonical
		// internal metadata already exists.
		cleanupComplete := true
		for _, k := range migration.cleanupKeys {
			if _, err := m.srv.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseAttachKey(k)}); err != nil {
				emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "migration")
				cleanupComplete = false
				break
			}
		}
		if cleanupComplete {
			if _, err := m.srv.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseStorageKey(id)}); err != nil {
				emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "migration")
			}
		}
	}
}

// applyLeaseRecords atomically replaces the in-memory lease state from storage:
// it stops existing expiry timers, rebuilds the lease map from the meta records
// and the key->lease bindings from the per-key attachment records, and schedules
// timers from a fresh now+TTL+election-window deadline. Any lease whose meta still carried an
// inline key list (a pre-#17 record) is included in the returned slice so the
// leader can migrate it to the new per-key format. Safe on both first restore
// (empty maps) and leadership reload.
func (m *leaseManager) applyLeaseRecords(records []leaseRecord, attachments map[string]int64) (legacy []legacyLeaseMigration) {
	return m.applyLeaseRecordsWithPrimary(records, attachments, true)
}

func (m *leaseManager) applyLeaseRecordsInactive(records []leaseRecord, attachments map[string]int64) (legacy []legacyLeaseMigration) {
	return m.applyLeaseRecordsWithPrimary(records, attachments, false)
}

func (m *leaseManager) applyLeaseRecordsWithPrimary(records []leaseRecord, attachments map[string]int64, primary bool) (legacy []legacyLeaseMigration) {
	now := time.Now()
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	m.leaseGeneration++
	for _, st := range m.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
		if st.checkpointTimer != nil {
			st.checkpointTimer.Stop()
		}
	}
	m.leases = make(map[int64]*leaseState, len(records))
	m.clearLeaseIncarnations()
	m.keyLeaseIndex = make(map[string]int64)
	for _, record := range records {
		// Match etcd lessor.initAndRecover: old or externally restored lease
		// metadata may predate the current minimum. Keep the persisted checkpoint
		// as the failover deadline bound, but never expose or renew a granted TTL
		// below MinLeaseTTL.
		grantedTTL := record.TTL
		if grantedTTL < minLeaseTTL {
			grantedTTL = minLeaseTTL
		}
		recoveryTTL := grantedTTL
		if record.RemainingTTL > 0 {
			recoveryTTL = record.RemainingTTL
		}
		st := &leaseState{
			id:           record.ID,
			incarnation:  record.Incarnation,
			ttl:          grantedTTL,
			remainingTTL: record.RemainingTTL,
			// Mirror etcd initAndRecover + Promote->refresh: a durable remaining
			// TTL checkpoint bounds failover extension for long leases. Without a
			// checkpoint, use the granted TTL; legacy absolute deadlines remain
			// ignored because keepalive never maintained them.
			deadline: now.Add(leaseRecoveryDuration(recoveryTTL, m.leasePromotionExtension)),
			keys:     make(map[string]struct{}, len(record.Keys)),
			revoked:  make(chan struct{}),
		}
		// Legacy (pre-#17) monolithic key list, if present. New records carry none.
		for _, key := range record.Keys {
			if previousID, bound := m.keyLeaseIndex[key]; bound && previousID != st.id {
				if previous := m.leases[previousID]; previous != nil {
					delete(previous.keys, key)
				}
			}
			st.keys[key] = struct{}{}
			m.keyLeaseIndex[key] = st.id
		}
		m.leases[st.id] = st
		m.leaseIncarnations.Store(st.id, st.incarnation)
	}
	// Per-key attachment records (#17). Attachments for a lease that no longer has
	// a meta record (revoked, tombstone not yet compacted) are skipped as orphans.
	for key, id := range attachments {
		st, ok := m.leases[id]
		if !ok {
			continue
		}
		if previousID, bound := m.keyLeaseIndex[key]; bound && previousID != id {
			if previous := m.leases[previousID]; previous != nil {
				delete(previous.keys, key)
			}
		}
		st.keys[key] = struct{}{}
		m.keyLeaseIndex[key] = id
	}
	legacy = m.legacyMigrationsLocked(records)
	if !primary {
		atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
		return legacy
	}

	// Match etcd lessor.Promote: a large recovered lease set commonly has the
	// same reconstructed deadline. Spread that pile-up before arming timers so a
	// leader change cannot trigger an unbounded burst of revoke transactions.
	restored := make([]*leaseState, 0, len(m.leases))
	for _, st := range m.leases {
		restored = append(restored, st)
	}
	spreadLeaseExpiries(restored, defaultLeaseRevokeRate)

	// Schedule timers once all keys are attached and promotion spreading is
	// complete.
	for _, st := range restored {
		m.scheduleLeaseLocked(st)
		m.scheduleLeaseCheckpointLocked(st)
		if !st.deadline.After(now) {
			st.timer.Reset(0)
		}
	}
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	return legacy
}

// legacyMigrationsLocked derives retry work from durable source markers and the
// current authoritative in-memory bindings. The caller holds leaseMu. Keeping
// this separate from applyLeaseRecords lets the periodic orphan sweep retry a
// transiently failed rolling-upgrade cleanup without resetting lease deadlines.
func (m *leaseManager) legacyMigrationsLocked(records []leaseRecord) (legacy []legacyLeaseMigration) {
	for _, record := range records {
		if !record.LegacyStorage && len(record.Keys) == 0 && len(record.LegacyAttachKeys) == 0 {
			continue
		}
		st := m.leases[record.ID]
		migration := legacyLeaseMigration{id: record.ID}
		if st != nil {
			migration.attachKeys = make([]string, 0, len(st.keys))
			for key := range st.keys {
				migration.attachKeys = append(migration.attachKeys, key)
			}
		}
		cleanupKeys := make(map[string]struct{}, len(record.LegacyKeys)+len(record.Keys)+len(record.LegacyAttachKeys))
		for _, key := range record.LegacyKeys {
			cleanupKeys[key] = struct{}{}
		}
		if len(record.LegacyKeys) == 0 {
			for _, key := range record.Keys {
				cleanupKeys[key] = struct{}{}
			}
		}
		for _, key := range record.LegacyAttachKeys {
			cleanupKeys[key] = struct{}{}
		}
		migration.cleanupKeys = make([]string, 0, len(cleanupKeys))
		for key := range cleanupKeys {
			migration.cleanupKeys = append(migration.cleanupKeys, key)
		}
		legacy = append(legacy, migration)
	}
	return legacy
}

func leaseRecoveryDuration(ttl int64, extension time.Duration) time.Duration {
	duration := time.Duration(ttl) * time.Second
	if extension <= 0 {
		return duration
	}
	if extension > time.Duration(math.MaxInt64)-duration {
		return time.Duration(math.MaxInt64)
	}
	return duration + extension
}

func spreadLeaseExpiries(leases []*leaseState, revokeRate int) {
	if revokeRate <= 0 || len(leases) < revokeRate {
		return
	}
	sort.Slice(leases, func(i, j int) bool {
		if leases[i].deadline.Equal(leases[j].deadline) {
			return leases[i].id < leases[j].id
		}
		return leases[i].deadline.Before(leases[j].deadline)
	})

	baseWindow := leases[0].deadline
	nextWindow := baseWindow.Add(time.Second)
	expires := 0
	targetExpiresPerSecond := (3 * revokeRate) / 4
	if targetExpiresPerSecond == 0 {
		targetExpiresPerSecond = 1
	}
	for _, st := range leases {
		if st.deadline.After(nextWindow) {
			baseWindow = st.deadline
			nextWindow = baseWindow.Add(time.Second)
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := time.Duration(float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond)))
		rateDelay -= st.deadline.Sub(baseWindow)
		nextWindow = baseWindow.Add(rateDelay)
		st.deadline = st.deadline.Add(rateDelay)
	}
}

// StopLeases stops every expiry timer and drops the in-memory lease snapshot. It
// MUST be called when this node loses leadership: otherwise a demoted leader
// keeps firing expiry timers (churn, and it would act on a stale snapshot) and
// answers lease reads from state the new leader has since advanced. Leadership
// re-acquisition rebuilds the state from storage via ReloadLeases (#57).
func (m *leaseManager) StopLeases() {
	m.stopLeases()
}

// PrepareLeaseReload withdraws the stale follower snapshot before leadership is
// published. ReloadLeases opens the gate only after the durable snapshot is
// completely installed.
func (m *leaseManager) PrepareLeaseReload() {
	m.leaseReady.Store(false)
	m.leaseReadyEpoch.Store(0)
}

func (m *leaseManager) stopLeases() {
	m.leaseReady.Store(false)
	m.leaseReadyEpoch.Store(0)
	m.leaseCheckpointMu.Lock()
	defer m.leaseCheckpointMu.Unlock()
	m.clearLeaseStateHoldingCheckpointLock()
}

// clearLeaseStateHoldingCheckpointLock withdraws every primary-only lease
// artifact while the caller holds leaseCheckpointMu exclusively. ReloadLeases uses it when
// its end-of-load epoch check fails; calling stopLeases there would recursively
// acquire the same mutex.
func (m *leaseManager) clearLeaseStateHoldingCheckpointLock() {
	m.leaseReady.Store(false)
	m.leaseReadyEpoch.Store(0)
	// Stop the sweeper first (it acquires leaseMu itself, so must run outside the
	// lock below).
	m.stopOrphanSweeper()
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	m.leaseGeneration++
	for _, st := range m.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
		if st.checkpointTimer != nil {
			st.checkpointTimer.Stop()
		}
	}
	m.leases = make(map[int64]*leaseState)
	m.pendingLeases = make(map[int64]uint64)
	m.clearLeaseIncarnations()
	m.keyLeaseIndex = make(map[string]int64)
	atomic.StoreInt64(&m.leasedKeyCount, 0)
}

func (m *leaseManager) clearLeaseIncarnations() {
	m.leaseIncarnations.Range(func(key, _ any) bool {
		m.leaseIncarnations.Delete(key)
		return true
	})
}

// orphanLeaseSweepInterval is how often the leader reconciles durable lease
// attachment records against live leases. It is a slow safety net (expiry itself
// is timer-driven), so a coarse interval is fine.
const orphanLeaseSweepInterval = 10 * time.Minute

// startOrphanSweeper launches the leader-side orphaned-leased-key sweeper if it is
// not already running. Called on leadership acquisition (after ReloadLeases has
// rebuilt lease state).
func (m *leaseManager) startOrphanSweeper(ctx context.Context, epoch uint64) {
	m.leaseMu.Lock()
	if m.orphanSweepStop != nil {
		if m.orphanSweepEpoch == epoch {
			m.leaseMu.Unlock()
			return
		}
		close(m.orphanSweepStop)
	}
	stop := make(chan struct{})
	m.orphanSweepStop = stop
	m.orphanSweepEpoch = epoch
	interval := m.orphanSweepInterval
	m.leaseMu.Unlock()
	m.startWorker(func(workerCtx context.Context) {
		defer m.finishOrphanSweeper(stop)
		merged, cancel := context.WithCancel(ctx)
		stopWorker := context.AfterFunc(workerCtx, cancel)
		defer func() {
			stopWorker()
			cancel()
		}()
		m.runOrphanSweeper(merged, stop, interval, epoch)
	})
}

// finishOrphanSweeper retires only the generation represented by stop. A new
// leadership reload may already have replaced it with another channel.
func (m *leaseManager) finishOrphanSweeper(stop chan struct{}) {
	m.leaseMu.Lock()
	if m.orphanSweepStop == stop {
		m.orphanSweepStop = nil
		m.orphanSweepEpoch = 0
	}
	m.leaseMu.Unlock()
}

// stopOrphanSweeper signals the sweeper goroutine to exit (idempotent).
func (m *leaseManager) stopOrphanSweeper() {
	m.leaseMu.Lock()
	if m.orphanSweepStop != nil {
		close(m.orphanSweepStop)
		m.orphanSweepStop = nil
		m.orphanSweepEpoch = 0
	}
	m.leaseMu.Unlock()
}

func (m *leaseManager) runOrphanSweeper(ctx context.Context, stop chan struct{}, interval time.Duration, expectedEpoch uint64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
			if !leadingFresh || epoch != expectedEpoch {
				return
			}
			m.sweepOrphanLeasedKeys(backend.WithLeadershipEpoch(ctx, epoch))
		}
	}
}

// sweepOrphanLeasedKeys is the safety-net reconciliation borrowed from kine's TTL
// poller: it guarantees a leased key is eventually collected even if its in-memory
// expiry timer was never (re)armed. That happens when a durable attachment record
// (leasekeys/<key> -> <id>) outlives its lease's meta record — e.g. a best-effort
// detach failed on revoke, then a leader change reloaded the attachment, which
// applyLeaseRecords drops as an orphan (no timer). The key would otherwise carry
// an inline lease in its value forever with nothing to expire it.
//
// For each durable attachment whose lease id is no longer live and whose key is
// not in the live index, it deletes the key (its lease is gone — etcd deletes a
// key when its lease lapses), or, when the key was rebound/removed, just reclaims
// the stale attachment record.
func (m *leaseManager) sweepOrphanLeasedKeys(ctx context.Context) {
	// Freeze every current-generation binding from durable commit through index
	// publication. This makes a stored attachment with no matching index an old
	// orphan even when its numeric lease ID has since been granted again.
	m.leaseWriteMu.Lock()
	defer m.leaseWriteMu.Unlock()

	records, attachments, legacyAttachments, staleInternalAttachments, err := m.loadLeaseRecords(ctx)
	if err != nil {
		m.srv.metricCli.EmitCounter("lease.orphan_sweep.err", 1)
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "load")
		klog.ErrorS(err, "orphan lease sweep: load attachment records failed")
		return
	}
	for key, id := range staleInternalAttachments {
		attachments[key] = id
	}
	m.leaseMu.Lock()
	migrations := m.legacyMigrationsLocked(records)
	m.leaseMu.Unlock()
	m.migrateLegacyLeases(ctx, migrations)
	for key, id := range attachments {
		m.leaseMu.Lock()
		_, leaseLive := m.leases[id]
		indexedID, indexed := m.keyLeaseIndex[key]
		m.leaseMu.Unlock()
		if leaseLive && indexed && indexedID == id {
			continue // healthy current-generation binding tracked for expiry
		}
		epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
		if !leadingFresh {
			return // lost leadership mid-sweep
		}
		m.reconcileOrphanAttachment(backend.WithLeadershipEpoch(ctx, epoch), key, id)
	}
	m.cleanupOrphanLegacyAttachments(ctx, legacyAttachments)
	if err := m.sealLegacyLeaseMigrationIfClean(ctx); err != nil {
		m.srv.metricCli.EmitCounter("lease.orphan_sweep.err", 1)
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "seal")
		klog.ErrorS(err, "orphan lease sweep: seal legacy migration failed")
	}
}

func (m *leaseManager) cleanupOrphanLegacyAttachments(ctx context.Context, legacyAttachments map[string]int64) {
	for key, legacyID := range legacyAttachments {
		m.leaseMu.Lock()
		_, leaseLive := m.leases[legacyID]
		currentID, indexed := m.keyLeaseIndex[key]
		m.leaseMu.Unlock()
		if leaseLive && indexed && currentID == legacyID {
			continue // normal migration owns this row
		}
		if !indexed || currentID == legacyID {
			if !m.reconcileOrphanLegacyAttachment(ctx, key, legacyID) {
				continue
			}
		}
		if _, err := m.srv.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseAttachKey(key)}); err != nil {
			emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "attachment_delete")
		}
	}
}

// reconcileOrphanLegacyAttachment extends orphan reconciliation to the brief
// rolling-upgrade format where both the user value and attachment lived in user
// MVCC. Values written before per-version lease metadata have Lease==0, so the
// ordinary reconciliation cannot prove that the defunct lease still owns them.
// The atomic legacy writer did, however, commit the value and attachment at the
// same revision. That equality is a safe ownership witness: a later re-Put has a
// different ModRevision and must not be deleted. Still older, non-atomic rows do
// not have this witness and remain deliberately conservative.
func (m *leaseManager) reconcileOrphanLegacyAttachment(ctx context.Context, key string, id int64) bool {
	resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
	if err != nil || len(resp.Kvs) == 0 || resp.Kvs[0].Lease != 0 {
		return m.reconcileOrphanAttachment(ctx, key, id)
	}
	legacy, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(key)})
	if err != nil || len(legacy.Kvs) == 0 {
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "legacy_attachment_read")
		return false
	}
	legacyID, err := parseLeaseAttachmentRecord(key, legacy.Kvs[0].Value)
	if err != nil || legacyID != id {
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "legacy_attachment_invalid")
		return false
	}
	if resp.Kvs[0].ModRevision != legacy.Kvs[0].ModRevision {
		// Pre-atomic legacy data or a subsequently recreated user key. Retire the
		// stale marker, but do not guess that the current value is lease-owned.
		return true
	}
	dresp, err := m.srv.backend.Delete(ctx, []byte(key), resp.Kvs[0].ModRevision, false)
	if err != nil {
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "key_delete")
		return false
	}
	if !dresp.GetSucceeded() {
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "key_compare")
		return false
	}
	m.srv.metricCli.EmitCounter("lease.orphan_sweep.legacy_key_deleted", 1)
	klog.InfoS("orphan lease sweep: deleted legacy key bound to a defunct lease", "key", util.LoggedKey([]byte(key)), "lease", id)
	return true
}

// reconcileOrphanAttachment handles one record that the caller, while holding
// exclusive leaseWriteMu, proved is not a healthy current-generation binding.
// It deletes the key only when authoritative per-version metadata still names
// this attachment; otherwise it merely reclaims the stale record.
func (m *leaseManager) reconcileOrphanAttachment(ctx context.Context, key string, id int64) bool {
	resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
	if err != nil {
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "user_read")
		return false
	}
	if len(resp.Kvs) == 0 || resp.Kvs[0].Lease != id {
		// Key already gone, or rebound/recreated to a different (or no) lease: the
		// attachment record is stale. Reclaim it; leave any live key alone. (A
		// legacy v1 value carries no inline lease and reads back Lease==0 here, so
		// its key is conservatively left in place — only its stale record is
		// cleaned.)
		if err := m.detachKeyFromStorage(ctx, key); err == nil {
			m.srv.metricCli.EmitCounter("lease.orphan_sweep.record_reclaimed", 1)
			return true
		}
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "attachment_delete")
		return false
	}
	// The key is live and its inline lease matches the stale attachment. The
	// exclusive generation/index snapshot above proves that a same-ID regrant did
	// not attach it; compare-delete still protects against non-cooperating legacy
	// writers and storage state changed outside the online mutation path.
	dresp, err := m.srv.backend.Delete(ctx, []byte(key), resp.Kvs[0].ModRevision, false)
	if err != nil {
		emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "key_delete")
		return false
	}
	if dresp.GetSucceeded() {
		if err := m.detachKeyFromStorage(ctx, key); err != nil {
			emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "attachment_delete")
		}
		m.srv.metricCli.EmitCounter("lease.orphan_sweep.key_deleted", 1)
		klog.InfoS("orphan lease sweep: deleted key bound to a defunct lease", "key", util.LoggedKey([]byte(key)), "lease", id)
		return true
	}
	emitLeaseOrphanSweepFailureForContext(ctx, m.srv.metricCli, "key_compare")
	return false
}

func (m *leaseManager) scheduleLeaseLocked(st *leaseState) {
	duration := time.Until(st.deadline)
	if duration < 0 {
		duration = 0
	}
	if st.timer == nil {
		generation := m.leaseGeneration
		epoch, _ := m.srv.peers.EpochAndLeadingFresh()
		st.timer = time.AfterFunc(duration, func() {
			m.startWorker(func(ctx context.Context) {
				m.expireLeaseGenerationWithContext(ctx, st.id, generation, epoch)
			})
		})
		return
	}
	st.timer.Reset(duration)
}

func (m *leaseManager) scheduleLeaseCheckpointLocked(st *leaseState) {
	if time.Until(st.deadline) <= leaseCheckpointInterval {
		if st.checkpointTimer != nil {
			st.checkpointTimer.Stop()
		}
		return
	}
	if st.checkpointTimer == nil {
		generation := m.leaseGeneration
		epoch, _ := m.srv.peers.EpochAndLeadingFresh()
		st.checkpointTimer = time.AfterFunc(leaseCheckpointInterval, func() {
			m.startWorker(func(ctx context.Context) {
				m.checkpointLeaseGenerationWithContext(ctx, st.id, generation, epoch)
			})
		})
		return
	}
	st.checkpointTimer.Reset(leaseCheckpointInterval)
}

func (m *leaseManager) retryLeaseCheckpoint(id int64, generation uint64) {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	if m.leaseGeneration != generation {
		return
	}
	st := m.leases[id]
	if st == nil || st.checkpointTimer == nil {
		return
	}
	st.checkpointTimer.Reset(leaseExpiryRetryInterval)
}

func (m *leaseManager) checkpointLease(id int64) {
	m.checkpointLeaseWithContext(context.Background(), id)
}

func (m *leaseManager) checkpointLeaseWithContext(workerCtx context.Context, id int64) {
	m.leaseMu.Lock()
	generation := m.leaseGeneration
	m.leaseMu.Unlock()
	epoch, _ := m.srv.peers.EpochAndLeadingFresh()
	m.checkpointLeaseGenerationWithContext(workerCtx, id, generation, epoch)
}

func (m *leaseManager) checkpointLeaseGenerationWithContext(workerCtx context.Context, id int64, generation, expectedEpoch uint64) {
	epoch, leadingFresh := m.srv.peers.EpochAndLeadingFresh()
	if !leadingFresh || epoch != expectedEpoch || !m.leaseReady.Load() || m.leaseReadyEpoch.Load() != epoch {
		m.retryLeaseCheckpoint(id, generation)
		return
	}
	ctx := backend.WithLeadershipEpoch(workerCtx, epoch)

	unlockCheckpoint := m.lockLeaseCheckpoint(id)
	defer unlockCheckpoint()
	m.leaseWriteMu.RLock()
	defer m.leaseWriteMu.RUnlock()

	m.leaseMu.Lock()
	if m.leaseGeneration != generation {
		m.leaseMu.Unlock()
		return
	}
	st := m.leases[id]
	if st == nil || !st.deadline.After(time.Now()) {
		m.leaseMu.Unlock()
		return
	}
	remainingTTL := int64(math.Ceil(time.Until(st.deadline).Seconds()))
	ttl := st.ttl
	incarnation := st.incarnation
	if remainingTTL >= ttl {
		m.scheduleLeaseCheckpointLocked(st)
		m.leaseMu.Unlock()
		return
	}
	m.leaseMu.Unlock()

	applyStart := time.Now()
	err := m.persistLeaseCheckpointIncarnation(ctx, id, ttl, remainingTTL, incarnation)
	emitEtcdLeaseCheckpointDurations(m.srv.metricCli, time.Since(applyStart), err)
	if err != nil {
		m.srv.metricCli.EmitCounter("lease.checkpoint.err", 1)
		emitLeaseBackgroundFailure(m.srv.metricCli, "checkpoint")
		klog.ErrorS(err, "lease checkpoint: failed to persist remaining TTL", "lease", id, "remainingTTL", remainingTTL)
		m.retryLeaseCheckpoint(id, generation)
		return
	}

	m.leaseMu.Lock()
	if m.leaseGeneration == generation {
		st = m.leases[id]
	} else {
		st = nil
	}
	if st != nil {
		st.remainingTTL = remainingTTL
		m.scheduleLeaseCheckpointLocked(st)
	}
	m.leaseMu.Unlock()
}

func emitEtcdLeaseCheckpointDurations(metricCli metrics.Metrics, duration time.Duration, err error) {
	emitEtcdApplyDuration(metricCli, "LeaseCheckpoint", duration, err)
	emitEtcdRequestDuration(metricCli, "LeaseCheckpoint", duration, err)
}

// persistLeaseMeta writes the small per-lease meta record {id, ttl}. Attachments
// are stored separately; remaining TTL is written only by periodic checkpoints.
func (m *leaseManager) persistLeaseMeta(ctx context.Context, id, ttl int64) error {
	incarnation, _ := m.leaseIncarnation(id)
	return m.persistLeaseMetaIncarnation(ctx, id, ttl, incarnation)
}

func (m *leaseManager) persistLeaseMetaIncarnation(ctx context.Context, id, ttl int64, incarnation string) error {
	data, err := json.Marshal(leaseRecord{ID: id, Incarnation: incarnation, TTL: ttl})
	if err != nil {
		return err
	}
	return m.persistLeaseRecord(ctx, leaseStorageKey(id), data, true)
}

func (m *leaseManager) persistLeaseCheckpoint(ctx context.Context, id, ttl, remainingTTL int64) error {
	incarnation, _ := m.leaseIncarnation(id)
	return m.persistLeaseCheckpointIncarnation(ctx, id, ttl, remainingTTL, incarnation)
}

func (m *leaseManager) persistLeaseCheckpointIncarnation(ctx context.Context, id, ttl, remainingTTL int64, incarnation string) error {
	data, err := json.Marshal(leaseRecord{ID: id, Incarnation: incarnation, TTL: ttl, RemainingTTL: remainingTTL})
	if err != nil {
		return err
	}
	return m.persistLeaseRecord(ctx, leaseStorageKey(id), data, false)
}

func (m *leaseManager) persistMigratedLeaseMeta(ctx context.Context, id, ttl, remainingTTL int64) error {
	incarnation, _ := m.leaseIncarnation(id)
	return m.persistMigratedLeaseMetaIncarnation(ctx, id, ttl, remainingTTL, incarnation)
}

func (m *leaseManager) persistMigratedLeaseMetaIncarnation(ctx context.Context, id, ttl, remainingTTL int64, incarnation string) error {
	data, err := json.Marshal(leaseRecord{ID: id, Incarnation: incarnation, TTL: ttl, RemainingTTL: remainingTTL})
	if err != nil {
		return err
	}
	return m.persistLeaseRecord(ctx, leaseStorageKey(id), data, true)
}

func (m *leaseManager) persistLeaseRecord(ctx context.Context, key, data []byte, corruptGuarded bool) error {
	var err error
	if corruptGuarded {
		err = m.srv.backend.InternalPutCorruptGuarded(ctx, key, data)
	} else {
		err = m.srv.backend.InternalPut(ctx, key, data)
	}
	if err == nil {
		return nil
	}
	// Storage commits explicitly classify an outcome that may have landed as
	// ErrUncertainResult. Definite admission, validation, CAS, and safety-fence
	// failures must never be overwritten by a coincidentally identical record.
	if !errors.Is(err, storage.ErrUncertainResult) {
		return err
	}

	// A TiKV commit can succeed even when its response is lost to cancellation
	// or a transport failure. Treating that as definitely uncommitted is unsafe:
	// an unobserved remaining-TTL checkpoint would not be cleared by the next
	// keepalive, so a later leader could expire a freshly renewed lease early.
	// The record is canonical and fully replaces one internal key, making an
	// exact linearized readback sufficient to prove this write's final state.
	reconcileCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), leaseMetadataReconciliationTimeout,
	)
	defer cancel()
	current, getErr := m.srv.backend.InternalGet(reconcileCtx, key)
	if getErr != nil {
		return errors.Join(
			err,
			fmt.Errorf("inspect lease metadata after failed write: %w", getErr),
		)
	}
	if bytes.Equal(current, data) {
		return nil
	}
	return errors.Join(err, errors.New("lease metadata differs after failed write"))
}

// persistLeaseCheckpointCAS changes only the expected version of one lease
// metadata record. It is used by Renew after dropping leaseWriteMu: a concurrent
// Revoke may delete the record, in which case the CAS must not recreate it.
func (m *leaseManager) persistLeaseCheckpointCAS(ctx context.Context, id, ttl, fromRemainingTTL, toRemainingTTL int64) error {
	incarnation, _ := m.leaseIncarnation(id)
	return m.persistLeaseCheckpointCASIncarnation(ctx, id, ttl, fromRemainingTTL, toRemainingTTL, incarnation)
}

func (m *leaseManager) persistLeaseCheckpointCASIncarnation(ctx context.Context, id, ttl, fromRemainingTTL, toRemainingTTL int64, incarnation string) error {
	expected, err := json.Marshal(leaseRecord{ID: id, Incarnation: incarnation, TTL: ttl, RemainingTTL: fromRemainingTTL})
	if err != nil {
		return err
	}
	updated, err := json.Marshal(leaseRecord{ID: id, Incarnation: incarnation, TTL: ttl, RemainingTTL: toRemainingTTL})
	if err != nil {
		return err
	}
	key := leaseStorageKey(id)
	err = m.srv.backend.InternalCAS(ctx, []backend.InternalCASOp{{
		Key:            key,
		Expected:       expected,
		ExpectedExists: true,
		Value:          updated,
	}})
	if err == nil || errors.Is(err, storage.ErrCASFailed) {
		return err
	}
	if !errors.Is(err, storage.ErrUncertainResult) {
		return err
	}

	// As with InternalPut above, a lost commit response is ambiguous. Exact
	// readback proves success; a concurrent Revoke leaves the key absent and is
	// resolved by the in-memory generation check after leaseWriteMu is reacquired.
	reconcileCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), leaseMetadataReconciliationTimeout,
	)
	defer cancel()
	current, getErr := m.srv.backend.InternalGet(reconcileCtx, key)
	if getErr == nil && bytes.Equal(current, updated) {
		return nil
	}
	if getErr != nil {
		return errors.Join(err, fmt.Errorf("inspect lease metadata after failed CAS: %w", getErr))
	}
	return errors.Join(err, errors.New("lease metadata differs after failed CAS"))
}

// attachKeyToStorage records that userKey is attached to lease id as a single
// small record (leasekeys/<userKey> -> <id>). O(1) per attach, replacing the old
// full-key-list rewrite (#17). The record is keyed by the user key, which has at
// most one lease, so a rebind simply overwrites it.
func (m *leaseManager) attachKeyToStorage(ctx context.Context, id int64, userKey string) error {
	incarnation, _ := m.leaseIncarnation(id)
	return m.srv.backend.InternalPutCorruptGuarded(ctx, leaseAttachKey(userKey), encodeLeaseAttachmentRecord(id, incarnation))
}

func (m *leaseManager) detachKeyFromStorage(ctx context.Context, userKey string) error {
	_, _, _, err := m.srv.backend.TxnApply(ctx, []backend.TxnWriteOp{{
		Delete: true, Internal: true, Key: leaseAttachKey(userKey),
	}}, nil, []bool{false})
	return err
}

func (m *leaseManager) deleteLeaseState(ctx context.Context, id int64) error {
	_, _, _, err := m.srv.backend.TxnApply(ctx, []backend.TxnWriteOp{{
		Delete: true, Internal: true, Key: leaseStorageKey(id),
	}}, nil, []bool{false})
	return err
}

func leaseAttachKey(userKey string) []byte {
	key := make([]byte, 0, len(leaseAttachPrefix)+len(userKey))
	key = append(key, leaseAttachPrefix...)
	key = append(key, userKey...)
	return key
}

func leaseStorageKey(id int64) []byte {
	key := make([]byte, 0, len(leaseStoragePrefix)+20)
	key = append(key, leaseStoragePrefix...)
	key = append(key, []byte(strconv.FormatInt(id, 10))...)
	return key
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}
