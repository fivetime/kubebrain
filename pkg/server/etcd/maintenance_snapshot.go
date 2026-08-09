// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcd

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/kubewharf/kubebrain/pkg/backend"
	production "github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
)

const snapshotSendBufferSize = 32 * 1024

const snapshotLeadershipPollInterval = 25 * time.Millisecond

var errSnapshotChanged = errors.New("snapshot state changed while it was captured")

var errSnapshotLeaderChanged = errors.New("snapshot leadership changed while it was captured")

var errSnapshotHistoricalLeaseUnknown = errors.New("snapshot cannot determine lease for retained legacy version")

// buildSnapshot retries a capture if its pinned stream and captured metadata
// cannot form a valid state (for example, a KV references no captured lease).
// Each failed attempt owns a fresh bbolt file; no partially captured backend can
// be sent to the client.
func (s *RPCServer) buildSnapshot(ctx context.Context, path string) error {
	for attempt := 0; ; attempt++ {
		_ = os.Remove(path)
		err := s.buildSnapshotOnce(ctx, path)
		if !errors.Is(err, errSnapshotChanged) && !errors.Is(err, errSnapshotLeaderChanged) {
			return err
		}
		if attempt >= 7 {
			if errors.Is(err, errSnapshotLeaderChanged) {
				return rpctypes.ErrGRPCLeaderChanged
			}
			return fmt.Errorf("capture stable etcd snapshot: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Millisecond):
		}
	}
}

func (s *RPCServer) buildSnapshotOnce(ctx context.Context, path string) (retErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		return rpctypes.ErrGRPCNotLeader
	}
	checkLeadership := func() error {
		currentEpoch, stillLeadingFresh := s.peers.EpochAndLeadingFresh()
		if !stillLeadingFresh {
			return rpctypes.ErrGRPCNotLeader
		}
		if currentEpoch != epoch {
			return errSnapshotLeaderChanged
		}
		return nil
	}
	rangeCtx, unlock := s.backend.BeginRangeTxn(ctx)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	ctx = rangeCtx
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return readBarrierStatusErr(err)
	}
	revision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return err
	}
	if revision >= math.MaxInt64 {
		return fmt.Errorf("snapshot revision leaves no room for next etcd write: %d", revision)
	}
	state, leaseIDs, leaseAttachments, err := s.snapshotMetadata(ctx, int64(revision))
	if err != nil {
		return err
	}
	state.PreserveHistory = true
	state.HasCompactRevision, err = s.backend.HasCompactRevision(ctx)
	if err != nil {
		return err
	}
	if state.HasCompactRevision {
		compactRevision, compactErr := s.backend.GetCompactRevisionFresh(ctx)
		if compactErr != nil {
			return compactErr
		}
		state.CompactRevision = int64(compactRevision)
	}
	chunks, err := s.backend.SnapshotHistoryStreamChan(ctx, revision)
	if err != nil {
		return err
	}
	// Wait for the scanner's first response while still excluding local Compact
	// and writes. Receiving it proves the fixed-revision storage snapshot has
	// actually been established; merely creating the channel starts a goroutine
	// whose timestamp/compaction checks may not have run yet.
	leadershipPoll := time.NewTicker(snapshotLeadershipPollInterval)
	defer leadershipPoll.Stop()
	var firstChunk backend.SnapshotHistoryChunk
	var ok bool
	waitingForFirst := true
	for waitingForFirst {
		select {
		case firstChunk, ok = <-chunks:
			waitingForFirst = false
		case <-leadershipPoll.C:
			if err = checkLeadership(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("range stream ended before establishing its pinned revision")
	}
	if firstChunk.Err != nil {
		return firstChunk.Err
	}
	if err = checkLeadership(); err != nil {
		return err
	}
	// Metadata and the pinned user revision define the snapshot's linearization
	// point. Historical scanning remains fixed at that revision, so retaining the
	// process-wide logical-write barrier while TiKV and bbolt stream the entire
	// keyspace would only stall later writes; it is not required for consistency.
	unlock()
	locked = false

	builder, err := production.NewBuilder(path, state)
	if err != nil {
		return fmt.Errorf("create etcd snapshot backend: %w", err)
	}
	defer func() {
		if err := builder.Close(); retErr == nil && err != nil {
			retErr = err
		}
	}()

	sawTerminal := false
	consume := func(chunk backend.SnapshotHistoryChunk) error {
		// The object iterator is a fixed TiKV snapshot, but ordered-event and
		// legacy metadata joins are streamed through independent reads protected
		// by a process-local compaction pin. A successor on another Pod cannot
		// observe that pin, so no chunk may cross a leadership term.
		if leadershipErr := checkLeadership(); leadershipErr != nil {
			return leadershipErr
		}
		if chunk.Err != nil {
			return chunk.Err
		}
		if chunk.Revision != revision {
			return errSnapshotChanged
		}
		if chunk.Done {
			if len(chunk.Records) != 0 {
				return fmt.Errorf("range stream terminal chunk contains records")
			}
			sawTerminal = true
			return nil
		}
		if sawTerminal {
			return fmt.Errorf("range stream returned records after its terminal chunk")
		}
		records := make([]production.Record, 0, len(chunk.Records))
		for _, record := range chunk.Records {
			// Legacy raw/v1 values did not persist lease IDs per MVCC version.
			// The pinned attachment map below can recover the current version,
			// but after a rebind it contains no evidence about an older version's
			// lease. Never turn that information loss into a plausible lease=0:
			// etcdutl would restore a valid-looking database with false history.
			// Once compaction physically removes the ambiguous retained version,
			// snapshots become available again; every v2 version is LeaseKnown.
			if !record.Current && !record.Tombstone && !record.LeaseKnown {
				return fmt.Errorf("%w: key %q revision %d", errSnapshotHistoricalLeaseUnknown, record.Key, record.ModRevision)
			}
			if record.Current {
				// Legacy rows do not carry a per-version lease. Reconcile them
				// against the durable attachment captured under the same write
				// barrier as lease metadata and the pinned history snapshot. A
				// live in-memory lookup here would race with later stream chunks.
				attachedLease, attached := leaseAttachments[string(record.Key)]
				if record.LeaseKnown {
					if (record.Lease == 0 && attached) ||
						(record.Lease != 0 && (!attached || attachedLease != record.Lease)) {
						return errSnapshotChanged
					}
				} else if attached {
					record.Lease = attachedLease
				}
				if record.Lease != 0 {
					if _, ok := leaseIDs[record.Lease]; !ok {
						return errSnapshotChanged
					}
				}
				if attached && attachedLease == 0 {
					return errSnapshotChanged
				}
			}
			records = append(records, production.Record{
				Key: record.Key, Value: record.Value, CreateRevision: int64(record.CreateRevision),
				ModRevision: int64(record.ModRevision), Version: int64(record.Version), Lease: record.Lease,
				SubRevision: int64(record.SubRevision), TotalChanges: int64(record.TotalChanges),
				Ordered: record.Ordered, Tombstone: record.Tombstone,
			})
		}
		if err = builder.Append(records); err != nil {
			return fmt.Errorf("append etcd snapshot records: %w", err)
		}
		return nil
	}
	if err = consume(firstChunk); err != nil {
		return err
	}
	for {
		select {
		case chunk, streamOpen := <-chunks:
			if !streamOpen {
				if !sawTerminal {
					if err := ctx.Err(); err != nil {
						return err
					}
					return fmt.Errorf("range stream ended without a terminal revision")
				}
				if err = builder.Finish(); err != nil {
					return fmt.Errorf("finish etcd snapshot backend: %w", err)
				}
				return nil
			}
			if err = consume(chunk); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-leadershipPoll.C:
			if err = checkLeadership(); err != nil {
				return err
			}
		}
	}
}

func (s *RPCServer) snapshotMetadata(ctx context.Context, revision int64) (production.State, map[int64]struct{}, map[string]int64, error) {
	state := production.State{Revision: revision}
	auth, err := s.auth.repo.load(ctx)
	if err != nil {
		return production.State{}, nil, nil, err
	}
	state.Auth.Enabled = auth.Config.Enabled
	state.Auth.Revision = auth.Config.Revision
	for _, name := range authUserNames(auth) {
		state.Auth.Users = append(state.Auth.Users, auth.Users[name])
	}
	for _, name := range authRoleNames(auth) {
		state.Auth.Roles = append(state.Auth.Roles, auth.Roles[name])
	}

	leaseRecords, leaseAttachments, err := s.loadLeaseRecords(ctx)
	if err != nil {
		return production.State{}, nil, nil, err
	}
	now := time.Now()
	sort.Slice(leaseRecords, func(i, j int) bool { return leaseRecords[i].ID < leaseRecords[j].ID })
	leaseIDs := make(map[int64]struct{}, len(leaseRecords))
	for _, record := range leaseRecords {
		remaining := record.RemainingTTL
		if record.DeadlineUnixNano > 0 {
			until := time.Unix(0, record.DeadlineUnixNano).Sub(now)
			remaining = int64((until + time.Second - 1) / time.Second)
			if remaining <= 0 {
				remaining = 1
			}
		} else if remaining <= 0 {
			remaining = record.TTL
		}
		leaseIDs[record.ID] = struct{}{}
		state.Leases = append(state.Leases, production.Lease{
			ID: record.ID, GrantedTTL: record.TTL, RemainingTTL: remaining,
		})
	}

	noSpace, err := s.backend.NoSpaceAlarms(ctx)
	if err != nil {
		return production.State{}, nil, nil, err
	}
	for _, memberID := range noSpace {
		state.Alarms = append(state.Alarms, &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE})
	}
	corrupt, err := s.backend.CorruptAlarms(ctx)
	if err != nil {
		return production.State{}, nil, nil, err
	}
	for _, memberID := range corrupt {
		state.Alarms = append(state.Alarms, &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT})
	}
	generic, err := s.genericAlarms(ctx, etcdserverpb.AlarmType_NONE)
	if err != nil {
		return production.State{}, nil, nil, err
	}
	state.Alarms = append(state.Alarms, generic...)
	return state, leaseIDs, leaseAttachments, nil
}

func (s *RPCServer) sendSnapshot(stream etcdserverpb.Maintenance_SnapshotServer) error {
	started := time.Now()
	defer func() { emitEtcdBackendSnapshotDuration(s.metricCli, time.Since(started)) }()
	tmp, err := os.CreateTemp("", ".kubebrain-maintenance-snapshot-*.db")
	if err != nil {
		return err
	}
	path := tmp.Name()
	if closeErr := tmp.Close(); closeErr != nil {
		_ = os.Remove(path)
		return closeErr
	}
	defer os.Remove(path)
	if err = s.buildSnapshot(stream.Context(), path); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	total := info.Size()
	sent := int64(0)
	hash := sha256.New()
	for sent < total {
		buf := make([]byte, snapshotSendBufferSize)
		n, readErr := io.ReadFull(f, buf)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		sent += int64(n)
		_, _ = hash.Write(buf[:n])
		if err = stream.Send(&etcdserverpb.SnapshotResponse{
			RemainingBytes: uint64(total - sent), Blob: buf[:n], Version: Version,
		}); err != nil {
			return err
		}
	}
	return stream.Send(&etcdserverpb.SnapshotResponse{
		RemainingBytes: 0, Blob: hash.Sum(nil), Version: Version,
	})
}
