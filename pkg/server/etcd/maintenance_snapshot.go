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
	"os"
	"sort"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	production "github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
)

const snapshotSendBufferSize = 32 * 1024

func (s *RPCServer) snapshotState(ctx context.Context) (production.State, error) {
	for attempt := 0; ; attempt++ {
		state, err := s.snapshotStateOnce(ctx)
		if !errors.Is(err, errSnapshotChanged) {
			return state, err
		}
		if attempt >= 7 {
			return production.State{}, fmt.Errorf("capture stable etcd snapshot: %w", err)
		}
		select {
		case <-ctx.Done():
			return production.State{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Millisecond):
		}
	}
}

var errSnapshotChanged = errors.New("snapshot state changed while it was captured")

func (s *RPCServer) snapshotStateOnce(ctx context.Context) (production.State, error) {
	rangeCtx, unlock := s.backend.BeginRangeTxn(ctx)
	defer unlock()
	ctx = rangeCtx
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return production.State{}, readBarrierStatusErr(err)
	}
	revision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return production.State{}, err
	}
	response, err := s.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0}, RangeEnd: []byte{0}, Revision: int64(revision),
	})
	if err != nil {
		return production.State{}, err
	}
	state := production.State{Revision: int64(revision), Records: make([]production.Record, 0, len(response.Kvs))}
	for _, kv := range response.Kvs {
		state.Records = append(state.Records, production.Record{
			Key: kv.Key, Value: kv.Value, CreateRevision: kv.CreateRevision,
			ModRevision: kv.ModRevision, Version: kv.Version, Lease: kv.Lease,
		})
	}

	auth, err := s.auth.repo.load(ctx)
	if err != nil {
		return production.State{}, err
	}
	state.Auth.Enabled = auth.Config.Enabled
	state.Auth.Revision = auth.Config.Revision
	for _, name := range authUserNames(auth) {
		state.Auth.Users = append(state.Auth.Users, auth.Users[name])
	}
	for _, name := range authRoleNames(auth) {
		state.Auth.Roles = append(state.Auth.Roles, auth.Roles[name])
	}

	leaseRecords, _, err := s.loadLeaseRecords(ctx)
	if err != nil {
		return production.State{}, err
	}
	now := time.Now()
	sort.Slice(leaseRecords, func(i, j int) bool { return leaseRecords[i].ID < leaseRecords[j].ID })
	leaseIDs := make(map[int64]struct{}, len(leaseRecords))
	for _, record := range leaseRecords {
		remaining := record.RemainingTTL
		if record.DeadlineUnixNano > 0 {
			// Use the same captured clock for every lease in the snapshot.
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
	for _, record := range state.Records {
		if record.Lease == 0 {
			continue
		}
		if _, ok := leaseIDs[record.Lease]; !ok {
			return production.State{}, errSnapshotChanged
		}
	}

	noSpace, err := s.backend.NoSpaceAlarms(ctx)
	if err != nil {
		return production.State{}, err
	}
	for _, memberID := range noSpace {
		state.Alarms = append(state.Alarms, &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE})
	}
	corrupt, err := s.backend.CorruptAlarms(ctx)
	if err != nil {
		return production.State{}, err
	}
	for _, memberID := range corrupt {
		state.Alarms = append(state.Alarms, &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT})
	}
	generic, err := s.genericAlarms(ctx, etcdserverpb.AlarmType_NONE)
	if err != nil {
		return production.State{}, err
	}
	state.Alarms = append(state.Alarms, generic...)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return production.State{}, readBarrierStatusErr(err)
	}
	latestRevision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return production.State{}, err
	}
	if latestRevision != revision {
		return production.State{}, errSnapshotChanged
	}
	return state, nil
}

func (s *RPCServer) sendSnapshot(stream etcdserverpb.Maintenance_SnapshotServer) error {
	state, err := s.snapshotState(stream.Context())
	if err != nil {
		return err
	}
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
	if err = production.WriteBackend(path, state); err != nil {
		return fmt.Errorf("write etcd snapshot backend: %w", err)
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
