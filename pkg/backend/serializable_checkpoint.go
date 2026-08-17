// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package backend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var ErrSerializableCheckpointUnavailable = errors.New("serializable checkpoint unavailable")

var serializableCheckpointKey = []byte("revision/serializable-checkpoint")

func initSerializableCheckpointMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitGauge("serializable.checkpoint.available", int64(0))
	_ = metricCli.EmitGauge("serializable.checkpoint.revision", int64(0))
	_ = metricCli.EmitGauge("serializable.checkpoint.remaining_seconds", int64(0))
	_ = metricCli.EmitCounter("serializable.checkpoint.refresh_err", int64(0))
}

const (
	serializableCheckpointFormat  = uint64(1)
	serializableCheckpointTTL     = 5 * time.Minute
	serializableCheckpointUsable  = serializableCheckpointTTL / 2
	serializableCheckpointRefresh = time.Second
	// A failed refresh usually means the shared TiKV/PD data path is already
	// degraded. Retrying once per second from every KubeBrain replica amplifies
	// that degradation and can keep Region caches in a permanent timeout loop.
	// Retain the fast steady-state cadence, but back off failed attempts while
	// the last protected checkpoint remains usable.
	serializableCheckpointMaxRetry = 30 * time.Second
	// Refresh the authoritative PD Region/store directory well inside the local
	// checkpoint safety window. This makes topology changes that complete while
	// PD is reachable part of a later isolation checkpoint without scanning PD
	// on every one-second safepoint renewal.
	serializableCheckpointRegionRefresh = 30 * time.Second
	// A Range that selected the previous checkpoint may remain in flight for
	// the server's ten-second unary request window. Rotate between two PD
	// service records no faster than this grace period, so the previous record
	// continues pinning its snapshot until every such request has terminated.
	// Reusing one slot therefore takes at least twice this interval.
	serializableCheckpointProtectionGrace = 30 * time.Second
)

// SerializableCheckpoint binds the client-visible revision and compact/auth
// watermarks to one TiKV snapshot. AuthRevision is zero only when auth/config
// was absent (the initialized auth revision is then 1 at the server layer).
type SerializableCheckpoint struct {
	Revision        uint64
	Timestamp       uint64
	CompactRevision uint64
	AuthRevision    uint64
	ValidUntil      time.Time
}

type serializableCheckpointContextKey struct{}

// WithSerializableCheckpoint pins all backend storage reads and the response
// header revision to c. Callers must obtain c from GetSerializableCheckpoint.
func WithSerializableCheckpoint(ctx context.Context, c SerializableCheckpoint) context.Context {
	ctx = storage.WithProtectedSnapshotTimestamp(ctx, c.Timestamp)
	return context.WithValue(ctx, serializableCheckpointContextKey{}, c)
}

// SerializableCheckpointFromContext returns the request-pinned checkpoint.
// Its local safety deadline is checked before it is attached; an in-flight
// request may continue using that already protected snapshot until completion.
func SerializableCheckpointFromContext(ctx context.Context) (SerializableCheckpoint, bool) {
	c, ok := ctx.Value(serializableCheckpointContextKey{}).(SerializableCheckpoint)
	return c, ok && c.Revision != 0 && c.Timestamp != 0
}

func (b *backend) snapshotGet(ctx context.Context, key []byte) (result []byte, retErr error) {
	if timestamp, ok := storage.SnapshotTimestampFromContext(ctx); ok {
		reader, supported := storage.FindCapability[storage.SnapshotGetter](b.kv)
		if supported {
			return reader.GetAt(ctx, key, timestamp)
		}
		if !storage.SnapshotIteratorFallbackFromContext(ctx) {
			return nil, ErrSerializableCheckpointUnavailable
		}
		end := append(append([]byte(nil), key...), 0)
		it, err := b.kv.Iter(ctx, key, end, timestamp, 1)
		if err != nil {
			return nil, err
		}
		defer func() { retErr = errors.Join(retErr, it.Close()) }()
		if err := it.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, storage.ErrKeyNotFound
			}
			return nil, err
		}
		if !bytes.Equal(it.Key(), key) {
			return nil, storage.ErrKeyNotFound
		}
		return append([]byte(nil), it.Val()...), nil
	}
	return b.kv.Get(ctx, key)
}

func encodeSerializableCheckpoint(c SerializableCheckpoint) []byte {
	value := make([]byte, 40)
	binary.BigEndian.PutUint64(value[0:8], serializableCheckpointFormat)
	binary.BigEndian.PutUint64(value[8:16], c.Revision)
	binary.BigEndian.PutUint64(value[16:24], c.Timestamp)
	binary.BigEndian.PutUint64(value[24:32], c.CompactRevision)
	binary.BigEndian.PutUint64(value[32:40], c.AuthRevision)
	return value
}

func decodeSerializableCheckpoint(value []byte) (SerializableCheckpoint, error) {
	if len(value) != 40 || binary.BigEndian.Uint64(value[0:8]) != serializableCheckpointFormat {
		return SerializableCheckpoint{}, fmt.Errorf("invalid serializable checkpoint encoding")
	}
	c := SerializableCheckpoint{
		Revision: binary.BigEndian.Uint64(value[8:16]), Timestamp: binary.BigEndian.Uint64(value[16:24]),
		CompactRevision: binary.BigEndian.Uint64(value[24:32]), AuthRevision: binary.BigEndian.Uint64(value[32:40]),
	}
	if c.Revision == 0 || c.Timestamp == 0 || c.CompactRevision > c.Revision {
		return SerializableCheckpoint{}, fmt.Errorf("invalid serializable checkpoint values")
	}
	return c, nil
}

func newSerializableCheckpointServiceID(keyspace, identity string) string {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", keyspace, identity, time.Now().UnixNano())))
		nonce = sum[:16]
	}
	sum := sha256.Sum256(append([]byte(keyspace+"\x00"+identity+"\x00"), nonce...))
	return fmt.Sprintf("kubebrain-serializable-%x", sum[:12])
}

// createSerializableCheckpoint excludes every logical/internal mutation while
// choosing the TSO. Consequently no user row above Revision, auth mutation, or
// compact watermark update can be visible in the chosen engine snapshot.
func (b *backend) createSerializableCheckpoint(ctx context.Context) (SerializableCheckpoint, error) {
	var err error
	ctx, err = b.withCurrentLeadershipEpoch(ctx)
	if err != nil {
		return SerializableCheckpoint{}, err
	}
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()
	revision := b.tso.GetRevision()
	if revision == 0 {
		return SerializableCheckpoint{}, ErrSerializableCheckpointUnavailable
	}
	compactRevision, authRevision, err := b.loadSerializableCheckpointWatermarks(ctx)
	if err != nil {
		return SerializableCheckpoint{}, err
	}
	if current := b.serializableCheckpoint.Load(); current != nil &&
		current.Revision == revision && current.CompactRevision == compactRevision && current.AuthRevision == authRevision {
		return *current, nil
	}
	if err := b.persistDurableRevisionContext(ctx, revision); err != nil {
		return SerializableCheckpoint{}, err
	}
	timestamp := uint64(0)
	if validator, ok := storage.FindCapability[storage.SnapshotReadinessValidator](b.kv); ok {
		objectTimestamp, readyErr := validator.SnapshotReadyTimestamp(
			ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(),
		)
		if readyErr != nil {
			return SerializableCheckpoint{}, readyErr
		}
		// The compact watermark is deliberately stored in the coordination-key
		// namespace, outside the tenant's encoded object interval. Serializable
		// reads re-read it at the checkpoint for the compaction fence, so the
		// advertised timestamp must also be safe for its Region. An exact-key
		// interval avoids making one tenant depend on every unrelated Region
		// between the coordination and encoded keyspaces.
		compactKey := getCompactKey(b.config.Prefix)
		compactEnd := append(append([]byte(nil), compactKey...), 0)
		metadataTimestamp, readyErr := validator.SnapshotReadyTimestamp(ctx, compactKey, compactEnd)
		if readyErr != nil {
			return SerializableCheckpoint{}, readyErr
		}
		timestamp = min(objectTimestamp, metadataTimestamp)
		if timestamp == 0 {
			return SerializableCheckpoint{}, ErrSerializableCheckpointUnavailable
		}
		revision, compactRevision, authRevision, err = b.loadSerializableCheckpointWatermarksAt(ctx, timestamp)
		if err != nil {
			return SerializableCheckpoint{}, err
		}
	} else {
		timestamp, err = b.kv.GetTimestampOracle(ctx)
		if err != nil {
			return SerializableCheckpoint{}, err
		}
	}
	c := SerializableCheckpoint{Revision: revision, Timestamp: timestamp, CompactRevision: compactRevision, AuthRevision: authRevision}
	batch := b.kv.BeginBatchWrite()
	batch.Put(b.ks.EncodeInternalKey(serializableCheckpointKey), encodeSerializableCheckpoint(c), 0)
	if err := batch.Commit(b.withLogicalWriteOwnership(ctx)); err != nil {
		return SerializableCheckpoint{}, err
	}
	return c, nil
}

func (b *backend) loadSerializableCheckpointWatermarksAt(ctx context.Context, timestamp uint64) (uint64, uint64, uint64, error) {
	reader, ok := storage.FindCapability[storage.SnapshotGetter](b.kv)
	if !ok {
		return 0, 0, 0, ErrSerializableCheckpointUnavailable
	}
	durableKey := b.ks.EncodeInternalKey(durableRevisionKey)
	compactKey := getCompactKey(b.config.Prefix)
	authKey := b.ks.EncodeInternalKey([]byte("auth/config"))
	values, err := reader.BatchGetAt(ctx, [][]byte{durableKey, compactKey, authKey}, timestamp)
	if err != nil {
		return 0, 0, 0, err
	}
	revision, err := decodeDurableRevisionWatermark(values[string(durableKey)])
	if err != nil {
		return 0, 0, 0, err
	}
	compactValue, compactExists := values[string(compactKey)]
	compactRevision, err := decodeSerializableCheckpointCompactWatermark(compactValue, compactExists)
	if err != nil {
		return 0, 0, 0, err
	}
	authValue, authExists := values[string(authKey)]
	authRevision, err := decodeSerializableCheckpointAuthWatermark(authValue, authExists)
	return revision, compactRevision, authRevision, err
}

// loadSerializableCheckpointWatermarks reads the two metadata rows that bind a
// checkpoint in one storage snapshot. createSerializableCheckpoint holds the
// exclusive logical-write barrier, so no compact or auth mutation can race this
// read; BatchGet therefore preserves the existing authoritative semantics while
// removing one TiKV transaction from the one-second checkpoint critical path.
func (b *backend) loadSerializableCheckpointWatermarks(ctx context.Context) (uint64, uint64, error) {
	compactKey := getCompactKey(b.config.Prefix)
	authKey := b.ks.EncodeInternalKey([]byte("auth/config"))
	if batchGetter, ok := storage.FindCapability[storage.BatchGetter](b.kv); ok {
		values, err := batchGetter.BatchGet(ctx, [][]byte{compactKey, authKey})
		if err != nil {
			return 0, 0, err
		}
		compactValue, compactExists := values[string(compactKey)]
		compactRevision, err := decodeSerializableCheckpointCompactWatermark(compactValue, compactExists)
		if err != nil {
			return 0, 0, err
		}
		authValue, authExists := values[string(authKey)]
		authRevision, err := decodeSerializableCheckpointAuthWatermark(authValue, authExists)
		return compactRevision, authRevision, err
	}

	compactRevision, err := b.loadCompactRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	authValue, err := b.kv.Get(ctx, authKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return compactRevision, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	authRevision, err := decodeSerializableCheckpointAuthWatermark(authValue, true)
	return compactRevision, authRevision, err
}

func decodeSerializableCheckpointCompactWatermark(value []byte, exists bool) (uint64, error) {
	if !exists {
		return 0, nil
	}
	revision, err := coder.ParseRevisionWatermark(value)
	if err != nil {
		return 0, invalidMVCCMetadataError(err, "decode compact revision watermark")
	}
	return revision, nil
}

func decodeSerializableCheckpointAuthWatermark(value []byte, exists bool) (uint64, error) {
	if !exists {
		return 0, nil
	}
	if len(value) != 9 || value[0] > 1 {
		return 0, fmt.Errorf("invalid auth config while creating serializable checkpoint")
	}
	revision := binary.BigEndian.Uint64(value[1:])
	if revision == 0 {
		return 0, fmt.Errorf("zero auth revision while creating serializable checkpoint")
	}
	return revision, nil
}

func (b *backend) loadSerializableCheckpoint(ctx context.Context) (SerializableCheckpoint, error) {
	value, err := b.kv.Get(ctx, b.ks.EncodeInternalKey(serializableCheckpointKey))
	if err != nil {
		return SerializableCheckpoint{}, err
	}
	return decodeSerializableCheckpoint(value)
}

func (b *backend) protectSerializableCheckpoint(ctx context.Context, c SerializableCheckpoint) error {
	protector, ok := storage.FindCapability[storage.SnapshotProtector](b.kv)
	if !ok {
		return ErrSerializableCheckpointUnavailable
	}
	b.serializableCheckpointProtectMu.Lock()
	defer b.serializableCheckpointProtectMu.Unlock()

	now := time.Now()
	current := b.serializableCheckpoint.Load()
	if current != nil && current.Timestamp != c.Timestamp &&
		!b.serializableCheckpointSwitchedAt.IsZero() &&
		now.Sub(b.serializableCheckpointSwitchedAt) < serializableCheckpointProtectionGrace {
		// Renew rather than replace the advertised generation. Advancing its
		// only service record here would allow GC to invalidate requests that
		// already pinned it. The durable candidate may be newer; a subsequent
		// refresh publishes it after the grace window.
		c = *current
	}
	// Warm and validate the candidate while the previous, older service
	// safepoint is still registered.
	regionsRefreshed, err := b.refreshSerializableCheckpointRegions(ctx, c.Timestamp)
	if err != nil {
		return err
	}
	slot := b.serializableCheckpointSlot
	if current != nil && current.Timestamp != c.Timestamp {
		slot = 1 - slot
	}
	minimum, err := protector.ProtectSnapshot(
		ctx, b.serializableCheckpointServiceIDs[slot], serializableCheckpointTTL, c.Timestamp,
	)
	if err != nil {
		return err
	}
	if minimum > c.Timestamp {
		return fmt.Errorf("%w: GC safepoint %d passed snapshot %d", ErrSerializableCheckpointUnavailable, minimum, c.Timestamp)
	}
	if regionsRefreshed {
		if err := b.validateSerializableCheckpointPublication(ctx, c.Timestamp); err != nil {
			return err
		}
	}
	if current == nil || current.Timestamp != c.Timestamp {
		b.serializableCheckpointSlot = slot
		b.serializableCheckpointSwitchedAt = now
	}
	c.ValidUntil = now.Add(serializableCheckpointUsable)
	b.serializableCheckpoint.Store(&c)
	return nil
}

func (b *backend) refreshSerializableCheckpointRegions(ctx context.Context, timestamp uint64) (bool, error) {
	b.serializableCheckpointRegionWarmMu.Lock()
	defer b.serializableCheckpointRegionWarmMu.Unlock()
	now := time.Now()
	warmedAt := b.serializableCheckpointRegionsWarmedAt.Load()
	if warmedAt != 0 && now.UnixNano() >= warmedAt && now.UnixNano()-warmedAt < int64(serializableCheckpointRegionRefresh) {
		return false, nil
	}
	if err := b.warmSerializableCheckpoint(ctx, timestamp); err != nil {
		return false, err
	}
	// Record completion rather than start: a large directory scan must not
	// consume the next refresh interval while it is still running.
	b.serializableCheckpointRegionsWarmedAt.Store(time.Now().UnixNano())
	return true, nil
}

func (b *backend) validateSerializableCheckpointPublication(ctx context.Context, timestamp uint64) error {
	validator, ok := storage.FindCapability[storage.SnapshotPublicationValidator](b.kv)
	if !ok {
		return nil
	}
	if err := validator.ValidateSnapshotPublication(
		ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), timestamp,
	); err != nil {
		return fmt.Errorf("%w: revalidate checkpoint object regions: %v", ErrSerializableCheckpointUnavailable, err)
	}
	compactKey := getCompactKey(b.config.Prefix)
	compactEnd := append(append([]byte(nil), compactKey...), 0)
	if err := validator.ValidateSnapshotPublication(ctx, compactKey, compactEnd, timestamp); err != nil {
		return fmt.Errorf("%w: revalidate checkpoint compact region: %v", ErrSerializableCheckpointUnavailable, err)
	}
	return nil
}

// warmSerializableCheckpoint makes the TiKV txn client's Region cache usable
// without PD before publishing c. ScanRegions alone is insufficient because it
// uses the PD client directly and does not populate the txn client's cache; a
// snapshot point lookup at every returned Region start does.
func (b *backend) warmSerializableCheckpoint(ctx context.Context, timestamp uint64) error {
	reader, ok := storage.FindCapability[storage.SnapshotGetter](b.kv)
	if !ok {
		return ErrSerializableCheckpointUnavailable
	}
	partitions, err := b.kv.GetPartitions(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd())
	if err != nil {
		return fmt.Errorf("discover checkpoint regions: %w", err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("%w: no checkpoint regions", ErrSerializableCheckpointUnavailable)
	}
	starts := make([][]byte, 0, len(partitions)+1)
	for _, partition := range partitions {
		starts = append(starts, append([]byte(nil), partition.Start...))
	}
	// GetCompactRevisionFresh is part of every checkpoint-backed Range and
	// RangeStream compaction fence, but the raw coordination key is outside the
	// encoded object interval above. Warm and protect its Region in every
	// independent TiKV client before making the checkpoint visible.
	starts = append(starts, getCompactKey(b.config.Prefix))
	if warmer, supported := storage.FindCapability[storage.SnapshotRegionWarmer](b.kv); supported {
		if err := warmer.WarmSnapshotRegions(ctx, starts, timestamp); err != nil {
			return fmt.Errorf("warm checkpoint regions: %w", err)
		}
	} else if err := warmSnapshotRegionStarts(ctx, reader, starts, timestamp); err != nil {
		return err
	}
	return nil
}

func warmSnapshotRegionStarts(ctx context.Context, reader storage.SnapshotGetter, starts [][]byte, timestamp uint64) error {
	for _, start := range starts {
		_, err := reader.GetAt(ctx, start, timestamp)
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return fmt.Errorf("warm checkpoint region at %x: %w", start, err)
		}
	}
	return nil
}

func (b *backend) GetSerializableCheckpoint() (SerializableCheckpoint, error) {
	c := b.serializableCheckpoint.Load()
	if c == nil || !time.Now().Before(c.ValidUntil) {
		return SerializableCheckpoint{}, ErrSerializableCheckpointUnavailable
	}
	return *c, nil
}

// releaseSerializableCheckpoint stops advertising this process's checkpoint
// before removing its PD service safepoint. The registration has a finite TTL,
// so failure is safe during a PD outage; a bounded best-effort release keeps a
// normal rollout from pinning TiKV MVCC history until that TTL expires.
func (b *backend) releaseSerializableCheckpoint(ctx context.Context) error {
	b.serializableCheckpointProtectMu.Lock()
	defer b.serializableCheckpointProtectMu.Unlock()
	checkpoint := b.serializableCheckpoint.Swap(nil)
	if checkpoint == nil {
		return nil
	}
	protector, ok := storage.FindCapability[storage.SnapshotProtector](b.kv)
	if !ok {
		return nil
	}
	var releaseErr error
	for _, serviceID := range b.serializableCheckpointServiceIDs {
		if serviceID == "" {
			continue
		}
		releaseErr = errors.Join(releaseErr, protector.ReleaseSnapshot(ctx, serviceID))
	}
	return releaseErr
}

// RefreshSerializableCheckpoint synchronously establishes the first protected
// checkpoint on leadership acquisition. Engines without snapshot capabilities
// keep their existing behavior.
func (b *backend) RefreshSerializableCheckpoint(ctx context.Context) error {
	if _, ok := storage.FindCapability[storage.SnapshotGetter](b.kv); !ok {
		return nil
	}
	if _, ok := storage.FindCapability[storage.SnapshotProtector](b.kv); !ok {
		return nil
	}
	c, err := b.createSerializableCheckpoint(ctx)
	if err != nil {
		return err
	}
	return b.protectSerializableCheckpoint(ctx, c)
}

func (b *backend) runSerializableCheckpoint(workerCtx context.Context) {
	if _, ok := storage.FindCapability[storage.SnapshotGetter](b.kv); !ok {
		return
	}
	if _, ok := storage.FindCapability[storage.SnapshotProtector](b.kv); !ok {
		return
	}
	retry := serializableCheckpointRefresh
	for {
		err := b.refreshSerializableCheckpoint(workerCtx)
		if err == nil {
			retry = serializableCheckpointRefresh
		}
		timer := time.NewTimer(retry)
		select {
		case <-workerCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if err != nil {
			retry *= 2
			if retry > serializableCheckpointMaxRetry {
				retry = serializableCheckpointMaxRetry
			}
		}
	}
}

func (b *backend) refreshSerializableCheckpoint(ctx context.Context) error {
	defer b.emitSerializableCheckpointMetrics(time.Now())
	ctx, cancel := context.WithTimeout(ctx, unaryRpcTimeout)
	defer cancel()
	var c SerializableCheckpoint
	var err error
	if b.leadingFresh() {
		c, err = b.createSerializableCheckpoint(ctx)
	} else {
		c, err = b.loadSerializableCheckpoint(ctx)
	}
	if err == nil {
		err = b.protectSerializableCheckpoint(ctx, c)
	}
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) && !errors.Is(err, ErrSerializableCheckpointUnavailable) {
		b.metricCli.EmitCounter("serializable.checkpoint.refresh_err", 1)
	}
	return err
}

func (b *backend) emitSerializableCheckpointMetrics(now time.Time) {
	if b.metricCli == nil {
		return
	}
	available, revision, remaining := int64(0), int64(0), int64(0)
	if checkpoint := b.serializableCheckpoint.Load(); checkpoint != nil && now.Before(checkpoint.ValidUntil) {
		available = 1
		revision = int64(checkpoint.Revision)
		remaining = int64(checkpoint.ValidUntil.Sub(now) / time.Second)
	}
	_ = b.metricCli.EmitGauge("serializable.checkpoint.available", available)
	_ = b.metricCli.EmitGauge("serializable.checkpoint.revision", revision)
	_ = b.metricCli.EmitGauge("serializable.checkpoint.remaining_seconds", remaining)
}
