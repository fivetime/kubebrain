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
	_ = metricCli.EmitGauge("serializable.checkpoint.remaining_seconds", int64(0))
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
	ctx = storage.WithSnapshotTimestamp(ctx, c.Timestamp)
	return context.WithValue(ctx, serializableCheckpointContextKey{}, c)
}

// SerializableCheckpointFromContext returns the request-pinned checkpoint.
// Its local safety deadline is checked before it is attached; an in-flight
// request may continue using that already protected snapshot until completion.
func SerializableCheckpointFromContext(ctx context.Context) (SerializableCheckpoint, bool) {
	c, ok := ctx.Value(serializableCheckpointContextKey{}).(SerializableCheckpoint)
	return c, ok && c.Revision != 0 && c.Timestamp != 0
}

func (b *backend) snapshotGet(ctx context.Context, key []byte) ([]byte, error) {
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
		defer it.Close()
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
	compactRevision, err := b.loadCompactRevision(ctx)
	if err != nil {
		return SerializableCheckpoint{}, err
	}
	authRevision := uint64(0)
	authValue, authErr := b.kv.Get(ctx, b.ks.EncodeInternalKey([]byte("auth/config")))
	if authErr == nil {
		if len(authValue) != 9 || authValue[0] > 1 {
			return SerializableCheckpoint{}, fmt.Errorf("invalid auth config while creating serializable checkpoint")
		}
		authRevision = binary.BigEndian.Uint64(authValue[1:])
		if authRevision == 0 {
			return SerializableCheckpoint{}, fmt.Errorf("zero auth revision while creating serializable checkpoint")
		}
	} else if !errors.Is(authErr, storage.ErrKeyNotFound) {
		return SerializableCheckpoint{}, authErr
	}
	if current := b.serializableCheckpoint.Load(); current != nil &&
		current.Revision == revision && current.CompactRevision == compactRevision && current.AuthRevision == authRevision {
		return *current, nil
	}
	if err := b.persistDurableRevisionContext(ctx, revision); err != nil {
		return SerializableCheckpoint{}, err
	}
	timestamp, err := b.kv.GetTimestampOracle(ctx)
	if err != nil {
		return SerializableCheckpoint{}, err
	}
	c := SerializableCheckpoint{Revision: revision, Timestamp: timestamp, CompactRevision: compactRevision, AuthRevision: authRevision}
	batch := b.kv.BeginBatchWrite()
	batch.Put(b.ks.EncodeInternalKey(serializableCheckpointKey), encodeSerializableCheckpoint(c), 0)
	if err := batch.Commit(b.withLogicalWriteOwnership(ctx)); err != nil {
		return SerializableCheckpoint{}, err
	}
	return c, nil
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
	minimum, err := protector.ProtectSnapshot(ctx, b.serializableCheckpointServiceID, serializableCheckpointTTL, c.Timestamp)
	if err != nil {
		return err
	}
	if minimum > c.Timestamp {
		return fmt.Errorf("%w: GC safepoint %d passed snapshot %d", ErrSerializableCheckpointUnavailable, minimum, c.Timestamp)
	}
	if err := b.refreshSerializableCheckpointRegions(ctx, c.Timestamp); err != nil {
		return err
	}
	c.ValidUntil = time.Now().Add(serializableCheckpointUsable)
	b.serializableCheckpoint.Store(&c)
	return nil
}

func (b *backend) refreshSerializableCheckpointRegions(ctx context.Context, timestamp uint64) error {
	b.serializableCheckpointRegionWarmMu.Lock()
	defer b.serializableCheckpointRegionWarmMu.Unlock()
	now := time.Now()
	warmedAt := b.serializableCheckpointRegionsWarmedAt.Load()
	if warmedAt != 0 && now.UnixNano() >= warmedAt && now.UnixNano()-warmedAt < int64(serializableCheckpointRegionRefresh) {
		return nil
	}
	if err := b.warmSerializableCheckpoint(ctx, timestamp); err != nil {
		return err
	}
	// Record completion rather than start: a large directory scan must not
	// consume the next refresh interval while it is still running.
	b.serializableCheckpointRegionsWarmedAt.Store(time.Now().UnixNano())
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
	starts := make([][]byte, 0, len(partitions))
	for _, partition := range partitions {
		starts = append(starts, append([]byte(nil), partition.Start...))
	}
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
	available, remaining := int64(0), int64(0)
	if checkpoint := b.serializableCheckpoint.Load(); checkpoint != nil && now.Before(checkpoint.ValidUntil) {
		available = 1
		remaining = int64(checkpoint.ValidUntil.Sub(now) / time.Second)
	}
	_ = b.metricCli.EmitGauge("serializable.checkpoint.available", available)
	_ = b.metricCli.EmitGauge("serializable.checkpoint.remaining_seconds", remaining)
}
