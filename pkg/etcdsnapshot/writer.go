// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcdsnapshot

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/coreos/go-semver/semver"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Record is one live key in a compacted-current-state etcd snapshot.
type Record struct {
	Key, Value                  []byte
	CreateRevision, ModRevision int64
	Version, Lease              int64
}

// Lease is the durable state needed for etcd to restore a lease.
type Lease struct {
	ID, GrantedTTL, RemainingTTL int64
}

// Auth is a self-consistent etcd authentication snapshot.
type Auth struct {
	Enabled  bool
	Revision uint64
	Users    []*authpb.User
	Roles    []*authpb.Role
}

// State describes the client-visible state encoded in a generated backend.
// The MVCC history is intentionally compacted to the supplied live records.
type State struct {
	Revision int64
	Records  []Record
	Leases   []Lease
	Auth     Auth
	Alarms   []*etcdserverpb.AlarmMember
}

// WriteBackend writes an official etcd bbolt backend without the trailing
// integrity hash. Maintenance.Snapshot streams that hash as its final message.
func WriteBackend(path string, state State) error {
	if state.Revision < 0 {
		return fmt.Errorf("snapshot revision must not be negative: %d", state.Revision)
	}
	records := append([]Record(nil), state.Records...)
	for i := range records {
		if err := validateRecord(records[i], state.Revision); err != nil {
			return fmt.Errorf("record %d: %w", i+1, err)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].ModRevision != records[j].ModRevision {
			return records[i].ModRevision < records[j].ModRevision
		}
		return string(records[i].Key) < string(records[j].Key)
	})

	be := backend.NewDefaultBackend(zap.NewNop(), path, backend.WithMmapSize(64*1024*1024))
	closed := false
	defer func() {
		if !closed {
			_ = be.Close()
		}
	}()
	tx := be.BatchTx()
	tx.LockOutsideApply()
	locked := true
	defer func() {
		if locked {
			tx.Unlock()
		}
	}()
	for _, bucket := range schema.AllBuckets {
		tx.UnsafeCreateBucket(bucket)
	}
	schema.UnsafeSetStorageVersion(tx, semver.Must(semver.NewVersion("3.7.0")))
	schema.UnsafeUpdateConsistentIndexForce(tx, 1, 1)
	mvcc.UnsafeSetScheduledCompact(tx, state.Revision)
	mvcc.UnsafeSetFinishedCompact(tx, state.Revision)

	subs := make(map[int64]int64)
	for _, rec := range records {
		encoded, err := proto.Marshal(&mvccpb.KeyValue{
			Key: rec.Key, Value: rec.Value, CreateRevision: rec.CreateRevision,
			ModRevision: rec.ModRevision, Version: rec.Version, Lease: rec.Lease,
		})
		if err != nil {
			return err
		}
		revKey := mvcc.RevToBytes(mvcc.Revision{Main: rec.ModRevision, Sub: subs[rec.ModRevision]}, mvcc.NewRevBytes())
		subs[rec.ModRevision]++
		tx.UnsafeSeqPut(schema.Key, revKey, encoded)
	}
	marker, err := proto.Marshal(&mvccpb.KeyValue{Key: []byte("\x00kubebrain-snapshot-revision")})
	if err != nil {
		return err
	}
	markerKey := mvcc.RevToBytes(mvcc.Revision{Main: state.Revision, Sub: subs[state.Revision]}, mvcc.NewRevBytes())
	markerKey = append(markerKey, 't')
	tx.UnsafeSeqPut(schema.Key, markerKey, marker)

	for _, lease := range state.Leases {
		if lease.ID == 0 || lease.GrantedTTL <= 0 || lease.RemainingTTL < 0 {
			return fmt.Errorf("invalid lease id=%d granted_ttl=%d remaining_ttl=%d", lease.ID, lease.GrantedTTL, lease.RemainingTTL)
		}
		schema.MustUnsafePutLease(tx, &leasepb.Lease{ID: lease.ID, TTL: lease.GrantedTTL, RemainingTTL: lease.RemainingTTL})
	}
	authEnabled := byte(0)
	if state.Auth.Enabled {
		authEnabled = 1
	}
	tx.UnsafePut(schema.Auth, schema.AuthEnabledKeyName, []byte{authEnabled})
	authRevision := make([]byte, 8)
	binary.BigEndian.PutUint64(authRevision, state.Auth.Revision)
	tx.UnsafePut(schema.Auth, schema.AuthRevisionKeyName, authRevision)
	for _, user := range state.Auth.Users {
		if user == nil || len(user.Name) == 0 {
			return fmt.Errorf("snapshot contains invalid auth user")
		}
		value, marshalErr := proto.Marshal(user)
		if marshalErr != nil {
			return marshalErr
		}
		tx.UnsafePut(schema.AuthUsers, user.Name, value)
	}
	for _, role := range state.Auth.Roles {
		if role == nil || len(role.Name) == 0 {
			return fmt.Errorf("snapshot contains invalid auth role")
		}
		value, marshalErr := proto.Marshal(role)
		if marshalErr != nil {
			return marshalErr
		}
		tx.UnsafePut(schema.AuthRoles, role.Name, value)
	}
	for _, alarm := range state.Alarms {
		if alarm == nil {
			return fmt.Errorf("snapshot contains nil alarm")
		}
		key, marshalErr := proto.Marshal(alarm)
		if marshalErr != nil {
			return marshalErr
		}
		tx.UnsafePut(schema.Alarm, key, nil)
	}
	tx.Unlock()
	locked = false
	be.ForceCommit()
	if err := be.Close(); err != nil {
		return err
	}
	closed = true
	return nil
}

func validateRecord(rec Record, snapshotRevision int64) error {
	if len(rec.Key) == 0 {
		return fmt.Errorf("empty key")
	}
	if rec.CreateRevision <= 0 || rec.ModRevision < rec.CreateRevision || rec.ModRevision > snapshotRevision || rec.Version <= 0 {
		return fmt.Errorf("invalid MVCC metadata create=%d mod=%d version=%d snapshot=%d", rec.CreateRevision, rec.ModRevision, rec.Version, snapshotRevision)
	}
	if rec.Version > rec.ModRevision-rec.CreateRevision+1 {
		return fmt.Errorf("version %d cannot fit between create revision %d and mod revision %d", rec.Version, rec.CreateRevision, rec.ModRevision)
	}
	return nil
}
