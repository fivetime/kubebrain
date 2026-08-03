// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcdsnapshot

import (
	"encoding/binary"
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"google.golang.org/protobuf/proto"
)

var (
	keyBucket         = []byte("key")
	metaBucket        = []byte("meta")
	leaseBucket       = []byte("lease")
	alarmBucket       = []byte("alarm")
	clusterBucket     = []byte("cluster")
	membersBucket     = []byte("members")
	membersGoneBucket = []byte("members_removed")
	authBucket        = []byte("auth")
	authUsersBucket   = []byte("authUsers")
	authRolesBucket   = []byte("authRoles")
	allBuckets        = [][]byte{keyBucket, metaBucket, leaseBucket, alarmBucket, clusterBucket, membersBucket, membersGoneBucket, authBucket, authUsersBucket, authRolesBucket}
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

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return err
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		return writeState(tx, state, records)
	}); err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}

func writeState(tx *bolt.Tx, state State, records []Record) error {
	for _, name := range allBuckets {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	meta := tx.Bucket(metaBucket)
	if err := meta.Put([]byte("storageVersion"), []byte("3.7.0")); err != nil {
		return err
	}
	one := make([]byte, 8)
	binary.BigEndian.PutUint64(one, 1)
	for _, name := range [][]byte{[]byte("consistent_index"), []byte("term")} {
		if err := meta.Put(name, one); err != nil {
			return err
		}
	}
	compact := revisionBytes(state.Revision, 0)
	for _, name := range [][]byte{[]byte("scheduledCompactRev"), []byte("finishedCompactRev")} {
		if err := meta.Put(name, compact); err != nil {
			return err
		}
	}

	subs := make(map[int64]int64)
	keys := tx.Bucket(keyBucket)
	for _, rec := range records {
		encoded, err := proto.Marshal(&mvccpb.KeyValue{
			Key: rec.Key, Value: rec.Value, CreateRevision: rec.CreateRevision,
			ModRevision: rec.ModRevision, Version: rec.Version, Lease: rec.Lease,
		})
		if err != nil {
			return err
		}
		if err = keys.Put(revisionBytes(rec.ModRevision, subs[rec.ModRevision]), encoded); err != nil {
			return err
		}
		subs[rec.ModRevision]++
	}
	marker, err := proto.Marshal(&mvccpb.KeyValue{Key: []byte("\x00kubebrain-snapshot-revision")})
	if err != nil {
		return err
	}
	markerKey := append(revisionBytes(state.Revision, subs[state.Revision]), 't')
	if err = keys.Put(markerKey, marker); err != nil {
		return err
	}

	for _, lease := range state.Leases {
		if lease.ID == 0 || lease.GrantedTTL <= 0 || lease.RemainingTTL < 0 {
			return fmt.Errorf("invalid lease id=%d granted_ttl=%d remaining_ttl=%d", lease.ID, lease.GrantedTTL, lease.RemainingTTL)
		}
		value, marshalErr := proto.Marshal(&leasepb.Lease{ID: lease.ID, TTL: lease.GrantedTTL, RemainingTTL: lease.RemainingTTL})
		if marshalErr != nil {
			return marshalErr
		}
		leaseKey := make([]byte, 8)
		binary.BigEndian.PutUint64(leaseKey, uint64(lease.ID))
		if err = tx.Bucket(leaseBucket).Put(leaseKey, value); err != nil {
			return err
		}
	}
	authEnabled := byte(0)
	if state.Auth.Enabled {
		authEnabled = 1
	}
	if err = tx.Bucket(authBucket).Put([]byte("authEnabled"), []byte{authEnabled}); err != nil {
		return err
	}
	authRevision := make([]byte, 8)
	binary.BigEndian.PutUint64(authRevision, state.Auth.Revision)
	if err = tx.Bucket(authBucket).Put([]byte("authRevision"), authRevision); err != nil {
		return err
	}
	for _, user := range state.Auth.Users {
		if user == nil || len(user.Name) == 0 {
			return fmt.Errorf("snapshot contains invalid auth user")
		}
		value, marshalErr := proto.Marshal(user)
		if marshalErr != nil {
			return marshalErr
		}
		if err = tx.Bucket(authUsersBucket).Put(user.Name, value); err != nil {
			return err
		}
	}
	for _, role := range state.Auth.Roles {
		if role == nil || len(role.Name) == 0 {
			return fmt.Errorf("snapshot contains invalid auth role")
		}
		value, marshalErr := proto.Marshal(role)
		if marshalErr != nil {
			return marshalErr
		}
		if err = tx.Bucket(authRolesBucket).Put(role.Name, value); err != nil {
			return err
		}
	}
	for _, alarm := range state.Alarms {
		if alarm == nil {
			return fmt.Errorf("snapshot contains nil alarm")
		}
		key, marshalErr := proto.Marshal(alarm)
		if marshalErr != nil {
			return marshalErr
		}
		if err = tx.Bucket(alarmBucket).Put(key, nil); err != nil {
			return err
		}
	}
	return nil
}

func revisionBytes(main, sub int64) []byte {
	value := make([]byte, 17, 18)
	binary.BigEndian.PutUint64(value[:8], uint64(main))
	value[8] = '_'
	binary.BigEndian.PutUint64(value[9:], uint64(sub))
	return value
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
