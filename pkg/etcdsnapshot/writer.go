// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcdsnapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"

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

// Record is one retained user MVCC version in an etcd snapshot. Tombstones
// carry only Key and ModRevision.
type Record struct {
	Key, Value                  []byte
	CreateRevision, ModRevision int64
	Version, Lease              int64
	SubRevision                 int64
	Ordered                     bool
	Tombstone                   bool
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
	Revision           int64
	Records            []Record
	Leases             []Lease
	Auth               Auth
	Alarms             []*etcdserverpb.AlarmMember
	PreserveHistory    bool
	HasCompactRevision bool
	CompactRevision    int64
}

// WriteBackend writes an official etcd bbolt backend without the trailing
// integrity hash. Maintenance.Snapshot streams that hash as its final message.
func WriteBackend(path string, state State) error {
	if state.Revision < 0 {
		return fmt.Errorf("snapshot revision must not be negative: %d", state.Revision)
	}
	builder, err := NewBuilder(path, state)
	if err != nil {
		return err
	}
	defer builder.Close()
	if err = builder.Append(state.Records); err != nil {
		return err
	}
	return builder.Finish()
}

// Builder incrementally commits record batches into a private snapshot file.
// Committed bbolt pages, rather than the complete keyspace, hold prior batches.
type Builder struct {
	db               *bolt.DB
	revision         int64
	restoredRevision int64
	nextSub          int64
	finished         bool
}

const fallbackSubRevisionBase int64 = 1 << 32

func NewBuilder(path string, state State) (*Builder, error) {
	if state.Revision < 0 {
		return nil, fmt.Errorf("snapshot revision must not be negative: %d", state.Revision)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	restoredRevision := int64(1) // upstream MVCC restore starts at revision 1
	builder := &Builder{db: db, revision: state.Revision, restoredRevision: restoredRevision}
	if err = db.Update(func(tx *bolt.Tx) error { return writeMetadata(tx, state) }); err != nil {
		_ = db.Close()
		return nil, err
	}
	return builder, nil
}

func writeMetadata(tx *bolt.Tx, state State) error {
	var err error
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
	if !state.PreserveHistory || state.HasCompactRevision {
		compactRevision := state.Revision
		if state.PreserveHistory {
			compactRevision = state.CompactRevision
		}
		if compactRevision < 0 || compactRevision > state.Revision {
			return fmt.Errorf("invalid compact revision %d for snapshot revision %d", compactRevision, state.Revision)
		}
		compact := revisionBytes(compactRevision, 0)
		for _, name := range [][]byte{[]byte("scheduledCompactRev"), []byte("finishedCompactRev")} {
			if err := meta.Put(name, compact); err != nil {
				return err
			}
		}
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
		leases := tx.Bucket(leaseBucket)
		if leases.Get(leaseKey) != nil {
			return fmt.Errorf("duplicate lease id %d", lease.ID)
		}
		if err = leases.Put(leaseKey, value); err != nil {
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
		users := tx.Bucket(authUsersBucket)
		if users.Get(user.Name) != nil {
			return fmt.Errorf("duplicate auth user %q", user.Name)
		}
		value, marshalErr := proto.Marshal(user)
		if marshalErr != nil {
			return marshalErr
		}
		if err = users.Put(user.Name, value); err != nil {
			return err
		}
	}
	for _, role := range state.Auth.Roles {
		if role == nil || len(role.Name) == 0 {
			return fmt.Errorf("snapshot contains invalid auth role")
		}
		roles := tx.Bucket(authRolesBucket)
		if roles.Get(role.Name) != nil {
			return fmt.Errorf("duplicate auth role %q", role.Name)
		}
		value, marshalErr := proto.Marshal(role)
		if marshalErr != nil {
			return marshalErr
		}
		if err = roles.Put(role.Name, value); err != nil {
			return err
		}
	}
	if err = validateAuthState(state.Auth); err != nil {
		return err
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

func validateAuthState(auth Auth) error {
	roleNames := make(map[string]struct{}, len(auth.Roles))
	minimumRevision := uint64(1)
	minimumRevision += uint64(len(auth.Roles))
	minimumRevision += uint64(len(auth.Users))
	for _, role := range auth.Roles {
		if role != nil && len(role.Name) != 0 {
			roleNames[string(role.Name)] = struct{}{}
			minimumRevision += uint64(len(role.KeyPermission))
			seenRanges := make(map[struct{ key, end string }]struct{}, len(role.KeyPermission))
			var previousKey []byte
			for i, permission := range role.KeyPermission {
				if permission == nil {
					return fmt.Errorf("auth role %q contains nil permission at index %d", role.Name, i)
				}
				if !validSnapshotPermissionRange(permission.Key, permission.RangeEnd) {
					return fmt.Errorf("auth role %q contains invalid permission range at index %d", role.Name, i)
				}
				if i != 0 && bytes.Compare(previousKey, permission.Key) > 0 {
					return fmt.Errorf("auth role %q permissions are not key-sorted at index %d", role.Name, i)
				}
				identity := struct{ key, end string }{string(permission.Key), string(permission.RangeEnd)}
				if _, exists := seenRanges[identity]; exists {
					return fmt.Errorf("auth role %q repeats permission range at index %d", role.Name, i)
				}
				seenRanges[identity] = struct{}{}
				previousKey = permission.Key
			}
		}
	}
	var root *authpb.User
	for _, user := range auth.Users {
		if user == nil || len(user.Name) == 0 {
			continue
		}
		if string(user.Name) == "root" {
			root = user
		}
		if user.Options != nil && user.Options.NoPassword && len(user.Password) != 0 {
			return fmt.Errorf("no-password auth user %q carries password bytes", user.Name)
		}
		seenRoles := make(map[string]struct{}, len(user.Roles))
		minimumRevision += uint64(len(user.Roles))
		for i, role := range user.Roles {
			if i != 0 && user.Roles[i-1] > role {
				return fmt.Errorf("auth user %q roles are not sorted at index %d", user.Name, i)
			}
			if _, exists := seenRoles[role]; exists {
				return fmt.Errorf("auth user %q repeats role %q", user.Name, role)
			}
			seenRoles[role] = struct{}{}
			if _, exists := roleNames[role]; !exists {
				return fmt.Errorf("auth user %q references missing role %q", user.Name, role)
			}
		}
	}
	if auth.Enabled {
		if root == nil {
			return fmt.Errorf("enabled auth requires root user")
		}
		hasRootRole := false
		for _, role := range root.Roles {
			if role == "root" {
				hasRootRole = true
				break
			}
		}
		if !hasRootRole {
			return fmt.Errorf("enabled auth requires root user to have root role")
		}
	}
	if len(auth.Users) == 0 && len(auth.Roles) == 0 && !auth.Enabled && auth.Revision == 0 {
		return nil
	}
	if auth.Revision < minimumRevision {
		return fmt.Errorf("auth revision %d is below graph minimum %d", auth.Revision, minimumRevision)
	}
	return nil
}

func validSnapshotPermissionRange(key, end []byte) bool {
	if len(key) == 0 {
		return false
	}
	return len(end) == 0 || bytes.Compare(key, end) < 0 || (len(end) == 1 && end[0] == 0)
}

func (b *Builder) Append(records []Record) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("snapshot builder is closed")
	}
	if b.finished {
		return fmt.Errorf("snapshot builder is already finished")
	}
	for i := range records {
		if err := validateRecord(records[i], b.revision); err != nil {
			return fmt.Errorf("record %d: %w", b.nextSub+int64(i)+1, err)
		}
	}
	nextSub := b.nextSub
	restoredRevision := b.restoredRevision
	err := b.db.Update(func(tx *bolt.Tx) error {
		keys := tx.Bucket(keyBucket)
		for _, rec := range records {
			kv := &mvccpb.KeyValue{
				Key: rec.Key, Value: rec.Value, CreateRevision: rec.CreateRevision,
				ModRevision: rec.ModRevision, Version: rec.Version, Lease: rec.Lease,
			}
			if rec.Tombstone {
				kv = &mvccpb.KeyValue{Key: rec.Key}
			}
			encoded, err := proto.Marshal(kv)
			if err != nil {
				return err
			}
			subRevision := fallbackSubRevisionBase + nextSub
			if rec.Ordered {
				subRevision = rec.SubRevision
			}
			revisionKey := revisionBytes(rec.ModRevision, subRevision)
			if rec.Ordered {
				tombstoneKey := append(append([]byte(nil), revisionKey...), 't')
				if keys.Get(revisionKey) != nil || keys.Get(tombstoneKey) != nil {
					return fmt.Errorf("duplicate ordered revision %d/%d", rec.ModRevision, rec.SubRevision)
				}
			}
			if rec.Tombstone {
				revisionKey = append(revisionKey, 't')
			}
			if err = keys.Put(revisionKey, encoded); err != nil {
				return err
			}
			if rec.ModRevision > restoredRevision {
				restoredRevision = rec.ModRevision
			}
			nextSub++
		}
		return nil
	})
	if err == nil {
		b.nextSub = nextSub
		b.restoredRevision = restoredRevision
	}
	return err
}

func (b *Builder) Finish() error {
	if b == nil || b.db == nil {
		return fmt.Errorf("snapshot builder is closed")
	}
	if b.finished {
		return fmt.Errorf("snapshot builder is already finished")
	}
	if b.restoredRevision >= b.revision {
		b.finished = true
		return nil
	}
	// Official MVCC restore derives currentRev from the greatest revision key,
	// so a marker is required when KubeBrain's published revision has no retained
	// user row (for example, a dealt-but-uncommitted revision). Use the empty key:
	// etcd's Watch API normalizes an empty watch key to \x00, and Range/Txn reject
	// empty keys, making this restore-only row unreachable to every legal client
	// key interval. A named \x00-prefixed marker would leak as a DELETE through an
	// all-key historical Watch after restore.
	marker, err := proto.Marshal(&mvccpb.KeyValue{Key: []byte{}})
	if err != nil {
		return err
	}
	err = b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(keyBucket).Put(append(revisionBytes(b.revision, fallbackSubRevisionBase+b.nextSub), 't'), marker)
	})
	if err == nil {
		b.finished = true
	}
	return err
}

func (b *Builder) Close() error {
	if b == nil || b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
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
	if rec.Ordered && (rec.SubRevision < 0 || rec.SubRevision >= fallbackSubRevisionBase) {
		return fmt.Errorf("invalid ordered subrevision %d", rec.SubRevision)
	}
	if rec.Tombstone {
		if rec.ModRevision <= 0 || rec.ModRevision > snapshotRevision {
			return fmt.Errorf("invalid tombstone revision mod=%d snapshot=%d", rec.ModRevision, snapshotRevision)
		}
		if len(rec.Value) != 0 || rec.CreateRevision != 0 || rec.Version != 0 || rec.Lease != 0 {
			return fmt.Errorf("tombstone must not carry value metadata")
		}
		return nil
	}
	if rec.CreateRevision <= 0 || rec.ModRevision < rec.CreateRevision || rec.ModRevision > snapshotRevision || rec.Version <= 0 {
		return fmt.Errorf("invalid MVCC metadata create=%d mod=%d version=%d snapshot=%d", rec.CreateRevision, rec.ModRevision, rec.Version, snapshotRevision)
	}
	if rec.Version > rec.ModRevision-rec.CreateRevision+1 {
		return fmt.Errorf("version %d cannot fit between create revision %d and mod revision %d", rec.Version, rec.CreateRevision, rec.ModRevision)
	}
	return nil
}
