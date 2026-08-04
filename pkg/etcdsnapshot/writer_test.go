// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcdsnapshot

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"google.golang.org/protobuf/proto"
)

func TestBuilderAppendsMultipleBatchesWithoutLosingSameRevisionRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	builder, err := NewBuilder(path, State{Revision: 12})
	require.NoError(t, err)
	require.NoError(t, builder.Append([]Record{
		{Key: []byte("c"), Value: []byte("3"), CreateRevision: 7, ModRevision: 12, Version: 2},
		{Key: []byte("a"), Value: []byte("1"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}))
	require.NoError(t, builder.Append([]Record{
		{Key: []byte("b"), Value: []byte("2"), CreateRevision: 12, ModRevision: 12, Version: 1},
	}))
	require.NoError(t, builder.Finish())
	require.NoError(t, builder.Close())

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	got := make(map[string]string)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(schema.Key.Name()).ForEach(func(_, value []byte) error {
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			if !bytes.Equal(kv.Key, []byte("\x00kubebrain-snapshot-revision")) {
				got[string(kv.Key)] = string(kv.Value)
			}
			return nil
		})
	}))
	require.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, got)
}

func TestWriteBackendPreservesAuthLeasesAndAlarms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	user := &authpb.User{Name: []byte("root"), Password: []byte("bcrypt"), Roles: []string{"root"}}
	role := &authpb.Role{Name: []byte("root"), KeyPermission: []*authpb.Permission{{Key: []byte{0}, RangeEnd: []byte{0}, PermType: authpb.READWRITE}}}
	alarm := &etcdserverpb.AlarmMember{MemberID: 23, Alarm: etcdserverpb.AlarmType_NOSPACE}
	require.NoError(t, WriteBackend(path, State{
		Revision: 41,
		Records:  []Record{{Key: []byte("leased"), Value: []byte("value"), CreateRevision: 41, ModRevision: 41, Version: 1, Lease: 17}},
		Leases:   []Lease{{ID: 17, GrantedTTL: 60, RemainingTTL: 29}},
		Auth:     Auth{Enabled: true, Revision: 7, Users: []*authpb.User{user}, Roles: []*authpb.Role{role}},
		Alarms:   []*etcdserverpb.AlarmMember{alarm},
	}))

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.Equal(t, []byte{1}, tx.Bucket(schema.Auth.Name()).Get(schema.AuthEnabledKeyName))
		require.Equal(t, uint64(7), binary.BigEndian.Uint64(tx.Bucket(schema.Auth.Name()).Get(schema.AuthRevisionKeyName)))
		var gotUser authpb.User
		require.NoError(t, proto.Unmarshal(tx.Bucket(schema.AuthUsers.Name()).Get(user.Name), &gotUser))
		require.True(t, proto.Equal(user, &gotUser))
		var gotRole authpb.Role
		require.NoError(t, proto.Unmarshal(tx.Bucket(schema.AuthRoles.Name()).Get(role.Name), &gotRole))
		require.True(t, proto.Equal(role, &gotRole))
		var gotLease leasepb.Lease
		require.NoError(t, tx.Bucket(schema.Lease.Name()).ForEach(func(_, value []byte) error {
			return proto.Unmarshal(value, &gotLease)
		}))
		require.True(t, proto.Equal(&leasepb.Lease{ID: 17, TTL: 60, RemainingTTL: 29}, &gotLease))
		var gotAlarm etcdserverpb.AlarmMember
		require.NoError(t, tx.Bucket(schema.Alarm.Name()).ForEach(func(key, _ []byte) error {
			return proto.Unmarshal(key, &gotAlarm)
		}))
		require.True(t, proto.Equal(alarm, &gotAlarm))
		return nil
	}))
}
