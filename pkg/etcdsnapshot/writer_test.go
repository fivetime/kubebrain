// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcdsnapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdAuth "go.etcd.io/etcd/server/v3/auth"
	v3alarm "go.etcd.io/etcd/server/v3/etcdserver/api/v3alarm"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	etcdbackend "go.etcd.io/etcd/server/v3/storage/backend"
	mvcc "go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/bcrypt"
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
			if len(kv.Key) != 0 {
				got[string(kv.Key)] = string(kv.Value)
			}
			return nil
		})
	}))
	require.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, got)
}

func TestBuilderUsesExactTxnSubrevisionInsteadOfPhysicalInputOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	builder, err := NewBuilder(path, State{Revision: 12, PreserveHistory: true})
	require.NoError(t, err)
	// Object storage scans these in key order. The transaction that produced the
	// revision wrote z, a, m, which must remain the order seen by restored Watch.
	require.NoError(t, builder.Append([]Record{
		{Key: []byte("a"), Value: []byte("a"), CreateRevision: 12, ModRevision: 12, Version: 1, SubRevision: 1, Ordered: true},
		{Key: []byte("m"), Value: []byte("m"), CreateRevision: 12, ModRevision: 12, Version: 1, SubRevision: 2, Ordered: true},
		{Key: []byte("z"), Value: []byte("z"), CreateRevision: 12, ModRevision: 12, Version: 1, SubRevision: 0, Ordered: true},
	}))
	require.NoError(t, builder.Finish())
	require.NoError(t, builder.Close())

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var keys []string
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(schema.Key.Name()).ForEach(func(revisionKey, value []byte) error {
			if mvcc.BytesToRev(revisionKey).Main != 12 || mvcc.IsTombstone(revisionKey) {
				return nil
			}
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			keys = append(keys, string(kv.Key))
			return nil
		})
	}))
	require.Equal(t, []string{"z", "a", "m"}, keys)
}

func TestBuilderOmitsRevisionMarkerWhenRealRowAlreadyPinsRevision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, WriteBackend(path, State{
		Revision: 12, PreserveHistory: true,
		Records: []Record{{Key: []byte("real"), Value: []byte("value"), CreateRevision: 12, ModRevision: 12, Version: 1}},
	}))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		rows := 0
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(_, _ []byte) error {
			rows++
			return nil
		}))
		require.Equal(t, 1, rows)
		return nil
	}))
}

func TestBuilderPinsCompactedSnapshotRevisionAboveLatestLiveRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, WriteBackend(path, State{
		Revision: 12,
		Records:  []Record{{Key: []byte("real"), Value: []byte("value"), CreateRevision: 7, ModRevision: 7, Version: 1}},
	}))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var revisions []int64
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(key, _ []byte) error {
			revisions = append(revisions, mvcc.BytesToRev(key).Main)
			return nil
		}))
		require.Equal(t, []int64{7, 12}, revisions)
		return nil
	}))
}

func TestBuilderPreservesHistoryTombstonesAndRealCompactWatermark(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	builder, err := NewBuilder(path, State{
		Revision: 9, PreserveHistory: true, HasCompactRevision: true, CompactRevision: 3,
	})
	require.NoError(t, err)
	require.NoError(t, builder.Append([]Record{
		{Key: []byte("k"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1},
		{Key: []byte("k"), ModRevision: 4, Tombstone: true},
		{Key: []byte("k"), Value: []byte("v2"), CreateRevision: 7, ModRevision: 7, Version: 1},
	}))
	require.NoError(t, builder.Finish())
	require.NoError(t, builder.Close())

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(schema.Meta.Name())
		require.Equal(t, revisionBytes(3, 0), meta.Get(schema.FinishedCompactKeyName))
		require.Equal(t, revisionBytes(3, 0), meta.Get(schema.ScheduledCompactKeyName))
		var revisions []int64
		var tombstones []bool
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(key, value []byte) error {
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			if !bytes.Equal(kv.Key, []byte("k")) {
				return nil
			}
			revisions = append(revisions, mvcc.BytesToRev(key).Main)
			tombstones = append(tombstones, mvcc.IsTombstone(key))
			return nil
		}))
		require.Equal(t, []int64{2, 4, 7}, revisions)
		require.Equal(t, []bool{false, true, false}, tombstones)
		return nil
	}))
}

func TestBuilderOmitsCompactMarkersForUncompactedHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, WriteBackend(path, State{
		Revision: 2, PreserveHistory: true,
		Records: []Record{{Key: []byte("k"), Value: []byte("v"), CreateRevision: 2, ModRevision: 2, Version: 1}},
	}))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(schema.Meta.Name())
		require.Nil(t, meta.Get(schema.FinishedCompactKeyName))
		require.Nil(t, meta.Get(schema.ScheduledCompactKeyName))
		return nil
	}))
}

func TestOfficialMVCCStoreRestoresHistoricalRangesAndTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, WriteBackend(path, State{
		Revision: 9, PreserveHistory: true,
		Records: []Record{
			{Key: []byte("k"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1},
			{Key: []byte("k"), Value: []byte("v2"), CreateRevision: 2, ModRevision: 3, Version: 2},
			{Key: []byte("k"), ModRevision: 4, Tombstone: true},
			{Key: []byte("k"), Value: []byte("v3"), CreateRevision: 7, ModRevision: 7, Version: 1},
		},
	}))
	lg := zaptest.NewLogger(t)
	be := etcdbackend.NewDefaultBackend(lg, path)
	store := mvcc.NewStore(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	defer func() {
		store.Close()
		require.NoError(t, be.Close())
	}()
	for _, tc := range []struct {
		revision int64
		value    string
		present  bool
	}{
		{revision: 2, value: "v1", present: true},
		{revision: 3, value: "v2", present: true},
		{revision: 4},
		{revision: 6},
		{revision: 7, value: "v3", present: true},
	} {
		result, err := store.Range(context.Background(), []byte("k"), nil, mvcc.RangeOptions{Rev: tc.revision})
		require.NoError(t, err)
		if !tc.present {
			require.Empty(t, result.KVs, "revision %d", tc.revision)
			continue
		}
		require.Len(t, result.KVs, 1)
		require.Equal(t, tc.value, string(result.KVs[0].Value))
	}
}

func TestOfficialWatchDoesNotExposeSyntheticSnapshotRevisionMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, WriteBackend(path, State{
		Revision: 9, PreserveHistory: true,
		Records: []Record{{Key: []byte("visible"), Value: []byte("value"), CreateRevision: 2, ModRevision: 2, Version: 1}},
	}))
	lg := zaptest.NewLogger(t)
	be := etcdbackend.NewDefaultBackend(lg, path)
	store := mvcc.New(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	defer func() {
		store.Close()
		require.NoError(t, be.Close())
	}()
	watch := store.NewWatchStream()
	defer watch.Close()
	_, err := watch.Watch(context.Background(), 0, []byte{0}, []byte{}, 2)
	require.NoError(t, err)
	select {
	case response := <-watch.Chan():
		require.Zero(t, response.CompactRevision)
		require.EqualValues(t, 9, response.Revision)
		require.Len(t, response.Events, 1)
		require.Equal(t, []byte("visible"), response.Events[0].Kv.Key)
	case <-time.After(time.Second):
		t.Fatal("official restored watch did not replay snapshot revision")
	}
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

func TestOfficialAuthAndAlarmStoresRecoverGeneratedBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	password, err := bcrypt.GenerateFromPassword([]byte("alice-secret"), bcrypt.MinCost)
	require.NoError(t, err)
	rootPassword, err := bcrypt.GenerateFromPassword([]byte("root-secret"), bcrypt.MinCost)
	require.NoError(t, err)
	reader := &authpb.Role{Name: []byte("reader"), KeyPermission: []*authpb.Permission{{
		Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"), PermType: authpb.READ,
	}}}
	require.NoError(t, WriteBackend(path, State{
		Revision: 41,
		Auth: Auth{Enabled: true, Revision: 7,
			Users: []*authpb.User{
				{Name: []byte("root"), Password: rootPassword, Roles: []string{"root"}},
				{Name: []byte("alice"), Password: password, Roles: []string{"reader"}},
			},
			Roles: []*authpb.Role{
				{Name: []byte("root"), KeyPermission: []*authpb.Permission{{Key: []byte{0}, RangeEnd: []byte{0}, PermType: authpb.READWRITE}}},
				reader,
			},
		},
		Alarms: []*etcdserverpb.AlarmMember{
			{MemberID: 23, Alarm: etcdserverpb.AlarmType_NOSPACE},
			{MemberID: 29, Alarm: etcdserverpb.AlarmType(127)},
		},
	}))

	lg := zaptest.NewLogger(t)
	be := etcdbackend.NewDefaultBackend(lg, path)
	defer func() { require.NoError(t, be.Close()) }()
	ready := func(uint64) <-chan struct{} {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	tokens, err := etcdAuth.NewTokenProvider(lg, "simple", ready, time.Minute)
	require.NoError(t, err)
	authStore := etcdAuth.NewAuthStore(lg, schema.NewAuthBackend(lg, be), tokens, bcrypt.MinCost)
	defer func() { require.NoError(t, authStore.Close()) }()
	require.True(t, authStore.IsAuthEnabled())
	require.EqualValues(t, 7, authStore.Revision())
	revision, err := authStore.CheckPassword("alice", "alice-secret")
	require.NoError(t, err)
	require.EqualValues(t, 7, revision)
	alice := &etcdAuth.AuthInfo{Username: "alice", Revision: 7}
	require.NoError(t, authStore.IsRangePermitted(alice, []byte("/allowed/key"), nil))
	require.Error(t, authStore.IsRangePermitted(alice, []byte("/denied/key"), nil))

	alarmStore, err := v3alarm.NewAlarmStore(lg, schema.NewAlarmBackend(lg, be))
	require.NoError(t, err)
	gotAlarms := make([]string, 0, 2)
	for _, alarm := range alarmStore.Get(etcdserverpb.AlarmType_NONE) {
		gotAlarms = append(gotAlarms, fmt.Sprintf("%d/%d", alarm.MemberID, alarm.Alarm))
	}
	require.ElementsMatch(t, []string{"23/1", "29/127"}, gotAlarms)
}
