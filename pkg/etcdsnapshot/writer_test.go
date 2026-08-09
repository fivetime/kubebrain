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

func TestBuilderRejectsDuplicateOrderedRevisionWithoutOverwritingHistory(t *testing.T) {
	for _, test := range []struct {
		name       string
		firstBatch []Record
	}{
		{
			name: "same batch",
			firstBatch: []Record{
				{Key: []byte("first"), Value: []byte("one"), CreateRevision: 7, ModRevision: 7, Version: 1, SubRevision: 2, Ordered: true},
				{Key: []byte("second"), Value: []byte("two"), CreateRevision: 7, ModRevision: 7, Version: 1, SubRevision: 2, Ordered: true},
			},
		},
		{
			name: "later batch",
			firstBatch: []Record{
				{Key: []byte("first"), Value: []byte("one"), CreateRevision: 7, ModRevision: 7, Version: 1, SubRevision: 2, Ordered: true},
			},
		},
		{
			name: "after tombstone",
			firstBatch: []Record{
				{Key: []byte("first"), ModRevision: 7, SubRevision: 2, Ordered: true, Tombstone: true},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.db")
			builder, err := NewBuilder(path, State{Revision: 7, PreserveHistory: true})
			require.NoError(t, err)

			if test.name == "same batch" {
				err = builder.Append(test.firstBatch)
			} else {
				require.NoError(t, builder.Append(test.firstBatch))
				err = builder.Append([]Record{
					{Key: []byte("second"), ModRevision: 7, SubRevision: 2, Ordered: true, Tombstone: true},
				})
			}
			require.ErrorContains(t, err, "duplicate ordered revision 7/2")
			require.NoError(t, builder.Finish())
			require.NoError(t, builder.Close())

			lg := zaptest.NewLogger(t)
			be := etcdbackend.NewDefaultBackend(lg, path)
			store := mvcc.NewStore(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
			defer func() {
				store.Close()
				require.NoError(t, be.Close())
			}()
			first, rangeErr := store.Range(context.Background(), []byte("first"), nil, mvcc.RangeOptions{})
			require.NoError(t, rangeErr)
			second, rangeErr := store.Range(context.Background(), []byte("second"), nil, mvcc.RangeOptions{})
			require.NoError(t, rangeErr)
			if test.name == "same batch" {
				require.Empty(t, first.KVs, "the rejected batch must be atomic")
			} else if test.name == "later batch" {
				require.Len(t, first.KVs, 1)
				require.Equal(t, "one", string(first.KVs[0].Value))
			} else {
				require.Empty(t, first.KVs)
			}
			require.Empty(t, second.KVs)
		})
	}
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

func TestWriteBackendRejectsDuplicateMetadataIdentities(t *testing.T) {
	alarmWithUnknownFields := &etcdserverpb.AlarmMember{MemberID: 23, Alarm: etcdserverpb.AlarmType_NOSPACE}
	alarmWithUnknownFields.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	for _, test := range []struct {
		name  string
		state State
		want  string
	}{
		{
			name: "lease ID",
			state: State{Leases: []Lease{
				{ID: 7, GrantedTTL: 60, RemainingTTL: 30},
				{ID: 7, GrantedTTL: 120, RemainingTTL: 90},
			}},
			want: "duplicate lease id 7",
		},
		{
			name: "auth user",
			state: State{Auth: Auth{Users: []*authpb.User{
				{Name: []byte("alice"), Roles: []string{"reader"}},
				{Name: []byte("alice"), Roles: []string{"writer"}},
			}}},
			want: `duplicate auth user "alice"`,
		},
		{
			name: "auth role",
			state: State{Auth: Auth{Roles: []*authpb.Role{
				{Name: []byte("reader")},
				{Name: []byte("reader"), KeyPermission: []*authpb.Permission{{Key: []byte("/"), PermType: authpb.READ}}},
			}}},
			want: `duplicate auth role "reader"`,
		},
		{
			name: "logical alarm",
			state: State{Alarms: []*etcdserverpb.AlarmMember{
				{MemberID: 23, Alarm: etcdserverpb.AlarmType_NOSPACE},
				alarmWithUnknownFields,
			}},
			want: "duplicate alarm member=23 type=NOSPACE",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.db")
			err := WriteBackend(path, test.state)
			require.ErrorContains(t, err, test.want)

			db, openErr := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
			require.NoError(t, openErr)
			defer db.Close()
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				require.Nil(t, tx.Bucket(schema.Lease.Name()), "metadata transaction must roll back all buckets")
				require.Nil(t, tx.Bucket(schema.AuthUsers.Name()))
				require.Nil(t, tx.Bucket(schema.AuthRoles.Name()))
				require.Nil(t, tx.Bucket(schema.Alarm.Name()))
				return nil
			}))
		})
	}
}

func TestWriteBackendRejectsInconsistentAuthState(t *testing.T) {
	rootRole := &authpb.Role{Name: []byte("root")}
	for _, test := range []struct {
		name string
		auth Auth
		want string
	}{
		{
			name: "enabled without root user",
			auth: Auth{Enabled: true, Revision: 7, Roles: []*authpb.Role{rootRole}},
			want: "enabled auth requires root user",
		},
		{
			name: "enabled root lacks root role",
			auth: Auth{Enabled: true, Revision: 7,
				Users: []*authpb.User{{Name: []byte("root")}},
				Roles: []*authpb.Role{rootRole}},
			want: "enabled auth requires root user to have root role",
		},
		{
			name: "user references missing role",
			auth: Auth{
				Users: []*authpb.User{{Name: []byte("alice"), Roles: []string{"missing"}}},
			},
			want: `auth user "alice" references missing role "missing"`,
		},
		{
			name: "user repeats role",
			auth: Auth{
				Users: []*authpb.User{{Name: []byte("alice"), Roles: []string{"reader", "reader"}}},
				Roles: []*authpb.Role{{Name: []byte("reader")}},
			},
			want: `auth user "alice" repeats role "reader"`,
		},
		{
			name: "user roles are not sorted",
			auth: Auth{Revision: 6,
				Users: []*authpb.User{{Name: []byte("alice"), Roles: []string{"writer", "reader"}}},
				Roles: []*authpb.Role{{Name: []byte("reader")}, {Name: []byte("writer")}},
			},
			want: `auth user "alice" roles are not sorted at index 1`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.db")
			err := WriteBackend(path, State{Auth: test.auth})
			require.ErrorContains(t, err, test.want)
			db, openErr := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
			require.NoError(t, openErr)
			defer db.Close()
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				require.Nil(t, tx.Bucket(schema.Auth.Name()), "inconsistent auth must roll back metadata")
				return nil
			}))
		})
	}
}

func TestWriteBackendRejectsPasswordOnNoPasswordUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	err := WriteBackend(path, State{Auth: Auth{Users: []*authpb.User{{
		Name: []byte("certificate-only"), Password: []byte("unexpected"),
		Options: &authpb.UserAddOptions{NoPassword: true},
	}}}})
	require.ErrorContains(t, err, `no-password auth user "certificate-only" carries password bytes`)
}

func TestWriteBackendRejectsAuthRevisionBelowReachableGraphMinimum(t *testing.T) {
	for _, test := range []struct {
		name string
		auth Auth
		want string
	}{
		{
			name: "user at initialization revision",
			auth: Auth{Revision: 1, Users: []*authpb.User{{Name: []byte("alice")}}},
			want: "auth revision 1 is below graph minimum 2",
		},
		{
			name: "role permission needs a separate mutation",
			auth: Auth{Revision: 2, Roles: []*authpb.Role{{
				Name:          []byte("reader"),
				KeyPermission: []*authpb.Permission{{Key: []byte("/"), PermType: authpb.READ}},
			}}},
			want: "auth revision 2 is below graph minimum 3",
		},
		{
			name: "user role grant needs a separate mutation",
			auth: Auth{Revision: 3,
				Users: []*authpb.User{{Name: []byte("alice"), Roles: []string{"reader"}}},
				Roles: []*authpb.Role{{Name: []byte("reader")}},
			},
			want: "auth revision 3 is below graph minimum 4",
		},
		{
			name: "enabled root graph needs three mutations",
			auth: Auth{Enabled: true, Revision: 3,
				Users: []*authpb.User{{Name: []byte("root"), Roles: []string{"root"}}},
				Roles: []*authpb.Role{{Name: []byte("root")}},
			},
			want: "auth revision 3 is below graph minimum 4",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.db")
			err := WriteBackend(path, State{Auth: test.auth})
			require.ErrorContains(t, err, test.want)
			db, openErr := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
			require.NoError(t, openErr)
			defer db.Close()
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				require.Nil(t, tx.Bucket(schema.Auth.Name()), "invalid auth revision must roll back metadata")
				return nil
			}))
		})
	}
}

func TestWriteBackendAllowsEmptyUninitializedAuthRevision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, WriteBackend(path, State{Auth: Auth{}}))

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.Equal(t, make([]byte, 8), tx.Bucket(schema.Auth.Name()).Get(schema.AuthRevisionKeyName))
		return nil
	}))
	require.NoError(t, db.Close())

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
	require.EqualValues(t, 1, authStore.Revision(), "upstream initializes the empty revision-zero sentinel")
	require.NoError(t, authStore.Close())
}

func TestWriteBackendPreservesOpaqueHashedPasswordBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	opaque := []byte("not-necessarily-bcrypt")
	require.NoError(t, WriteBackend(path, State{Auth: Auth{Revision: 2, Users: []*authpb.User{{
		Name: []byte("legacy"), Password: opaque,
	}}}}))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var user authpb.User
		require.NoError(t, proto.Unmarshal(tx.Bucket(schema.AuthUsers.Name()).Get([]byte("legacy")), &user))
		require.Equal(t, opaque, user.Password)
		return nil
	}))
}

func TestWriteBackendRejectsImpossibleRolePermissions(t *testing.T) {
	for _, test := range []struct {
		name        string
		permissions []*authpb.Permission
		want        string
	}{
		{name: "nil permission", permissions: []*authpb.Permission{nil}, want: `auth role "reader" contains nil permission`},
		{name: "empty key", permissions: []*authpb.Permission{{PermType: authpb.READ}}, want: `auth role "reader" contains invalid permission range at index 0`},
		{name: "empty interval", permissions: []*authpb.Permission{{PermType: authpb.READ, Key: []byte("b"), RangeEnd: []byte("b")}}, want: `auth role "reader" contains invalid permission range at index 0`},
		{name: "descending interval", permissions: []*authpb.Permission{{PermType: authpb.READ, Key: []byte("b"), RangeEnd: []byte("a")}}, want: `auth role "reader" contains invalid permission range at index 0`},
		{
			name: "duplicate interval",
			permissions: []*authpb.Permission{
				{PermType: authpb.READ, Key: []byte("a"), RangeEnd: []byte("m")},
				{PermType: authpb.WRITE, Key: []byte("a"), RangeEnd: []byte("m")},
			},
			want: `auth role "reader" repeats permission range at index 1`,
		},
		{
			name: "unsorted keys",
			permissions: []*authpb.Permission{
				{PermType: authpb.READ, Key: []byte("m")},
				{PermType: authpb.READ, Key: []byte("a")},
			},
			want: `auth role "reader" permissions are not key-sorted at index 1`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.db")
			err := WriteBackend(path, State{Auth: Auth{Roles: []*authpb.Role{{
				Name: []byte("reader"), KeyPermission: test.permissions,
			}}}})
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestOfficialAuthAndAlarmStoresRecoverGeneratedBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	const authRevision = uint64(13)
	password, err := bcrypt.GenerateFromPassword([]byte("alice-secret"), bcrypt.MinCost)
	require.NoError(t, err)
	rootPassword, err := bcrypt.GenerateFromPassword([]byte("root-secret"), bcrypt.MinCost)
	require.NoError(t, err)
	reader := &authpb.Role{Name: []byte("reader"), KeyPermission: []*authpb.Permission{{
		Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"), PermType: authpb.READ,
	}}}
	unknown := &authpb.Role{Name: []byte("unknown"), KeyPermission: []*authpb.Permission{{
		Key: []byte("/unknown/"), RangeEnd: []byte("/unknown0"), PermType: authpb.Permission_Type(99),
	}}}
	require.NoError(t, WriteBackend(path, State{
		Revision: 41,
		Auth: Auth{Enabled: true, Revision: authRevision,
			Users: []*authpb.User{
				{Name: []byte("root"), Password: rootPassword, Roles: []string{"root"}},
				{Name: []byte("alice"), Password: password, Roles: []string{"reader"}},
				{Name: []byte("bob"), Password: password, Roles: []string{"unknown"}},
			},
			Roles: []*authpb.Role{
				{Name: []byte("root"), KeyPermission: []*authpb.Permission{{Key: []byte{0}, RangeEnd: []byte{0}, PermType: authpb.READWRITE}}},
				reader,
				unknown,
			},
		},
		Alarms: []*etcdserverpb.AlarmMember{
			{MemberID: 23, Alarm: etcdserverpb.AlarmType_NOSPACE},
			{MemberID: 23, Alarm: etcdserverpb.AlarmType_CORRUPT},
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
	require.EqualValues(t, authRevision, authStore.Revision())
	revision, err := authStore.CheckPassword("alice", "alice-secret")
	require.NoError(t, err)
	require.EqualValues(t, authRevision, revision)
	alice := &etcdAuth.AuthInfo{Username: "alice", Revision: authRevision}
	require.NoError(t, authStore.IsRangePermitted(alice, []byte("/allowed/key"), nil))
	require.Error(t, authStore.IsRangePermitted(alice, []byte("/denied/key"), nil))
	bob := &etcdAuth.AuthInfo{Username: "bob", Revision: authRevision}
	require.Error(t, authStore.IsRangePermitted(bob, []byte("/unknown/key"), nil))
	require.Error(t, authStore.IsPutPermitted(bob, []byte("/unknown/key")))

	alarmStore, err := v3alarm.NewAlarmStore(lg, schema.NewAlarmBackend(lg, be))
	require.NoError(t, err)
	gotAlarms := make([]string, 0, 3)
	for _, alarm := range alarmStore.Get(etcdserverpb.AlarmType_NONE) {
		gotAlarms = append(gotAlarms, fmt.Sprintf("%d/%d", alarm.MemberID, alarm.Alarm))
	}
	require.ElementsMatch(t, []string{"23/1", "23/2", "29/127"}, gotAlarms)
}
