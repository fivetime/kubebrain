// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package backend

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestSnapshotHistoryStreamPreservesLegacyMetadataTombstoneAndCurrentVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	// Disable inline metadata to exercise the pre-Approach-A compatibility path:
	// the stream must join each raw value with its historical etcdmeta record and
	// must never export those reserved metadata objects as user keys.
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/snapshot-history/legacy")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("v2"), Revision: created.Header.Revision}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: key})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)
	recreated, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v3")})
	require.NoError(t, err)
	waitCommitted(t, b, recreated.Header.Revision)

	stream, err := b.SnapshotHistoryStream(ctx, recreated.Header.Revision)
	require.NoError(t, err)
	var records []SnapshotHistoryRecord
	terminal := false
	for chunk := range stream {
		require.NoError(t, chunk.Err)
		require.Equal(t, recreated.Header.Revision, chunk.Revision)
		if chunk.Done {
			terminal = true
		}
		records = append(records, chunk.Records...)
	}
	require.True(t, terminal)
	var matching []SnapshotHistoryRecord
	for _, record := range records {
		if string(record.Key) == string(key) {
			matching = append(matching, record)
		}
		require.NotContains(t, string(record.Key), string(etcdMetadataPrefix))
	}
	require.Len(t, matching, 4)
	require.Equal(t, []byte("v1"), matching[0].Value)
	require.Equal(t, created.Header.Revision, matching[0].CreateRevision)
	require.EqualValues(t, 1, matching[0].Version)
	require.Equal(t, []byte("v2"), matching[1].Value)
	require.Equal(t, created.Header.Revision, matching[1].CreateRevision)
	require.EqualValues(t, 2, matching[1].Version)
	require.True(t, matching[2].Tombstone)
	require.Empty(t, matching[2].Value)
	require.Equal(t, []byte("v3"), matching[3].Value)
	require.Equal(t, recreated.Header.Revision, matching[3].CreateRevision)
	require.EqualValues(t, 1, matching[3].Version)
	require.False(t, matching[0].Current)
	require.False(t, matching[1].Current)
	require.False(t, matching[2].Current)
	require.True(t, matching[3].Current)
}
