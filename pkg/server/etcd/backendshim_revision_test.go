// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

type rangeRevisionProbeBackend struct {
	backend.Backend
	getRevision        uint64
	listRevision       uint64
	countRevision      uint64
	backendCountCalled bool
}

type currentCountRevisionRaceBackend struct {
	backend.Backend
	current         uint64
	countedRevision uint64
}

type historicalCountScanProbeBackend struct {
	backend.Backend
	scanKey      []byte
	scanEnd      []byte
	scanRevision uint64
	listCalled   bool
}

func (b *currentCountRevisionRaceBackend) CountAtRevision(_ context.Context, _, _ []byte, revision uint64) (int64, uint64, bool) {
	if revision == 0 {
		b.countedRevision = b.current
		// Simulate a commit becoming visible after the index selected its snapshot
		// but before the shim asks GetCurrentRevision for the response header.
		b.current++
	}
	return 3, b.countedRevision, true
}

func (b *currentCountRevisionRaceBackend) GetCurrentRevision() uint64 { return b.current }

func (b *historicalCountScanProbeBackend) CountAtRevision(context.Context, []byte, []byte, uint64) (int64, uint64, bool) {
	return 0, 0, false
}

func (b *historicalCountScanProbeBackend) CountAtRevisionScan(_ context.Context, key, end []byte, revision uint64) (int64, uint64, error) {
	b.scanKey = append([]byte(nil), key...)
	b.scanEnd = append([]byte(nil), end...)
	b.scanRevision = revision
	return 4, 11, nil
}

func (b *historicalCountScanProbeBackend) List(context.Context, *proto.RangeRequest) (*proto.RangeResponse, error) {
	b.listCalled = true
	return nil, fmt.Errorf("historical CountOnly unexpectedly materialized List")
}

type malformedInlineValueBackend struct {
	backend.Backend
	value       []byte
	metadataErr error
}

func (b *malformedInlineValueBackend) Get(context.Context, *proto.GetRequest) (*proto.GetResponse, error) {
	return &proto.GetResponse{
		Header: &proto.ResponseHeader{Revision: 11},
		Kv:     &proto.KeyValue{Key: []byte("key"), Value: b.value, Revision: 11},
	}, nil
}

func (b *malformedInlineValueBackend) GetEtcdMetadata(context.Context, []byte, uint64) (backend.EtcdMetadata, error) {
	return backend.EtcdMetadata{}, b.metadataErr
}

func (b *rangeRevisionProbeBackend) Get(_ context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	b.getRevision = req.Revision
	return &proto.GetResponse{Header: &proto.ResponseHeader{Revision: 11}}, nil
}

func (b *rangeRevisionProbeBackend) List(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	b.listRevision = req.Revision
	return &proto.RangeResponse{Header: &proto.ResponseHeader{Revision: 11}}, nil
}

func (b *rangeRevisionProbeBackend) CountAtRevision(_ context.Context, _, _ []byte, revision uint64) (int64, uint64, bool) {
	b.countRevision = revision
	return 0, b.GetCurrentRevision(), true
}

func (b *rangeRevisionProbeBackend) Count(ctx context.Context, _ *proto.CountRequest) (*proto.CountResponse, error) {
	b.backendCountCalled = true
	revision := b.GetCurrentRevision()
	if checkpoint, ok := backend.SerializableCheckpointFromContext(ctx); ok {
		revision = checkpoint.Revision
	}
	return &proto.CountResponse{Header: &proto.ResponseHeader{Revision: revision}, Count: 2}, nil
}

func (b *rangeRevisionProbeBackend) GetCurrentRevision() uint64 { return 11 }

func TestBackendShimNormalizesSignedRangeRevision(t *testing.T) {
	metricCli := &recordingMetrics{}
	for _, tc := range []struct {
		name string
		wire int64
		want uint64
	}{
		{name: "minus one", wire: -1, want: 0},
		{name: "minimum int64", wire: math.MinInt64, want: 0},
		{name: "positive", wire: 7, want: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &rangeRevisionProbeBackend{}
			shim := NewBackendShim(probe, metricCli)
			req := &etcdserverpb.RangeRequest{
				Key: []byte("/signed-revision/"), RangeEnd: []byte("/signed-revision0"), Revision: tc.wire,
			}

			_, err := shim.Get(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tc.want, probe.getRevision, "point Range revision")
			_, err = shim.List(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tc.want, probe.listRevision, "range List revision")
			_, err = shim.Count(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tc.want, probe.countRevision, "CountOnly revision")
		})
	}
}

func TestBackendShimCountUsesCheckpointHeader(t *testing.T) {
	probe := &rangeRevisionProbeBackend{}
	shim := NewBackendShim(probe, &recordingMetrics{})
	checkpoint := backend.SerializableCheckpoint{Revision: 29, Timestamp: 101}
	ctx := backend.WithSerializableCheckpoint(context.Background(), checkpoint)

	response, err := shim.Count(ctx, &etcdserverpb.RangeRequest{
		Key: []byte("/checkpoint/"), RangeEnd: []byte("/checkpoint0"),
		Revision: math.MinInt64, Serializable: true, CountOnly: true, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(checkpoint.Revision), response.Header.Revision)
	require.Equal(t, int64(2), response.Count)
	require.Empty(t, response.Kvs)
	require.False(t, response.More)
	require.True(t, probe.backendCountCalled)
	require.Zero(t, probe.countRevision, "checkpoint count must bypass the shim's local index response")
}

func TestBackendShimHistoricalCountUsesRequestedRevisionInsideCheckpoint(t *testing.T) {
	probe := &rangeRevisionProbeBackend{}
	shim := NewBackendShim(probe, &recordingMetrics{})
	checkpoint := backend.SerializableCheckpoint{Revision: 29, Timestamp: 101}
	ctx := backend.WithSerializableCheckpoint(context.Background(), checkpoint)

	response, err := shim.Count(ctx, &etcdserverpb.RangeRequest{
		Key: []byte("/checkpoint/history/"), RangeEnd: []byte("/checkpoint/history0"),
		Revision: 7, Serializable: true, CountOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), probe.countRevision)
	require.False(t, probe.backendCountCalled, "an explicit historical revision must not be replaced by the checkpoint revision")
	require.Equal(t, int64(probe.GetCurrentRevision()), response.Header.Revision)
}

func TestBackendShimHistoricalCountIndexMissUsesUnfilteredScan(t *testing.T) {
	probe := &historicalCountScanProbeBackend{}
	shim := NewBackendShim(probe, &recordingMetrics{})
	request := &etcdserverpb.RangeRequest{
		Key: []byte("/historical-count-scan/"), RangeEnd: []byte("/historical-count-scan0"),
		Revision: 7, CountOnly: true, Limit: 1,
		SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
		MinModRevision: 100, MaxModRevision: 10, MinCreateRevision: 200, MaxCreateRevision: 20,
	}

	response, err := shim.Count(context.Background(), request)
	require.NoError(t, err)
	require.False(t, probe.listCalled)
	require.Equal(t, request.Key, probe.scanKey)
	require.Equal(t, request.RangeEnd, probe.scanEnd)
	require.Equal(t, uint64(request.Revision), probe.scanRevision)
	require.Equal(t, int64(11), response.GetHeader().GetRevision())
	require.Equal(t, int64(4), response.Count)
	require.Empty(t, response.Kvs)
	require.False(t, response.More)
}

func TestBackendShimCurrentCountPreservesResolvedRevision(t *testing.T) {
	probe := &currentCountRevisionRaceBackend{current: 11}
	shim := NewBackendShim(probe, &recordingMetrics{})

	response, err := shim.Count(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/current-count/"), RangeEnd: []byte("/current-count0"), CountOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), response.Count)
	require.Equal(t, int64(probe.countedRevision), response.GetHeader().GetRevision(),
		"current count and response header must describe the same resolved snapshot")
	require.Equal(t, uint64(12), probe.GetCurrentRevision(), "fixture must expose the simulated concurrent commit")
}

func TestBackendShimGetRejectsMalformedInlineValue(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)
	shim.backend = &malformedInlineValueBackend{Backend: shim.backend, value: []byte{0, 'k', 'b', 3}}
	response, err := shim.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.ErrorIs(t, err, backend.ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "inline value metadata v3 length 4 is shorter than 20")
	require.Nil(t, response)
}

func TestBackendShimGetPropagatesLegacyMetadataError(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)
	want := fmt.Errorf("%w: invalid etcd metadata length 1", backend.ErrInvalidMVCCMetadata)
	shim.backend = &malformedInlineValueBackend{Backend: shim.backend, value: []byte("legacy"), metadataErr: want}

	response, err := shim.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.ErrorIs(t, err, backend.ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "invalid etcd metadata length 1")
	require.Nil(t, response)
}
