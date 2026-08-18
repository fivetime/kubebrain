// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package backend

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestRangeStreamFailureMetricInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initRangeStreamFailureMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "backend.list.by.stream.failed", value: 0},
	}, recorder.records)
}

func TestRangeStreamFailureMetricExcludesCallerCancellation(t *testing.T) {
	t.Run("storage failure", func(t *testing.T) {
		recorder := newRecordCounters()
		store := &armedFailPartitionsKV{KvStorage: memkv.NewKvStorage()}
		defer func() { require.NoError(t, store.Close()) }()
		b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity()}, recorder).(*backend)
		defer func() { require.NoError(t, b.Close()) }()
		atomic.StoreInt32(&store.armed, 1)

		for range b.scanner.RangeStream(context.Background(), []byte("a"), []byte("z"), 1, false) {
		}
		require.Equal(t, float64(1), recorder.get("backend.list.by.stream.failed"))
	})

	t.Run("caller cancellation", func(t *testing.T) {
		recorder := newRecordCounters()
		store := &cancelFirstPartitionsKV{KvStorage: memkv.NewKvStorage(), entered: make(chan struct{})}
		defer func() { require.NoError(t, store.Close()) }()
		b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity()}, recorder).(*backend)
		defer func() { require.NoError(t, b.Close()) }()
		ctx, cancel := context.WithCancel(context.Background())
		stream := b.scanner.RangeStream(ctx, []byte("a"), []byte("z"), 1, false)
		<-store.entered
		cancel()
		for range stream {
		}

		require.Zero(t, recorder.get("backend.list.by.stream.failed"))
		require.Equal(t, float64(1), recorder.get("backend.list.by.stream.canceled"))
	})
}
