// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package backend

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rangeStreamSpillMetricRecorder struct {
	compactMetricRecorder
	registeredHistograms []compactMetricRecord
}

func (r *rangeStreamSpillMetricRecorder) RegisterHistogram(name string, tags ...metrics.T) error {
	r.registeredHistograms = append(r.registeredHistograms, compactMetricRecord{
		kind: "histogram_registration", name: name, tags: tags,
	})
	return nil
}

func TestRangeStreamSpillMetricsInitializeAuthoritativeZero(t *testing.T) {
	recorder := &rangeStreamSpillMetricRecorder{}
	initRangeStreamSpillMetrics(recorder)

	want := []compactMetricRecord{{kind: "gauge", name: "backend.range_stream.spill_active", value: int64(0)}}
	for _, path := range []string{"decoded", "latest_metadata"} {
		for _, outcome := range []string{"completed", "quota_exhausted", "canceled", "failed"} {
			want = append(want, compactMetricRecord{
				kind: "counter", name: "backend.range_stream.spill_outcome", value: int64(0),
				tags: []metrics.T{metrics.Tag("path", path), metrics.Tag("outcome", outcome)},
			})
		}
	}
	require.Equal(t, want, recorder.records)
	require.Equal(t, []compactMetricRecord{
		{kind: "histogram_registration", name: "backend.range_stream.spill_wait_seconds", tags: []metrics.T{metrics.Tag("path", "decoded")}},
		{kind: "histogram_registration", name: "backend.range_stream.spill_wait_seconds", tags: []metrics.T{metrics.Tag("path", "latest_metadata")}},
	}, recorder.registeredHistograms)
}

func TestRangeStreamSpillAttemptMetricsUseBoundedOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		finish  func(*rangeStreamSpillAttempt)
		outcome string
	}{
		{name: "completed", path: rangeStreamSpillPathLatestMetadata, finish: func(a *rangeStreamSpillAttempt) { a.complete() }, outcome: rangeStreamSpillOutcomeCompleted},
		{name: "quota", path: rangeStreamSpillPathDecoded, finish: func(a *rangeStreamSpillAttempt) {
			a.recordError(fmt.Errorf("write run: %w", status.Error(codes.ResourceExhausted, "quota")))
		}, outcome: rangeStreamSpillOutcomeQuotaExhausted},
		{name: "canceled", path: rangeStreamSpillPathDecoded, finish: func(a *rangeStreamSpillAttempt) {
			a.recordError(fmt.Errorf("scan: %w", context.Canceled))
		}, outcome: rangeStreamSpillOutcomeCanceled},
		{name: "failed", path: rangeStreamSpillPathLatestMetadata, finish: func(a *rangeStreamSpillAttempt) {
			a.recordError(errors.New("disk failure"))
		}, outcome: rangeStreamSpillOutcomeFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &compactMetricRecorder{}
			b := &backend{decodedRangeSpillSem: make(chan struct{}, 1), metricCli: recorder}
			attempt, admitted := b.acquireRangeStreamSpill(context.Background(), test.path)
			require.True(t, admitted)
			require.Len(t, b.decodedRangeSpillSem, 1)
			test.finish(attempt)
			attempt.finish(context.Background())
			require.Empty(t, b.decodedRangeSpillSem)

			var positive []compactMetricRecord
			for _, record := range recorder.records {
				if record.kind == "counter" && record.name == "backend.range_stream.spill_outcome" {
					positive = append(positive, record)
				}
			}
			require.Equal(t, []compactMetricRecord{{
				kind: "counter", name: "backend.range_stream.spill_outcome", value: int64(1),
				tags: []metrics.T{metrics.Tag("path", test.path), metrics.Tag("outcome", test.outcome)},
			}}, positive)
		})
	}
}

func TestRangeStreamSpillCleanupFailureWinsOverCallerCancellation(t *testing.T) {
	recorder := &compactMetricRecorder{}
	b := &backend{decodedRangeSpillSem: make(chan struct{}, 1), metricCli: recorder}
	attempt, admitted := b.acquireRangeStreamSpill(context.Background(), rangeStreamSpillPathLatestMetadata)
	require.True(t, admitted)

	attempt.recordSorterCleanup(&externalKeySorter{dir: "invalid\x00workspace"}, "remove spill workspace")
	require.True(t, attempt.errorRecorded)
	require.Equal(t, rangeStreamSpillOutcomeFailed, attempt.outcome)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempt.finish(ctx)

	var positive []compactMetricRecord
	for _, record := range recorder.records {
		if record.kind == "counter" && record.name == "backend.range_stream.spill_outcome" {
			positive = append(positive, record)
		}
	}
	require.Equal(t, []compactMetricRecord{{
		kind: "counter", name: "backend.range_stream.spill_outcome", value: int64(1),
		tags: []metrics.T{
			metrics.Tag("path", rangeStreamSpillPathLatestMetadata),
			metrics.Tag("outcome", rangeStreamSpillOutcomeFailed),
		},
	}}, positive, "a failed workspace removal must not be hidden as routine cancellation")
}

func TestRangeStreamSpillLimitCompletionHasAccurateOutcome(t *testing.T) {
	tests := []struct {
		name    string
		finish  func(*rangeStreamSpillAttempt)
		outcome string
	}{
		{
			name: "pure internal cancellation is successful completion",
			finish: func(attempt *rangeStreamSpillAttempt) {
				attempt.recordError(context.Canceled)
			},
			outcome: rangeStreamSpillOutcomeCompleted,
		},
		{
			name: "cleanup failure still wins",
			finish: func(attempt *rangeStreamSpillAttempt) {
				attempt.recordError(context.Canceled)
				attempt.recordSorterCleanup(&externalKeySorter{dir: "invalid\x00workspace"}, "remove spill workspace")
			},
			outcome: rangeStreamSpillOutcomeFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &compactMetricRecorder{}
			b := &backend{decodedRangeSpillSem: make(chan struct{}, 1), metricCli: recorder}
			ctx, stop, limitSatisfied := WithRangeStreamLimitCancellation(context.Background())
			defer stop()
			attempt, admitted := b.acquireRangeStreamSpill(ctx, rangeStreamSpillPathLatestMetadata)
			require.True(t, admitted)
			test.finish(attempt)
			limitSatisfied()
			attempt.finish(ctx)

			var positive []compactMetricRecord
			for _, record := range recorder.records {
				if record.kind == "counter" && record.name == "backend.range_stream.spill_outcome" {
					positive = append(positive, record)
				}
			}
			require.Equal(t, []compactMetricRecord{{
				kind: "counter", name: "backend.range_stream.spill_outcome", value: int64(1),
				tags: []metrics.T{
					metrics.Tag("path", rangeStreamSpillPathLatestMetadata),
					metrics.Tag("outcome", test.outcome),
				},
			}}, positive)
		})
	}
}

func TestRangeStreamSpillCanceledContextNeverAcquiresSlot(t *testing.T) {
	recorder := &compactMetricRecorder{}
	b := &backend{decodedRangeSpillSem: make(chan struct{}, 1), metricCli: recorder}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attempt, admitted := b.acquireRangeStreamSpill(ctx, rangeStreamSpillPathLatestMetadata)
	require.False(t, admitted)
	require.Nil(t, attempt)
	require.Empty(t, b.decodedRangeSpillSem)

	var outcomes []compactMetricRecord
	var activeOnes int
	var waits int
	for _, record := range recorder.records {
		switch {
		case record.kind == "counter" && record.name == "backend.range_stream.spill_outcome":
			outcomes = append(outcomes, record)
		case record.kind == "gauge" && record.name == "backend.range_stream.spill_active" && record.value == int64(1):
			activeOnes++
		case record.kind == "histogram" && record.name == "backend.range_stream.spill_wait_seconds":
			waits++
		}
	}
	require.Equal(t, []compactMetricRecord{{
		kind: "counter", name: "backend.range_stream.spill_outcome", value: int64(1),
		tags: []metrics.T{metrics.Tag("path", rangeStreamSpillPathLatestMetadata), metrics.Tag("outcome", rangeStreamSpillOutcomeCanceled)},
	}}, outcomes)
	require.Zero(t, activeOnes)
	require.Equal(t, 1, waits)
}

func TestRangeStreamFailureMetricInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initRangeStreamFailureMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "backend.list.by.stream.failed", value: 0},
		{kind: "counter", name: "backend.list.by.stream.canceled", value: 0},
		{kind: "counter", name: "backend.list.by.stream.limit_satisfied", value: 0},
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

	t.Run("successful client limit", func(t *testing.T) {
		recorder := newRecordCounters()
		store := &cancelFirstPartitionsKV{KvStorage: memkv.NewKvStorage(), entered: make(chan struct{})}
		defer func() { require.NoError(t, store.Close()) }()
		b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity()}, recorder).(*backend)
		defer func() { require.NoError(t, b.Close()) }()
		ctx, stop, limitSatisfied := WithRangeStreamLimitCancellation(context.Background())
		defer stop()
		stream := b.scanner.RangeStream(ctx, []byte("a"), []byte("z"), 1, false)
		<-store.entered
		limitSatisfied()
		for range stream {
		}

		require.Zero(t, recorder.get("backend.list.by.stream.failed"))
		require.Zero(t, recorder.get("backend.list.by.stream.canceled"))
		require.Equal(t, float64(1), recorder.get("backend.list.by.stream.limit_satisfied"))
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
