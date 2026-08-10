// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package backend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestCompactWatermarkCorruptionFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: nil},
		{name: "short", raw: []byte{1}},
		{name: "trailing bytes", raw: make([]byte, coder.RevisionValueLength+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := mock.NewMinimalMetrics(ctrl)
			kv := imemkv.NewKvStorage()
			defer func() { require.NoError(t, kv.Close()) }()
			b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
			ctx := context.Background()

			batch := kv.BeginBatchWrite()
			batch.Put(getCompactKey(prefix), test.raw, 0)
			require.NoError(t, batch.Commit(ctx))

			_, err := b.GetCompactRevisionFresh(ctx)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)

			_, err = b.HashKV(ctx, 0)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)

			_, err = b.setCompactRecord(ctx, 2)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
		})
	}
}
