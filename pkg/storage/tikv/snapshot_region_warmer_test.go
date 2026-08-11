// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package tikv

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingSnapshotRegionReader struct {
	mu   sync.Mutex
	keys [][]byte
}

func (r *recordingSnapshotRegionReader) Get(_ context.Context, key []byte) ([]byte, error) {
	r.mu.Lock()
	r.keys = append(r.keys, append([]byte(nil), key...))
	r.mu.Unlock()
	return nil, nil
}

func TestWarmSnapshotRegionReadersTouchesEveryIndependentClientCache(t *testing.T) {
	readers := []*recordingSnapshotRegionReader{{}, {}, {}}
	starts := [][]byte{{0x10}, {0x20}, {0x30}}
	require.NoError(t, warmSnapshotRegionReaders(context.Background(), starts, len(readers), func(index int) snapshotRegionReader {
		return readers[index]
	}))
	for _, reader := range readers {
		require.Equal(t, starts, reader.keys)
	}
}
