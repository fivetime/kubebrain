// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package coder

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRevisionWatermarkRequiresExactWidth(t *testing.T) {
	valid := make([]byte, RevisionValueLength)
	binary.BigEndian.PutUint64(valid, 42)
	revision, err := ParseRevisionWatermark(valid)
	require.NoError(t, err)
	require.Equal(t, uint64(42), revision)
	binary.BigEndian.PutUint64(valid, math.MaxInt64)
	revision, err = ParseRevisionWatermark(valid)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxInt64), revision)

	overflow := make([]byte, RevisionValueLength)
	binary.BigEndian.PutUint64(overflow, uint64(math.MaxInt64)+1)
	for _, raw := range [][]byte{{1}, append(append([]byte(nil), valid...), 0), overflow} {
		_, err = ParseRevisionWatermark(raw)
		require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	}
}
