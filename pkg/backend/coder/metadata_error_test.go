// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coder

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRevisionPreservesDiagnosticAndClassifiesCorruption(t *testing.T) {
	_, _, err := ParseRevision([]byte{1})
	require.EqualError(t, err, ErrInvalidRevFormat.Error())
	require.ErrorIs(t, err, ErrInvalidRevFormat)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
}

func TestParseRevisionRejectsWireOverflow(t *testing.T) {
	raw := make([]byte, RevisionValueLength)
	binary.BigEndian.PutUint64(raw, uint64(math.MaxInt64)+1)
	_, _, err := ParseRevision(raw)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "exceeds MaxInt64")

	_, _, err = ParseRevision(append(raw, 0))
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
}

func TestMarkInvalidMVCCMetadataPreservesCause(t *testing.T) {
	cause := errors.New("decode physical object key")
	err := MarkInvalidMVCCMetadata(cause)
	require.EqualError(t, err, cause.Error())
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Equal(t, err, MarkInvalidMVCCMetadata(err), "classification must be idempotent")
}
