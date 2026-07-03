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

package backend

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueMetaRoundTrip(t *testing.T) {
	raw := []byte("k8s\x00some-protobuf-object-bytes")
	meta := EtcdMetadata{CreateRevision: 12345, Version: 7}
	enc := encodeValueWithMeta(raw, meta)

	require.True(t, hasValueMeta(enc))
	got, rawOut, ok := decodeValueWithMeta(enc)
	require.True(t, ok)
	require.Equal(t, meta, got)
	require.True(t, bytes.Equal(raw, rawOut))
}

// TestValueMetaV2Lease pins review #9: a leased version round-trips through the v2
// envelope carrying its lease, while an unleased version stays v1 (no size
// regression) and decodes with lease 0. A v1 envelope written before the lease
// field is still decoded correctly (lease 0), so no migration is needed.
func TestValueMetaV2Lease(t *testing.T) {
	raw := []byte("k8s\x00leased-object")

	// Leased -> v2, lease preserved (including a large/negative-bit lease ID).
	for _, lease := range []int64{1, 0x0123456789abcdef, -1} {
		meta := EtcdMetadata{CreateRevision: 42, Version: 3, Lease: lease}
		enc := encodeValueWithMeta(raw, meta)
		require.True(t, hasValueMetaV2(enc))
		require.False(t, hasValueMeta(enc), "v2 magic must not match the v1 check")
		require.Equal(t, valueMetaHeaderLenV2+len(raw), len(enc))
		got, rawOut, ok := decodeValueWithMeta(enc)
		require.True(t, ok)
		require.Equal(t, meta, got)
		require.True(t, bytes.Equal(raw, rawOut))
	}

	// Unleased -> v1, no lease field, no size regression.
	unleased := encodeValueWithMeta(raw, EtcdMetadata{CreateRevision: 42, Version: 3})
	require.True(t, hasValueMeta(unleased))
	require.False(t, hasValueMetaV2(unleased))
	require.Equal(t, valueMetaHeaderLen+len(raw), len(unleased))
	got, _, ok := decodeValueWithMeta(unleased)
	require.True(t, ok)
	require.Equal(t, int64(0), got.Lease)
}

func TestValueMetaEmptyValue(t *testing.T) {
	enc := encodeValueWithMeta(nil, EtcdMetadata{CreateRevision: 1, Version: 1})
	meta, rawOut, ok := decodeValueWithMeta(enc)
	require.True(t, ok)
	require.Equal(t, EtcdMetadata{CreateRevision: 1, Version: 1}, meta)
	require.Empty(t, rawOut)
}

// TestValueMetaLegacyValuesNotMistaken guards the collision-freedom claim: real
// stored values (k8s protobuf/JSON) and the tombstone marker must never be
// mistaken for an inline-metadata envelope.
func TestValueMetaLegacyValuesNotMistaken(t *testing.T) {
	legacy := [][]byte{
		[]byte("k8s\x00\x1a\x0bexample"), // protobuf-prefixed object
		[]byte(`{"kind":"Pod"}`),         // JSON object
		tombStoneBytes,                   // "tombstone"
		{},                               // empty
		{0x00},                           // shorter than header, leading 0x00
		{0x00, 0x6b, 0x62},               // 3-byte prefix, still shorter than header
		[]byte("arbitrary value bytes"),
	}
	for _, v := range legacy {
		require.False(t, hasValueMeta(v), "legacy value %q must not look enveloped", v)
		meta, rawOut, ok := decodeValueWithMeta(v)
		require.False(t, ok)
		require.Equal(t, EtcdMetadata{}, meta)
		require.True(t, bytes.Equal(v, rawOut))
	}
}
