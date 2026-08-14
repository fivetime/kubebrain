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
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueMetaRoundTrip(t *testing.T) {
	raw := []byte("k8s\x00some-protobuf-object-bytes")
	meta := EtcdMetadata{CreateRevision: 12345, Version: 7}
	enc := encodeValueWithMeta(raw, meta)

	require.True(t, hasValueMetaV3(enc))
	require.True(t, InlineValueLeaseKnown(enc))
	got, rawOut, ok := decodeValueWithMeta(enc)
	require.True(t, ok)
	require.Equal(t, meta, got)
	require.True(t, bytes.Equal(raw, rawOut))
}

// TestValueMetaV2Lease pins review #9: a leased version round-trips through the v2
// envelope carrying its lease, while an unleased version uses the same-size v3
// known-zero envelope. A v1 envelope written before the lease field remains
// readable, but is explicitly lease-unknown for snapshot safety.
func TestValueMetaV2Lease(t *testing.T) {
	raw := []byte("k8s\x00leased-object")

	// Leased -> v2, lease preserved (including a large/negative-bit lease ID).
	for _, lease := range []int64{1, 0x0123456789abcdef, -1} {
		meta := EtcdMetadata{CreateRevision: 42, Version: 3, Lease: lease}
		enc := encodeValueWithMeta(raw, meta)
		require.True(t, hasValueMetaV2(enc))
		require.False(t, hasValueMeta(enc), "v2 magic must not match the v1 check")
		require.True(t, InlineValueLeaseKnown(enc))
		require.Equal(t, valueMetaHeaderLenV2+len(raw), len(enc))
		got, rawOut, ok := decodeValueWithMeta(enc)
		require.True(t, ok)
		require.Equal(t, meta, got)
		require.True(t, bytes.Equal(raw, rawOut))
	}

	// Unleased -> v3, no lease field and no size regression, but known zero.
	unleased := encodeValueWithMeta(raw, EtcdMetadata{CreateRevision: 42, Version: 3})
	require.True(t, hasValueMetaV3(unleased))
	require.False(t, hasValueMeta(unleased))
	require.False(t, hasValueMetaV2(unleased))
	require.True(t, InlineValueLeaseKnown(unleased))
	require.Equal(t, valueMetaHeaderLen+len(raw), len(unleased))
	got, _, ok := decodeValueWithMeta(unleased)
	require.True(t, ok)
	require.Equal(t, int64(0), got.Lease)

	legacyV1 := append([]byte(nil), unleased...)
	copy(legacyV1, valueMetaMagic)
	got, rawOut, ok := decodeValueWithMeta(legacyV1)
	require.True(t, ok)
	require.Equal(t, EtcdMetadata{CreateRevision: 42, Version: 3}, got)
	require.Equal(t, raw, rawOut)
	require.False(t, InlineValueLeaseKnown(legacyV1))
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

func TestDecodeInlineValueCheckedRejectsMalformedReservedEnvelope(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "short v1", raw: []byte{0, 'k', 'b', 1}, want: "inline value metadata v1 length 4 is shorter than 20"},
		{name: "short v2", raw: []byte{0, 'k', 'b', 2}, want: "inline value metadata v2 length 4 is shorter than 28"},
		{name: "short v3", raw: []byte{0, 'k', 'b', 3}, want: "inline value metadata v3 length 4 is shorter than 20"},
		{name: "unknown version", raw: []byte{0, 'k', 'b', 99}, want: "unsupported inline value metadata version 99"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := DecodeInlineValueChecked(test.raw)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
			require.ErrorContains(t, err, test.want)
		})
	}

	for _, legacy := range [][]byte{nil, {0}, {0, 'k'}, {0, 'k', 'b'}, {0, 'x', 'b'}, []byte("ordinary value")} {
		_, raw, inlined, err := DecodeInlineValueChecked(legacy)
		require.NoError(t, err)
		require.False(t, inlined)
		require.Equal(t, legacy, raw)
	}
}

func TestDecodeInlineValueCheckedRejectsInvalidMetadataFields(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "zero create revision", raw: encodeValueWithMeta(nil, EtcdMetadata{Version: 1}), want: "create revision is zero"},
		{name: "zero version", raw: encodeValueWithMeta(nil, EtcdMetadata{CreateRevision: 1}), want: "version is zero"},
		{name: "create revision overflow", raw: encodeValueWithMeta(nil, EtcdMetadata{CreateRevision: uint64(math.MaxInt64) + 1, Version: 1}), want: "create revision 9223372036854775808 exceeds MaxInt64"},
		{name: "version overflow", raw: encodeValueWithMeta(nil, EtcdMetadata{CreateRevision: 1, Version: uint64(math.MaxInt64) + 1}), want: "version 9223372036854775808 exceeds MaxInt64"},
	}
	v2ZeroLease := make([]byte, valueMetaHeaderLenV2)
	copy(v2ZeroLease, valueMetaMagicV2)
	binary.BigEndian.PutUint64(v2ZeroLease[4:], 1)
	binary.BigEndian.PutUint64(v2ZeroLease[12:], 1)
	tests = append(tests, struct {
		name string
		raw  []byte
		want string
	}{name: "v2 zero lease", raw: v2ZeroLease, want: "v2 lease is zero"})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := DecodeInlineValueChecked(test.raw)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestValidateEtcdMetadataAtRevisionRejectsImpossibleLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		meta        EtcdMetadata
		modRevision uint64
		want        string
	}{
		{name: "zero mod revision", meta: EtcdMetadata{CreateRevision: 1, Version: 1}, want: "mod revision is zero"},
		{name: "future create revision", meta: EtcdMetadata{CreateRevision: 6, Version: 1}, modRevision: 5, want: "create revision 6 exceeds mod revision 5"},
		{name: "version one after create", meta: EtcdMetadata{CreateRevision: 3, Version: 1}, modRevision: 5, want: "version 1 create revision 3 differs from mod revision 5"},
		{name: "impossible version", meta: EtcdMetadata{CreateRevision: 3, Version: 4}, modRevision: 5, want: "version 4 exceeds maximum 3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateEtcdMetadataAtRevision(test.meta, test.modRevision, "test metadata")
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
			require.ErrorContains(t, err, test.want)
		})
	}
	require.NoError(t, ValidateEtcdMetadataAtRevision(
		EtcdMetadata{CreateRevision: 3, Version: 3}, 5, "test metadata"))
}
