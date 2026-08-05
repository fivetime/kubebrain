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

import "encoding/binary"

// Approach A-core: inline a key version's etcd metadata (create_revision,
// version, and — for leased keys — the lease ID) into the stored object value,
// so LIST/Get/watch need no separate metadata lookup and the
// \x00kubebrain/etcdmeta/ keyspace stops growing.
//
// A value is wrapped in one of two envelopes, distinguished by the low magic
// byte (the version tag):
//
//	v1 (\x00kb\x01): [ magic (4B) ][ createRevision (8B) ][ version (8B) ][ raw... ]
//	v2 (\x00kb\x02): [ magic (4B) ][ createRevision (8B) ][ version (8B) ][ lease (8B) ][ raw... ]
//
// v2 is written only when a version carries a non-zero lease (review #9: the
// lease must be recorded per MVCC version so historical reads, prevKv, and
// delete events report the lease that key held at that revision — not merely the
// key's current binding). v3 has the same 20-byte shape as v1 and explicitly
// means lease=0. The distinction is needed because v1 predates per-version
// leases: an old v1 row may have been leased even though it has no lease field.
// A reader-first rollout teaches every replica v3 before a following release
// starts writing it, so snapshots can eventually distinguish a known zero from
// legacy ambiguity without increasing the common Kubernetes object's size.
//
// The magic starts with 0x00. Kubernetes-stored values always begin with the
// protobuf prefix "k8s\x00" (0x6b) or JSON '{' (0x7b), and KubeBrain's tombstone
// marker begins with 't' (0x74) — none start with 0x00 — so an enveloped value
// is unambiguously distinguishable from a legacy raw value. Envelopes are only
// written in EnableEtcdCompatibility mode; the reserved value prefix is
// documented as such. Legacy (un-enveloped) values are still read correctly via
// a fallback to the etcdmeta keyspace, so no data migration is required.
var (
	valueMetaMagic   = []byte{0x00, 0x6b, 0x62, 0x01} // v1 "\x00kb\x01"
	valueMetaMagicV2 = []byte{0x00, 0x6b, 0x62, 0x02} // v2 "\x00kb\x02" (adds lease)
	valueMetaMagicV3 = []byte{0x00, 0x6b, 0x62, 0x03} // v3 "\x00kb\x03" (known lease=0)
)

const (
	valueMetaHeaderLen   = 4 + 8 + 8     // v1: magic + createRevision + version
	valueMetaHeaderLenV2 = 4 + 8 + 8 + 8 // v2: + lease
)

// encodeValueWithMeta wraps a raw value with its inline metadata envelope,
// choosing v2 (with an inline lease) only when the version is leased.
func encodeValueWithMeta(value []byte, meta EtcdMetadata) []byte {
	if meta.Lease != 0 {
		buf := make([]byte, valueMetaHeaderLenV2+len(value))
		copy(buf, valueMetaMagicV2)
		binary.BigEndian.PutUint64(buf[4:], meta.CreateRevision)
		binary.BigEndian.PutUint64(buf[12:], meta.Version)
		binary.BigEndian.PutUint64(buf[20:], uint64(meta.Lease))
		copy(buf[valueMetaHeaderLenV2:], value)
		return buf
	}
	buf := make([]byte, valueMetaHeaderLen+len(value))
	copy(buf, valueMetaMagicV3)
	binary.BigEndian.PutUint64(buf[4:], meta.CreateRevision)
	binary.BigEndian.PutUint64(buf[12:], meta.Version)
	copy(buf[valueMetaHeaderLen:], value)
	return buf
}

// hasValueMeta reports whether stored is a v1 inline-metadata envelope.
func hasValueMeta(stored []byte) bool {
	return len(stored) >= valueMetaHeaderLen &&
		stored[0] == valueMetaMagic[0] &&
		stored[1] == valueMetaMagic[1] &&
		stored[2] == valueMetaMagic[2] &&
		stored[3] == valueMetaMagic[3]
}

// hasValueMetaV2 reports whether stored is a v2 (lease-carrying) envelope.
func hasValueMetaV2(stored []byte) bool {
	return len(stored) >= valueMetaHeaderLenV2 &&
		stored[0] == valueMetaMagicV2[0] &&
		stored[1] == valueMetaMagicV2[1] &&
		stored[2] == valueMetaMagicV2[2] &&
		stored[3] == valueMetaMagicV2[3]
}

func hasValueMetaV3(stored []byte) bool {
	return len(stored) >= valueMetaHeaderLen &&
		stored[0] == valueMetaMagicV3[0] &&
		stored[1] == valueMetaMagicV3[1] &&
		stored[2] == valueMetaMagicV3[2] &&
		stored[3] == valueMetaMagicV3[3]
}

// InlineValueLeaseKnown reports whether the envelope was written after the
// per-version lease format became explicit. v2 carries a non-zero lease and v3
// explicitly carries no lease; legacy v1 cannot prove either state.
func InlineValueLeaseKnown(stored []byte) bool {
	return hasValueMetaV2(stored) || hasValueMetaV3(stored)
}

// DecodeInlineValue is the exported form of decodeValueWithMeta for other
// packages (etcd shim, brain server) that must strip the envelope before
// returning a value to clients.
func DecodeInlineValue(stored []byte) (meta EtcdMetadata, rawValue []byte, inlined bool) {
	return decodeValueWithMeta(stored)
}

// StripInlineValue returns the raw value with any inline-metadata envelope
// removed (passthrough for legacy values). For consumers that need only the
// value, not the metadata.
func StripInlineValue(stored []byte) []byte {
	_, raw, _ := decodeValueWithMeta(stored)
	return raw
}

// decodeValueWithMeta splits an enveloped value into its metadata and raw value.
// The returned rawValue aliases stored (no copy); callers that retain it beyond
// the buffer's lifetime must copy. ok is false for legacy (un-enveloped) values,
// in which case rawValue == stored and meta is zero.
func decodeValueWithMeta(stored []byte) (meta EtcdMetadata, rawValue []byte, ok bool) {
	if hasValueMetaV2(stored) {
		meta.CreateRevision = binary.BigEndian.Uint64(stored[4:12])
		meta.Version = binary.BigEndian.Uint64(stored[12:20])
		meta.Lease = int64(binary.BigEndian.Uint64(stored[20:valueMetaHeaderLenV2]))
		return meta, stored[valueMetaHeaderLenV2:], true
	}
	if hasValueMetaV3(stored) {
		meta.CreateRevision = binary.BigEndian.Uint64(stored[4:12])
		meta.Version = binary.BigEndian.Uint64(stored[12:valueMetaHeaderLen])
		return meta, stored[valueMetaHeaderLen:], true
	}
	if !hasValueMeta(stored) {
		return EtcdMetadata{}, stored, false
	}
	meta.CreateRevision = binary.BigEndian.Uint64(stored[4:12])
	meta.Version = binary.BigEndian.Uint64(stored[12:valueMetaHeaderLen])
	return meta, stored[valueMetaHeaderLen:], true
}
