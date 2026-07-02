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
// version) into the stored object value, so LIST/Get/watch need no separate
// metadata lookup and the \x00kubebrain/etcdmeta/ keyspace stops growing.
//
// A new value is wrapped in an envelope:
//
//	[ valueMetaMagic (4B) ][ createRevision (8B) ][ version (8B) ][ raw value... ]
//
// The magic starts with 0x00. Kubernetes-stored values always begin with the
// protobuf prefix "k8s\x00" (0x6b) or JSON '{' (0x7b), and KubeBrain's tombstone
// marker begins with 't' (0x74) — none start with 0x00 — so an enveloped value
// is unambiguously distinguishable from a legacy raw value. Envelopes are only
// written in EnableEtcdCompatibility mode; the reserved value prefix is
// documented as such. Legacy (un-enveloped) values are still read correctly via
// a fallback to the etcdmeta keyspace, so no data migration is required.
var valueMetaMagic = []byte{0x00, 0x6b, 0x62, 0x01} // "\x00kb\x01"

const valueMetaHeaderLen = 4 + 8 + 8 // magic + createRevision + version

// encodeValueWithMeta wraps a raw value with its inline metadata envelope.
func encodeValueWithMeta(value []byte, meta EtcdMetadata) []byte {
	buf := make([]byte, valueMetaHeaderLen+len(value))
	copy(buf, valueMetaMagic)
	binary.BigEndian.PutUint64(buf[4:], meta.CreateRevision)
	binary.BigEndian.PutUint64(buf[12:], meta.Version)
	copy(buf[valueMetaHeaderLen:], value)
	return buf
}

// hasValueMeta reports whether stored is an inline-metadata envelope.
func hasValueMeta(stored []byte) bool {
	return len(stored) >= valueMetaHeaderLen &&
		stored[0] == valueMetaMagic[0] &&
		stored[1] == valueMetaMagic[1] &&
		stored[2] == valueMetaMagic[2] &&
		stored[3] == valueMetaMagic[3]
}

// decodeValueWithMeta splits an enveloped value into its metadata and raw value.
// The returned rawValue aliases stored (no copy); callers that retain it beyond
// the buffer's lifetime must copy. ok is false for legacy (un-enveloped) values,
// in which case rawValue == stored and meta is zero.
func decodeValueWithMeta(stored []byte) (meta EtcdMetadata, rawValue []byte, ok bool) {
	if !hasValueMeta(stored) {
		return EtcdMetadata{}, stored, false
	}
	meta.CreateRevision = binary.BigEndian.Uint64(stored[4:12])
	meta.Version = binary.BigEndian.Uint64(stored[12:valueMetaHeaderLen])
	return meta, stored[valueMetaHeaderLen:], true
}
