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
)

// Event-log keyspace (#45): every committed write appends, in the SAME storage
// transaction, one small entry keyed by its revision. A watch that reconnects
// below the ring's oldest revision replays this log with a bounded range read —
// O(events in the window) — instead of a full-prefix object scan, which at 10M+
// keys took minutes per reconnect and could not outrun the write rate.
//
// Layout: {keyspace magic} + "\x00elog\x00" + bigEndian8(revision) + userKey.
// The suffix keeps entries unique when one revision covers many keys
// (DeleteRange), while the big-endian revision keeps the log range-scannable in
// revision order. The "\x00elog\x00" segment sorts BELOW every object/revision
// key (their user keys start with '/', and '\x00' < '/'), so object-range scans
// never touch the log; sharing the keyspace magic keeps each tenant's log
// inside its own GC/scan borders (#76). Key construction and decoding live on
// Keyspace (keyspace.go); this file keeps the keyspace-independent value codec.

// eventLogInfix separates the keyspace magic from the big-endian revision in
// event-log keys, and elogMetaInfix marks the log's completeness watermark key
// (entries at or below the watermark may have been cleaned — replays must fall
// back to the object scan).
var (
	eventLogInfix = []byte("\x00elog\x00")
	elogMetaInfix = []byte("\x00elogmeta")
)

// EncodeEventLogValue packs an entry's payload: [1B verb][8B prevRevision].
// verb is the proto.Event_EventType numeric value; kept as a raw byte here so
// the coder stays free of the rpc types.
func EncodeEventLogValue(verb byte, prevRev uint64) []byte {
	v := make([]byte, 9)
	v[0] = verb
	binary.BigEndian.PutUint64(v[1:], prevRev)
	return v
}

// DecodeEventLogValue unpacks an entry's payload.
func DecodeEventLogValue(v []byte) (verb byte, prevRev uint64, ok bool) {
	if len(v) != 9 {
		return 0, 0, false
	}
	return v[0], binary.BigEndian.Uint64(v[1:]), true
}

// keyPrefixEnd returns the smallest key strictly greater than every key that has
// p as a prefix: p with its last non-0xFF byte incremented and trailing 0xFF
// bytes dropped. It returns nil ("no upper bound") only when p is entirely 0xFF,
// which cannot happen for an elogMagic-prefixed key (the infix contains '\x00').
func keyPrefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
