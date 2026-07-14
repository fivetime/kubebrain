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
	"bytes"
	"encoding/binary"
	"fmt"
)

// Event-log keyspace (#45): every committed write appends, in the SAME storage
// transaction, one small entry keyed by its revision. A watch that reconnects
// below the ring's oldest revision replays this log with a bounded range read —
// O(events in the window) — instead of a full-prefix object scan, which at 10M+
// keys took minutes per reconnect and could not outrun the write rate.
//
// Layout: elogMagic + bigEndian8(revision) + userKey. The suffix keeps entries
// unique when one revision covers many keys (DeleteRange), while the big-endian
// revision keeps the log range-scannable in revision order. elogMagic sorts
// BELOW every object/revision key (their user keys start with '/', and
// '\x00' < '/'), so full-keyspace scans (rebuild, count fallback, compaction
// borders anchored at the configured prefix) never touch the log.
var (
	elogMagic = append(append([]byte(nil), magicBytes...), []byte("\x00elog\x00")...)
	// ElogMetaStartKey holds the revision the event log is complete AFTER:
	// entries with revision <= this may have been cleaned (or never written —
	// e.g. before an upgrade introduced the log). Replays at or below it must
	// fall back to the object scan.
	ElogMetaStartKey = append(append([]byte(nil), magicBytes...), []byte("\x00elogmeta")...)
)

// EncodeEventLogKey builds the event-log key for one (revision, userKey) entry.
func EncodeEventLogKey(revision uint64, userKey []byte) []byte {
	k := make([]byte, 0, len(elogMagic)+8+len(userKey))
	k = append(k, elogMagic...)
	var rev [8]byte
	binary.BigEndian.PutUint64(rev[:], revision)
	k = append(k, rev[:]...)
	return append(k, userKey...)
}

// EventLogRangeStart returns the smallest key of entries with revision >= rev.
func EventLogRangeStart(rev uint64) []byte { return EncodeEventLogKey(rev, nil) }

// EventLogRangeEnd returns the exclusive upper bound covering every entry with
// revision <= rev. It is the byte-level prefix end of rev's key space, NOT
// EncodeEventLogKey(rev+1): at rev == MaxUint64, rev+1 wraps to 0, producing an
// end that sorts BELOW the start so the range read scans nothing (revision is a
// TSO, so this is ~584 years out — theoretical, but a silent-wrong-answer, not a
// loud failure). The prefix end instead carries into elogMagic and stays above
// every entry at rev. For rev < MaxUint64 it is a tighter byte encoding of the
// exact same exclusive boundary as EncodeEventLogKey(rev+1, nil).
func EventLogRangeEnd(rev uint64) []byte { return keyPrefixEnd(EncodeEventLogKey(rev, nil)) }

// keyPrefixEnd returns the smallest key strictly greater than every key that has
// p as a prefix: p with its last non-0xFF byte incremented and trailing 0xFF
// bytes dropped. It returns nil ("no upper bound") only when p is entirely 0xFF,
// which cannot happen for an elogMagic-prefixed key (elogMagic ends in '\x00').
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

// DecodeEventLogKey splits an event-log key into (revision, userKey).
func DecodeEventLogKey(key []byte) (revision uint64, userKey []byte, err error) {
	if !bytes.HasPrefix(key, elogMagic) || len(key) < len(elogMagic)+8 {
		return 0, nil, fmt.Errorf("not an event log key: %q", key)
	}
	rest := key[len(elogMagic):]
	return binary.BigEndian.Uint64(rest[:8]), rest[8:], nil
}

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
