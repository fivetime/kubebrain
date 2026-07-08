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
// revision <= rev (i.e. the start of rev+1).
func EventLogRangeEnd(rev uint64) []byte { return EncodeEventLogKey(rev+1, nil) }

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
