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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"regexp"
)

// A Keyspace derives every magic-prefixed key family — object keys, the event
// log, the event-log meta key — for ONE tenant, so multiple KubeBrain clusters
// can share a storage cluster without seeing (or garbage-collecting) each
// other's data (#76). Everything that touches storage flows through a Keyspace
// (or the Coder it builds); there are deliberately no package-level key
// constructors left, so a call site cannot accidentally reach into the default
// tenant's space.
//
// The unnamed ("") keyspace uses the original 4-byte magic, so every existing
// single-tenant deployment keeps reading its data unchanged. A named keyspace
// gets a 9-byte magic 0x58 || sha256(name)[:8]: fixed length, so no keyspace's
// range can be a prefix of another's, and disjoint from the legacy magic
// (0x57...) by first byte, so a legacy cluster's whole-keyspace GC scan can
// never reach a named tenant and vice versa.
type Keyspace struct {
	name        string
	magic       []byte
	elogMagic   []byte
	elogMetaKey []byte
}

var internalKVInfix = []byte("\x00internal\x00")

// EncodeInternalKey maps service metadata into a tenant-scoped raw storage
// keyspace. Internal values are durable but deliberately bypass user MVCC,
// revisions, event logs, watches, and the count index.
func (k *Keyspace) EncodeInternalKey(key []byte) []byte {
	out := make([]byte, 0, len(k.magic)+len(internalKVInfix)+len(key))
	out = append(out, k.magic...)
	out = append(out, internalKVInfix...)
	return append(out, key...)
}

// keyspaceNameRE bounds names to something that also embeds cleanly into the
// raw coordination-key namespace (election lock, compact watermark).
var keyspaceNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)

const namedKeyspaceMagicFirstByte = 0x58

// NewKeyspace builds the keyspace for name; "" is the default (legacy) tenant.
func NewKeyspace(name string) (*Keyspace, error) {
	if name == "" {
		return newKeyspaceFromMagic("", magicBytes), nil
	}
	if !keyspaceNameRE.MatchString(name) {
		return nil, fmt.Errorf("invalid keyspace %q: must match %s", name, keyspaceNameRE)
	}
	sum := sha256.Sum256([]byte(name))
	magic := make([]byte, 0, 9)
	magic = append(magic, namedKeyspaceMagicFirstByte)
	magic = append(magic, sum[:8]...)
	return newKeyspaceFromMagic(name, magic), nil
}

// DefaultKeyspace returns the legacy ("") tenant.
func DefaultKeyspace() *Keyspace {
	ks, _ := NewKeyspace("")
	return ks
}

func newKeyspaceFromMagic(name string, magic []byte) *Keyspace {
	return &Keyspace{
		name:        name,
		magic:       append([]byte(nil), magic...),
		elogMagic:   append(append([]byte(nil), magic...), eventLogInfix...),
		elogMetaKey: append(append([]byte(nil), magic...), elogMetaInfix...),
	}
}

// Name returns the keyspace name ("" for the default tenant).
func (k *Keyspace) Name() string { return k.name }

// NewCoder builds the object-key Coder bound to this keyspace's magic.
func (k *Keyspace) NewCoder() Coder {
	return &normalEncoderDecoder{magic: k.magic}
}

// ObjectKeyspaceStart returns the smallest possible encoded object key of this
// keyspace: every object key is {magic}{userKey}{split}{revision}, so the bare
// magic prefix sorts at or before all of them. Paired with ObjectKeyspaceEnd it
// bounds this tenant's ENTIRE object keyspace (including the event log and the
// internal \x00kubebrain/ namespace, which share the magic) — used by
// compaction and the count-index rebuild so neither depends on any configured
// prefix matching the client's real keys, and neither can cross into another
// tenant.
func (k *Keyspace) ObjectKeyspaceStart() []byte {
	return append([]byte(nil), k.magic...)
}

// ObjectKeyspaceEnd returns the exclusive upper bound of this keyspace.
func (k *Keyspace) ObjectKeyspaceEnd() []byte {
	end := append([]byte(nil), k.magic...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0xff}
}

// EncodeEventLogKey builds the event-log key for one (revision, userKey) entry.
// See the event-log keyspace comment in eventlog.go for the layout rationale.
func (k *Keyspace) EncodeEventLogKey(revision uint64, userKey []byte) []byte {
	out := make([]byte, 0, len(k.elogMagic)+8+len(userKey))
	out = append(out, k.elogMagic...)
	var rev [8]byte
	binary.BigEndian.PutUint64(rev[:], revision)
	out = append(out, rev[:]...)
	return append(out, userKey...)
}

// EventLogRangeStart returns the smallest key of entries with revision >= rev.
func (k *Keyspace) EventLogRangeStart(rev uint64) []byte { return k.EncodeEventLogKey(rev, nil) }

// EventLogRangeEnd returns the exclusive upper bound covering every entry with
// revision <= rev; see eventlog.go for why this is a prefix end rather than
// EncodeEventLogKey(rev+1).
func (k *Keyspace) EventLogRangeEnd(rev uint64) []byte {
	return keyPrefixEnd(k.EncodeEventLogKey(rev, nil))
}

// ElogMetaStartKey returns this keyspace's event-log completeness watermark key.
func (k *Keyspace) ElogMetaStartKey() []byte {
	return append([]byte(nil), k.elogMetaKey...)
}

// IsInternalStorageKey reports whether key belongs to a raw, non-object family
// that shares this tenant's broad physical keyspace bounds.
func (k *Keyspace) IsInternalStorageKey(key []byte) bool {
	internalPrefix := append(append([]byte(nil), k.magic...), internalKVInfix...)
	return bytes.HasPrefix(key, k.elogMagic) ||
		bytes.HasPrefix(key, k.elogMetaKey) ||
		bytes.HasPrefix(key, internalPrefix)
}

// DecodeEventLogKey splits one of this keyspace's event-log keys back into
// (revision, userKey).
func (k *Keyspace) DecodeEventLogKey(key []byte) (revision uint64, userKey []byte, err error) {
	if len(key) < len(k.elogMagic)+8 || !bytes.Equal(key[:len(k.elogMagic)], k.elogMagic) {
		return 0, nil, fmt.Errorf("not an event log key: %q", key)
	}
	rest := key[len(k.elogMagic):]
	return binary.BigEndian.Uint64(rest[:8]), rest[8:], nil
}
