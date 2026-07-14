// Copyright 2022 ByteDance and/or its affiliates
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
	"encoding/hex"

	"github.com/pkg/errors"
)

var (
	magic      = "\x57\xfb\x80\x8b"
	magicBytes = []byte(magic)

	// splitKey must be less than [ 0-9 a-z A-Z - . _ ], so keys of one object are continuous
	splitKey  = string(splitByte)
	splitByte = byte('$')
)

func NewNormalCoder() Coder {
	return &normalEncoderDecoder{}
}

// ObjectKeyspaceStart returns the smallest possible encoded object key: every
// object key is {magic}{userKey}{split}{revision}, so the bare magic prefix sorts
// at or before all of them (and after every non-object key, which lacks the magic
// prefix). Paired with ObjectKeyspaceEnd it bounds the ENTIRE object keyspace
// regardless of user prefix — used by compaction so physical GC never depends on
// a configured key-prefix matching the client's real keys.
func ObjectKeyspaceStart() []byte {
	return append([]byte(nil), magicBytes...)
}

func ObjectKeyspaceEnd() []byte {
	end := append([]byte(nil), magicBytes...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0xff}
}

// normalEncoderDecoder encode user key with larger revision to larger internal key
type normalEncoderDecoder struct{}

// EncodeObjectKey implements Coder interface
func (n *normalEncoderDecoder) EncodeObjectKey(userKey []byte, revision uint64) []byte {
	key := make([]byte, len(magicBytes)+len(userKey)+1+8)
	// {magic}:{raw_key}:{split_key}:{revision}
	copy(key, magicBytes)
	copy(key[len(magicBytes):], userKey)
	copy(key[len(magicBytes)+len(userKey):], splitKey)
	binary.BigEndian.PutUint64(key[len(magicBytes)+len(userKey)+1:], revision)
	return key
}

// EncodeRevisionKey implements Coder interface
func (n *normalEncoderDecoder) EncodeRevisionKey(key []byte) []byte {
	return n.EncodeObjectKey(key, 0)
}

// RevisionBoundaryForBorder returns the rev=0 object key (the start of a user
// key's version run) for the user key that `border` falls within, and true, for
// ANY border inside the object keyspace ({magic}{userKey}{split}{...}) — even when
// border is not a full/decodable object key (e.g. a TiKV region split point like
// {objectKey}\x00 or a truncated key). The scanner snaps a partition boundary to
// this so a single user key's MVCC versions never straddle two scan partitions,
// which would otherwise let a partition holding an older live version emit a key
// whose latest version (a tombstone in the adjacent partition) marks it deleted.
//
// It relies on the coder invariant that splitByte is smaller than every byte a
// user key can contain, so the FIRST splitByte after the magic prefix is always
// the userKey/revision delimiter. Returns (nil,false) when border is not in the
// object keyspace or has no split byte yet (already at/before a user-key start).
func RevisionBoundaryForBorder(border []byte) ([]byte, bool) {
	if len(border) < len(magicBytes) || !bytes.Equal(border[:len(magicBytes)], magicBytes) {
		return nil, false
	}
	rest := border[len(magicBytes):]
	i := bytes.IndexByte(rest, splitByte)
	if i < 0 {
		return nil, false
	}
	userKey := rest[:i]
	return (&normalEncoderDecoder{}).EncodeObjectKey(userKey, 0), true
}

// Decode implements Coder interface
func (n *normalEncoderDecoder) Decode(internalKey []byte) (userKey []byte, revision uint64, err error) {
	if !bytes.Equal(internalKey[:len(magicBytes)], magicBytes) {
		return nil, 0, errors.Errorf("magic number not right for object key %v", hex.EncodeToString(internalKey))
	}

	if internalKey[len(internalKey)-9] != splitByte {
		return nil, 0, errors.Errorf("split byte not right for object key %v", hex.EncodeToString(internalKey))
	}

	revision = binary.BigEndian.Uint64(internalKey[len(internalKey)-8:])
	userKey = internalKey[len(magic) : len(internalKey)-9]
	return
}
