// Copyright 2023 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"math"
)

// ParseRevisionWatermark decodes an exact-width persisted MVCC watermark.
// Unlike ParseRevision, watermarks never carry the tombstone suffix; accepting
// either a short value or trailing bytes would make corruption look like a
// missing/valid compaction boundary.
func ParseRevisionWatermark(value []byte) (uint64, error) {
	if len(value) != RevisionValueLength {
		return 0, MarkInvalidMVCCMetadata(fmt.Errorf("invalid revision watermark length %d", len(value)))
	}
	revision := binary.BigEndian.Uint64(value)
	if revision > math.MaxInt64 {
		return 0, MarkInvalidMVCCMetadata(fmt.Errorf("revision watermark %d exceeds MaxInt64", revision))
	}
	return revision, nil
}

const (
	RevisionValueLength                 = 8
	RevisionValueLengthWithDeletionFlag = 9
)

var (
	ErrInvalidRevFormat = errors.New("invalid format of revision bytes")
)

// ParseRevision parses revision bytes
func ParseRevision(revisionBytes []byte) (rev uint64, isTombStone bool, err error) {
	if len(revisionBytes) == RevisionValueLength {
		rev = binary.BigEndian.Uint64(revisionBytes[0:8])
		if rev > math.MaxInt64 {
			return 0, false, MarkInvalidMVCCMetadata(fmt.Errorf("revision %d exceeds MaxInt64", rev))
		}
		return rev, false, nil
	}

	if len(revisionBytes) == RevisionValueLengthWithDeletionFlag {
		rev = binary.BigEndian.Uint64(revisionBytes[0:8])
		if rev > math.MaxInt64 {
			return 0, false, MarkInvalidMVCCMetadata(fmt.Errorf("revision %d exceeds MaxInt64", rev))
		}
		return rev, true, nil
	}

	// may be caused by
	// 1. issues of storage
	// 2. unexpected writing
	return 0, false, MarkInvalidMVCCMetadata(ErrInvalidRevFormat)
}
