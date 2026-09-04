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
	"fmt"
	"math"
)

// latestMetadata is an auxiliary latest-only projection committed beside the
// revision index and object row. ModRevision is the join fence: a reader may
// use Metadata only when it exactly matches the authoritative revision index
// from the same engine snapshot. This makes mixed old/new writer rollouts safe.
type latestMetadata struct {
	ModRevision uint64
	Metadata    EtcdMetadata
	Tombstone   bool
}

var latestMetadataMagic = []byte{0x00, 'k', 'm', 0x01}

const latestMetadataEncodedLen = 4 + 8 + 8 + 8 + 8 + 1

func encodeLatestMetadata(meta latestMetadata) []byte {
	value := make([]byte, latestMetadataEncodedLen)
	copy(value, latestMetadataMagic)
	binary.BigEndian.PutUint64(value[4:12], meta.ModRevision)
	binary.BigEndian.PutUint64(value[12:20], meta.Metadata.CreateRevision)
	binary.BigEndian.PutUint64(value[20:28], meta.Metadata.Version)
	binary.BigEndian.PutUint64(value[28:36], uint64(meta.Metadata.Lease))
	if meta.Tombstone {
		value[36] = 1
	}
	return value
}

func decodeLatestMetadata(value []byte) (latestMetadata, error) {
	if len(value) != latestMetadataEncodedLen {
		return latestMetadata{}, fmt.Errorf("%w: latest metadata length %d differs from %d",
			ErrInvalidMVCCMetadata, len(value), latestMetadataEncodedLen)
	}
	if !bytes.Equal(value[:3], latestMetadataMagic[:3]) {
		return latestMetadata{}, fmt.Errorf("%w: invalid latest metadata magic", ErrInvalidMVCCMetadata)
	}
	if value[3] != latestMetadataMagic[3] {
		return latestMetadata{}, fmt.Errorf("%w: unsupported latest metadata version %d",
			ErrInvalidMVCCMetadata, value[3])
	}
	if value[36] > 1 {
		return latestMetadata{}, fmt.Errorf("%w: invalid latest metadata flags %#x",
			ErrInvalidMVCCMetadata, value[36])
	}
	result := latestMetadata{
		ModRevision: binary.BigEndian.Uint64(value[4:12]),
		Metadata: EtcdMetadata{
			CreateRevision: binary.BigEndian.Uint64(value[12:20]),
			Version:        binary.BigEndian.Uint64(value[20:28]),
			Lease:          int64(binary.BigEndian.Uint64(value[28:36])),
		},
		Tombstone: value[36] == 1,
	}
	if result.ModRevision == 0 || result.ModRevision > math.MaxInt64 {
		return latestMetadata{}, fmt.Errorf("%w: latest metadata mod revision %d is invalid",
			ErrInvalidMVCCMetadata, result.ModRevision)
	}
	if result.Tombstone {
		if result.Metadata != (EtcdMetadata{}) {
			return latestMetadata{}, fmt.Errorf("%w: tombstone latest metadata carries live fields",
				ErrInvalidMVCCMetadata)
		}
		return result, nil
	}
	if err := ValidateEtcdMetadataAtRevision(result.Metadata, result.ModRevision, "latest metadata"); err != nil {
		return latestMetadata{}, err
	}
	return result, nil
}
