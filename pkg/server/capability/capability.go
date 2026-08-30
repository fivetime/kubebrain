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

// Package capability defines machine-readable data-plane capabilities consumed
// by DBaaS rollout orchestration. These are KubeBrain release-safety contracts,
// not etcd API features.
package capability

import (
	"fmt"
	"slices"
)

const (
	DocumentFormat = "kubebrain.info-capabilities.v1"

	// SnapshotDrainPinned means Snapshot establishes its immutable history
	// iterator and compaction pin before releasing the logical write barrier,
	// and does not hold that barrier while waiting for stream data.
	SnapshotDrainPinned = "snapshot-history-pin-before-write-barrier-release.v1"
)

var supported = []string{SnapshotDrainPinned}

type Document struct {
	Format       string   `json:"format"`
	Capabilities []string `json:"capabilities"`
}

func SupportedDocument() Document {
	return Document{Format: DocumentFormat, Capabilities: slices.Clone(supported)}
}

func (d Document) Validate() error {
	if d.Format != DocumentFormat {
		return fmt.Errorf("capability document format %q is not %q", d.Format, DocumentFormat)
	}
	for index, value := range d.Capabilities {
		if !validName(value) {
			return fmt.Errorf("capability at index %d is not a canonical name", index)
		}
		if index > 0 && d.Capabilities[index-1] >= value {
			return fmt.Errorf("capabilities must be strictly sorted and unique")
		}
	}
	return nil
}

func validName(value string) bool {
	if len(value) == 0 || len(value) > 128 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range []byte(value[1:]) {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') &&
			character != '.' && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func (d Document) Has(required string) bool {
	_, found := slices.BinarySearch(d.Capabilities, required)
	return found
}
