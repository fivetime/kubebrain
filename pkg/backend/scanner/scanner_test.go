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

package scanner

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/golang/mock/gomock"
	mock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestAdjustPartitionBorders(t *testing.T) {
	ast := assert.New(t)
	c := coder.NewNormalCoder()
	s := scanner{coder: c}

	keys := [][]byte{
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 36, 0, 0, 0, 0, 0, 0, 0, 0},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 101, 118, 101, 110, 116, 115, 47, 98, 100, 101, 102, 97, 117, 108, 116, 47, 118, 107, 45, 116, 101, 115, 116, 45, 112, 111, 100, 45, 118, 113, 115, 114, 106, 46, 49, 54, 98, 101, 101, 51, 101, 55, 56, 52, 98, 50, 101, 48, 101, 57},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 101, 118, 101, 110, 116, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 116, 101, 115, 116, 45, 115, 105, 100, 101, 99, 97, 114, 45, 116, 101, 115, 116, 45, 55, 52, 57, 54, 53, 100, 55, 98, 55, 57, 45, 99, 120, 99, 104, 112, 46, 49, 54, 99, 49, 51, 55, 100, 49, 54, 57, 98, 48, 56, 54, 99, 98},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 101, 118, 101, 110, 116, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 116, 101, 115, 116, 45, 115, 105, 100, 101, 99, 97, 114, 45, 116, 101, 115, 116, 45, 55, 52, 57, 54, 53, 100, 55, 98, 55, 57, 45, 108, 103, 119, 112, 99, 46, 49, 54, 99, 49, 52, 54, 101, 99, 48, 53, 54, 53, 51, 100, 97, 102},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 101, 118, 101, 110, 116, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 116, 101, 115, 116, 45, 115, 105, 100, 101, 99, 97, 114, 45, 116, 101, 115, 116, 45, 55, 52, 57, 54, 53, 100, 55, 98, 55, 57, 45, 115, 55, 107, 57, 120, 46, 49, 54, 99, 49, 51, 57, 100, 48, 57, 101, 50, 54, 49, 97, 57, 102},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 101, 118, 101, 110, 116, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 116, 101, 115, 116, 45, 115, 105, 100, 101, 99, 97, 114, 45, 116, 101, 115, 116, 45, 57, 55, 98, 98, 57, 53, 55, 52, 55, 45, 50, 108, 102, 52, 104, 46, 49, 54, 99, 49, 51, 56, 55, 54, 100, 49, 49, 49, 52, 56, 49, 99},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 101, 118, 101, 110, 116, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 118, 107, 45, 112, 101, 114, 102, 111, 114, 109, 97, 99, 101, 45, 112, 111, 100, 45, 114, 120, 120, 115, 52, 46, 49, 54, 98, 101, 98, 98, 97, 101, 55, 97, 51, 56, 54, 54, 52, 57},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 112, 111, 100, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 116, 101, 115, 116, 45, 115, 105, 100, 101, 99, 97, 114, 45, 116, 101, 115, 116, 45, 55, 52, 57, 54, 53, 100, 55, 98, 55, 57, 45, 100, 108, 122, 108, 50},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 112, 111, 100, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 116, 101, 115, 116, 45, 115, 105, 100, 101, 99, 97, 114, 45, 116, 101, 115, 116, 45, 55, 52, 57, 54, 53, 100, 55, 98, 55, 57, 45, 110, 122, 98, 106, 99},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 112, 111, 100, 115, 47, 116, 101, 115, 116, 47, 99, 114, 45, 56, 53, 53, 53, 55, 102, 99, 100, 45, 108, 112, 118, 45, 116, 101, 115, 116, 45, 118, 107, 54, 45, 104, 108, 45, 100, 114, 105, 118, 101, 114, 45, 100, 57, 122, 100, 110},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 112, 111, 100, 115, 47, 116, 101, 115, 116, 47, 99, 114, 45, 56, 53, 53, 53, 55, 102, 99, 100, 45, 116, 101, 115, 116, 45, 115, 116, 97, 116, 117, 115, 45, 99, 97, 99, 104, 101, 45, 116, 101, 115, 116, 45, 118, 107, 54, 45, 104, 108, 45, 116, 101, 115, 116, 45, 115, 116, 97, 116, 117, 115, 45, 99, 97, 99, 104, 101, 45, 108, 116, 55, 103, 113},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 114, 101, 112, 108, 105, 99, 97, 115, 101, 116, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 57, 55, 49, 55, 50, 57, 54, 50, 53, 98, 45, 54, 98, 100, 52, 52, 57, 57, 52, 102, 56},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 49, 98, 56, 54, 56, 51, 51, 57, 53, 97, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 52, 97, 49, 99, 51, 97, 56, 56, 57, 57, 48, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 53, 49, 97, 54, 49, 97, 97, 102, 50, 100, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 55, 56, 53, 101, 50, 55, 101, 53, 56, 50, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 57, 100, 52, 51, 57, 50, 101, 55, 51, 52, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 99, 57, 50, 50, 51, 101, 49, 50, 102, 98, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 47, 116, 101, 115, 116, 47, 115, 116, 97, 116, 101, 102, 117, 108, 115, 101, 116, 101, 120, 116, 101, 110, 115, 105, 111, 110, 115, 47, 100, 101, 102, 97, 117, 108, 116, 47, 100, 112, 45, 100, 57, 102, 97, 49, 56, 51, 55, 101, 54, 45, 48},
		{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 48, 36, 0, 0, 0, 0, 0, 0, 0, 0},
	}

	var partitions []storage.Partition
	for i := 1; i < len(keys); i++ {
		partitions = append(partitions, storage.Partition{
			Start: keys[i-1],
			End:   keys[i],
		})
	}

	for idx, p := range partitions {
		t.Log()
		t.Log(idx, string(p.Start))
		t.Log(idx, string(p.End))
	}

	partitions = s.adjustPartitionsBorders(partitions)

	for idx, p := range partitions {
		t.Log()
		t.Log(idx, string(p.Start))
		t.Log(idx, string(p.End))
		ast.True(!bytes.Equal(p.Start, p.End))
	}
}

// splitStore wraps a real KvStorage but forces GetPartitions to split the scanned
// range at chosen boundaries, so a single user key's object versions can be placed
// in different scan partitions deterministically.
type splitStore struct {
	storage.KvStorage
	splits [][]byte
}

func (s *splitStore) GetPartitions(ctx context.Context, start, end []byte) ([]storage.Partition, error) {
	bounds := [][]byte{start}
	for _, b := range s.splits {
		if bytes.Compare(start, b) < 0 && bytes.Compare(b, end) < 0 {
			bounds = append(bounds, b)
		}
	}
	bounds = append(bounds, end)
	var ps []storage.Partition
	for i := 1; i < len(bounds); i++ {
		ps = append(ps, storage.Partition{Start: bounds[i-1], End: bounds[i]})
	}
	return ps, nil
}

func beU64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// TestScannerCrossPartitionTombstone: a user key whose latest version is a
// tombstone must NOT resurface as live in a List when its versions straddle a scan
// partition boundary. A boundary that decodes cleanly is snapped by
// adjustPartitionsBorders; a boundary that does NOT decode must still be safe.
func TestScannerCrossPartitionTombstone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.NewNormalCoder()
	tomb := []byte("tombstone")

	key := []byte("/registry/pods/default/p1")
	const r1 uint64 = 100
	const r2 uint64 = 200

	cases := []struct {
		name  string
		split []byte
	}{
		{"decodable-boundary-at-tombstone", c.EncodeObjectKey(key, r2)},
		{"nondecodable-boundary-between-versions", append(append([]byte{}, c.EncodeObjectKey(key, r1)...), 0x00)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := imemkv.NewKvStorage()
			defer kv.Close()
			b := kv.BeginBatchWrite()
			b.Put(c.EncodeObjectKey(key, 0), append(beU64(r2), 0), 0)  // deleted index {r2}{del}
			b.Put(c.EncodeObjectKey(key, r1), []byte("live-value"), 0) // older live object
			b.Put(c.EncodeObjectKey(key, r2), tomb, 0)                 // latest = tombstone
			require.NoError(t, b.Commit(context.Background()))

			st := &splitStore{KvStorage: kv, splits: [][]byte{tc.split}}
			sc := NewScanner(st, c, Config{CompactKey: []byte("/compact"), Tombstone: tomb}, m)

			start := c.EncodeObjectKey(key, 0)
			end := c.EncodeObjectKey(append(append([]byte{}, key...), 0xff), 0)
			kvs, err := sc.Range(context.Background(), start, end, 1000, 0)
			require.NoError(t, err)
			for _, got := range kvs {
				if bytes.Equal(got.Key, key) {
					t.Fatalf("DELETED key resurfaced in List at rev=%d (partition split=%v)", got.Revision, tc.split)
				}
			}
		})
	}
}
