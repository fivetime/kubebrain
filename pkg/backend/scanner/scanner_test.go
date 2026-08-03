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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	mock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestAdjustPartitionBorders(t *testing.T) {
	ast := assert.New(t)
	c := coder.DefaultKeyspace().NewCoder()
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

// TestScannerCrossPartitionTombstoneLargeGap reproduces the exact shape of the
// live "poison key" anomaly: a user key whose live base object and its later
// tombstone are separated by a HUGE inter-version revision gap (the real values
// observed: 467364010600169522 and 467421706742398990), scanned across a
// partition boundary placed at many realistic TiKV region-split positions
// (decodable object key, object key + \x00, a split cutting the 8-byte revision
// suffix in half, the bare "{userKey}$" delimiter, and a boundary with no split
// byte at all). In every case the deleted key must NOT resurface as live in List.
// This locks the 03676a5 cross-partition fix against the large-gap case.
func TestScannerCrossPartitionTombstoneLargeGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	tomb := []byte("tombstone")

	key := []byte("/registry-kubebrain-apiserver-smoke-1782852222/apiextensions.k8s.io/customresourcedefinitions/v1.apiextensions.k8s.io")
	const rLive uint64 = 467364010600169522 // un-compacted live base object
	const rTomb uint64 = 467421706742398990 // latest = tombstone (huge gap above)

	objTomb := c.EncodeObjectKey(key, rTomb)
	objLive := c.EncodeObjectKey(key, rLive)
	withNul := func(b []byte) []byte { return append(append([]byte{}, b...), 0x00) }
	prefixTrim := func(b []byte, n int) []byte { return append([]byte{}, b[:len(b)-n]...) }
	magic := objTomb[:len(objTomb)-len(key)-9] // bytes before userKey
	bareDelim := append(append(append([]byte{}, magic...), key...), 0x24)

	cases := []struct {
		name  string
		split []byte
	}{
		{"decodable-at-tombstone", objTomb},
		{"decodable-at-live", objLive},
		{"nondecodable-after-live-nul", withNul(objLive)},
		{"nondecodable-mid-revision-suffix", prefixTrim(objTomb, 4)}, // cut the 8B rev in half
		{"bare-userkey-delimiter", bareDelim},                        // {userKey}$ , no revision
		{"boundary-no-split-byte", append(append([]byte{}, magic...), key...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := imemkv.NewKvStorage()
			defer kv.Close()
			b := kv.BeginBatchWrite()
			b.Put(c.EncodeObjectKey(key, 0), append(beU64(rTomb), 0), 0) // deleted index -> {rTomb}{del}
			b.Put(objLive, []byte("live-value"), 0)                      // older live object
			b.Put(objTomb, tomb, 0)                                      // latest = tombstone
			require.NoError(t, b.Commit(context.Background()))

			st := &splitStore{KvStorage: kv, splits: [][]byte{tc.split}}
			sc := NewScanner(st, c, Config{CompactKey: []byte("/compact"), Tombstone: tomb}, m)

			start := c.EncodeObjectKey(key, 0)
			end := c.EncodeObjectKey(append(append([]byte{}, key...), 0xff), 0)
			kvs, err := sc.Range(context.Background(), start, end, 1000, 0)
			require.NoError(t, err)
			for _, got := range kvs {
				if bytes.Equal(got.Key, key) {
					t.Fatalf("DELETED key resurfaced as live (rev=%d) with partition split=%x", got.Revision, tc.split)
				}
			}
		})
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
	c := coder.DefaultKeyspace().NewCoder()
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

// A '$' inside an arbitrary etcd key is indistinguishable from the legacy
// object-key delimiter to RevisionBoundaryForBorder's conservative first-byte
// search. For a narrow scan, that conservative boundary can sort before the
// requested start; partition adjustment must never broaden the scan and leak a
// neighboring lower key.
func TestScannerDollarKeyNarrowRangeDoesNotScanBeforeStartAcrossPartition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	tomb := []byte("tombstone")
	kv := imemkv.NewKvStorage()
	defer kv.Close()

	lower := []byte("/registry/items/a")
	target := []byte("/registry/items/a$target")
	const liveRevision uint64 = 100
	const deleteRevision uint64 = 200
	b := kv.BeginBatchWrite()
	b.Put(c.EncodeObjectKey(lower, liveRevision), []byte("must-not-leak"), 0)
	b.Put(c.EncodeObjectKey(target, 0), append(beU64(deleteRevision), 0), 0)
	b.Put(c.EncodeObjectKey(target, liveRevision), []byte("deleted"), 0)
	b.Put(c.EncodeObjectKey(target, deleteRevision), tomb, 0)
	require.NoError(t, b.Commit(context.Background()))

	// Force a region boundary at the target's tombstone. The old first-'$'
	// normalization turns it into EncodeObjectKey(lower, 0), before scanStart.
	st := &splitStore{KvStorage: kv, splits: [][]byte{c.EncodeObjectKey(target, deleteRevision)}}
	sc := NewScanner(st, c, Config{CompactKey: []byte("/compact"), Tombstone: tomb}, m)
	scanStart := c.EncodeObjectKey(target, 0)
	scanEnd := c.EncodeObjectKey(append(append([]byte(nil), target...), 0xff), 0)
	kvs, err := sc.Range(context.Background(), scanStart, scanEnd, 1000, 0)
	require.NoError(t, err)
	require.Empty(t, kvs, "narrow target range must neither resurrect the tombstone nor leak the lower key")
}

// An extension key can split two physical versions of a shorter key. Full
// compaction must compare versions by decoded user key, not only adjacent rows,
// or the shorter key's superseded version is stranded forever.
func TestScannerCompactDollarExtensionDoesNotStrandShorterVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	tomb := []byte("tombstone")
	kv := imemkv.NewKvStorage()
	defer kv.Close()

	shortKey := []byte("/registry/items/a")
	const firstRevision = uint64(0x1800000000000100)
	const foreignBoundary = uint64(0x2800000000000000)
	const latestRevision = uint64(0x3800000000000100)
	foreignSuffix := make([]byte, 8)
	binary.BigEndian.PutUint64(foreignSuffix, foreignBoundary)
	foreignKey := append(append(append([]byte(nil), shortKey...), '$'), foreignSuffix...)
	foreignKey = append(foreignKey, 'x')

	oldObject := c.EncodeObjectKey(shortKey, firstRevision)
	foreignObject := c.EncodeObjectKey(foreignKey, foreignBoundary)
	latestObject := c.EncodeObjectKey(shortKey, latestRevision)
	batch := kv.BeginBatchWrite()
	batch.Put(oldObject, []byte("short-v1"), 0)
	batch.Put(foreignObject, []byte("foreign-v1"), 0)
	batch.Put(latestObject, []byte("short-v2"), 0)
	require.NoError(t, batch.Commit(context.Background()))

	sc := NewScanner(kv, c, Config{CompactKey: []byte("/compact"), Tombstone: tomb}, m)
	borders := [][]byte{coder.DefaultKeyspace().ObjectKeyspaceStart(), coder.DefaultKeyspace().ObjectKeyspaceEnd()}
	require.NoError(t, sc.Compact(context.Background(), borders, latestRevision))

	_, err := kv.Get(context.Background(), oldObject)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	got, err := kv.Get(context.Background(), latestObject)
	require.NoError(t, err)
	require.Equal(t, []byte("short-v2"), got)
	got, err = kv.Get(context.Background(), foreignObject)
	require.NoError(t, err)
	require.Equal(t, []byte("foreign-v1"), got)
}

// A DELETE at exactly the compact watermark is still watchable in etcd. Keep
// its tombstone and immediate predecessor for event/PrevKV reconstruction, but
// reclaim both as soon as a later compaction advances past that revision.
func TestScannerCompactRetainsBoundaryDeleteHistoryUntilNextRevision(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	tomb := []byte("tombstone")
	kv := imemkv.NewKvStorage()
	defer kv.Close()

	key := []byte("/registry/configmaps/default/deleted")
	const liveRevision uint64 = 100
	const deleteRevision uint64 = 200
	liveKey := c.EncodeObjectKey(key, liveRevision)
	deleteKey := c.EncodeObjectKey(key, deleteRevision)
	b := kv.BeginBatchWrite()
	b.Put(c.EncodeObjectKey(key, 0), append(beU64(deleteRevision), 0), 0)
	b.Put(liveKey, []byte("previous-value"), 0)
	b.Put(deleteKey, tomb, 0)
	require.NoError(t, b.Commit(context.Background()))

	sc := NewScanner(kv, c, Config{CompactKey: []byte("/compact"), Tombstone: tomb}, m)
	borders := [][]byte{coder.DefaultKeyspace().ObjectKeyspaceStart(), coder.DefaultKeyspace().ObjectKeyspaceEnd()}
	require.NoError(t, sc.Compact(context.Background(), borders, deleteRevision))

	got, err := kv.Get(context.Background(), liveKey)
	require.NoError(t, err)
	require.Equal(t, []byte("previous-value"), got)
	got, err = kv.Get(context.Background(), deleteKey)
	require.NoError(t, err)
	require.Equal(t, tomb, got)

	// Once the watermark moves beyond the DELETE, revision deleteRevision is no
	// longer observable and both retained object versions become collectible.
	require.NoError(t, sc.Compact(context.Background(), borders, deleteRevision+1))
	got, err = kv.Get(context.Background(), liveKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Empty(t, got)
	got, err = kv.Get(context.Background(), deleteKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Empty(t, got)
}

// TestScannerCompactBatchesLargeTombstoneBacklog covers the #66 batched GC path:
// a backlog whose delete count spans several compactDeleteBatchSize flushes (plus
// a final partial one) must reclaim EVERY superseded version, tombstone object,
// and revision-key tombstone — nothing stranded at a batch boundary.
func TestScannerCompactBatchesLargeTombstoneBacklog(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	tomb := []byte("tombstone")

	kv := imemkv.NewKvStorage()
	defer kv.Close()

	// n well above compactDeleteBatchSize so deletes span multiple batched flushes;
	// each deleted key contributes 3 GC'able entries (deleted index + older live +
	// tombstone), so total deletes are ~3n.
	const n = 300
	const r1 uint64 = 100
	const r2 uint64 = 200
	require.Greater(t, n, compactDeleteBatchSize, "n must exceed one batch to exercise flush boundaries")

	b := kv.BeginBatchWrite()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("/registry/pods/default/p%05d", i))
		b.Put(c.EncodeObjectKey(key, 0), append(beU64(r2), 0), 0) // deleted index {r2}{del}
		b.Put(c.EncodeObjectKey(key, r1), []byte("live"), 0)      // older live object
		b.Put(c.EncodeObjectKey(key, r2), tomb, 0)                // latest = tombstone
	}
	require.NoError(t, b.Commit(context.Background()))

	sc := NewScanner(kv, c, Config{CompactKey: []byte("/compact"), Tombstone: tomb}, m)
	borders := [][]byte{coder.DefaultKeyspace().ObjectKeyspaceStart(), coder.DefaultKeyspace().ObjectKeyspaceEnd()}
	require.NoError(t, sc.Compact(context.Background(), borders, 1000))

	// A List over the whole keyspace returns nothing...
	kvs, err := sc.Range(context.Background(), coder.DefaultKeyspace().ObjectKeyspaceStart(), coder.DefaultKeyspace().ObjectKeyspaceEnd(), 100000, 0)
	require.NoError(t, err)
	require.Empty(t, kvs, "all deleted keys must be GC'd across batch boundaries")

	// ...and the raw store holds no object versions.
	it, err := kv.Iter(context.Background(), coder.DefaultKeyspace().ObjectKeyspaceStart(), coder.DefaultKeyspace().ObjectKeyspaceEnd(), 0, 0)
	require.NoError(t, err)
	defer it.Close()
	remaining := 0
	for it.Next(context.Background()) == nil {
		remaining++
	}
	require.Zero(t, remaining, "raw store must hold no object versions after batched compaction")
}

func TestScannerSkipsInternalStorageRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	ks := coder.DefaultKeyspace()
	c := ks.NewCoder()
	tomb := []byte("tombstone")
	kv := imemkv.NewKvStorage()
	defer kv.Close()

	userKey := []byte("/registry/pods/default/p1")
	const revision uint64 = 42
	b := kv.BeginBatchWrite()
	b.Put(c.EncodeObjectKey(userKey, revision), []byte("value"), 0)
	b.Put(ks.EncodeEventLogKey(revision, userKey), []byte("event"), 0)
	b.Put(ks.ElogMetaStartKey(), []byte("watermark"), 0)
	b.Put(ks.EncodeInternalKey([]byte("lease/meta")), []byte("metadata"), 0)
	require.NoError(t, b.Commit(context.Background()))

	var classified int32
	classify := func(key []byte) bool {
		if !ks.IsInternalStorageKey(key) {
			return false
		}
		atomic.AddInt32(&classified, 1)
		return true
	}
	sc := NewScanner(kv, c, Config{
		CompactKey:           []byte("/compact"),
		Tombstone:            tomb,
		IsInternalStorageKey: classify,
	}, m)

	kvs, err := sc.Range(
		context.Background(),
		ks.ObjectKeyspaceStart(),
		ks.ObjectKeyspaceEnd(),
		100,
		0,
	)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, userKey, kvs[0].Key)
	require.Equal(t, []byte("value"), kvs[0].Value)
	require.Equal(t, revision, kvs[0].Revision)
	require.EqualValues(t, 3, atomic.LoadInt32(&classified))

	unknownMalformed := append(ks.ObjectKeyspaceStart(), []byte("\x00unknown")...)
	require.False(t, classify(unknownMalformed), "unknown malformed rows must remain observable as decode errors")
}

// TestRangeStreamKeysOnlyDropsValues pins the review-51 failover-spike fix: the
// count-index rebuild consumes only key+revision, so RangeStream(keysOnly=true)
// must emit every key with its revision but a nil value — the value-carrying
// stream buffered ~300k large objects at once on a fresh leader's rebuild.
func TestRangeStreamKeysOnlyDropsValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()

	kv := imemkv.NewKvStorage()
	defer kv.Close()

	const n = 6
	const rev uint64 = 100
	want := map[string]string{}
	b := kv.BeginBatchWrite()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("/registry/configmaps/c%02d", i)
		val := fmt.Sprintf("value-%02d-payload", i)
		b.Put(c.EncodeObjectKey([]byte(key), rev), []byte(val), 0)
		want[key] = val
	}
	require.NoError(t, b.Commit(context.Background()))

	sc := NewScanner(kv, c, Config{CompactKey: []byte("/compact"), Tombstone: []byte("tomb")}, m)
	start, end := coder.DefaultKeyspace().ObjectKeyspaceStart(), coder.DefaultKeyspace().ObjectKeyspaceEnd()

	collect := func(keysOnly bool) map[string]string {
		got := map[string]string{}
		for resp := range sc.RangeStream(context.Background(), start, end, 1000, keysOnly) {
			require.Empty(t, resp.Err)
			for _, kv := range resp.RangeResponse.Kvs {
				got[string(kv.Key)] = string(kv.Value)
			}
		}
		return got
	}

	// keysOnly=false: full values present (the range-read path).
	withVals := collect(false)
	require.Equal(t, want, withVals, "values must be present when keysOnly=false")

	// keysOnly=true: same key set (so the rebuild count is exact), values dropped.
	keysOnlyGot := collect(true)
	require.Len(t, keysOnlyGot, n, "all keys must still be emitted for an exact count")
	for k := range want {
		v, ok := keysOnlyGot[k]
		require.Truef(t, ok, "key %s must be emitted in keysOnly mode", k)
		require.Emptyf(t, v, "value must be dropped when keysOnly=true (key %s)", k)
	}
}

type partitionedTestStorage struct {
	storage.KvStorage
	partitions []storage.Partition
	delayStart []byte
}

func (s *partitionedTestStorage) GetPartitions(context.Context, []byte, []byte) ([]storage.Partition, error) {
	return append([]storage.Partition(nil), s.partitions...), nil
}

func (s *partitionedTestStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	it, err := s.KvStorage.Iter(ctx, start, end, timestamp, limit)
	if err != nil || !bytes.Equal(start, s.delayStart) {
		return it, err
	}
	return &delayedFirstIter{Iter: it, delay: 100 * time.Millisecond}, nil
}

type delayedFirstIter struct {
	storage.Iter
	once  sync.Once
	delay time.Duration
}

func (i *delayedFirstIter) Next(ctx context.Context) error {
	i.once.Do(func() {
		select {
		case <-time.After(i.delay):
		case <-ctx.Done():
		}
	})
	return i.Iter.Next(ctx)
}

func TestRangeStreamPreservesKeyOrderAcrossPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	kv := imemkv.NewKvStorage()
	defer kv.Close()

	const n = 700
	b := kv.BeginBatchWrite()
	for idx := 0; idx < n; idx++ {
		key := []byte(fmt.Sprintf("/ordered/%04d", idx))
		b.Put(c.EncodeObjectKey(key, 100), []byte("v"), 0)
	}
	require.NoError(t, b.Commit(context.Background()))
	start := coder.DefaultKeyspace().ObjectKeyspaceStart()
	end := coder.DefaultKeyspace().ObjectKeyspaceEnd()
	split := c.EncodeObjectKey([]byte("/ordered/0350"), 0)
	store := &partitionedTestStorage{
		KvStorage: kv,
		partitions: []storage.Partition{
			{Start: start, End: split},
			{Start: split, End: end},
		},
		delayStart: start,
	}
	sc := NewScanner(store, c, Config{CompactKey: []byte("/compact"), Tombstone: []byte("tomb")}, m)

	var got [][]byte
	for resp := range sc.RangeStream(context.Background(), start, end, 1000, false) {
		require.Empty(t, resp.Err)
		for _, kv := range resp.RangeResponse.Kvs {
			got = append(got, append([]byte(nil), kv.Key...))
		}
	}
	require.Len(t, got, n)
	for idx := 1; idx < len(got); idx++ {
		require.Less(t, bytes.Compare(got[idx-1], got[idx]), 0,
			"stream keys must be strictly ascending at index %d: %q then %q", idx, got[idx-1], got[idx])
	}
}

func TestRangeStreamMorePartitionsThanGlobalWorkerLimitDoesNotDeadlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	c := coder.DefaultKeyspace().NewCoder()
	kv := imemkv.NewKvStorage()
	defer kv.Close()

	const (
		partitionCount = globalScanWorkers + 2
		keysPerPart    = rangeStreamBatch + 1 // Fill buffer, then block on flush.
	)
	b := kv.BeginBatchWrite()
	for part := 0; part < partitionCount; part++ {
		for idx := 0; idx < keysPerPart; idx++ {
			key := []byte(fmt.Sprintf("/many/%02d/%04d", part, idx))
			b.Put(c.EncodeObjectKey(key, 100), []byte("v"), 0)
		}
	}
	require.NoError(t, b.Commit(context.Background()))

	start := coder.DefaultKeyspace().ObjectKeyspaceStart()
	end := coder.DefaultKeyspace().ObjectKeyspaceEnd()
	partitions := make([]storage.Partition, partitionCount)
	borders := make([][]byte, partitionCount+1)
	borders[0], borders[partitionCount] = start, end
	for part := 1; part < partitionCount; part++ {
		borders[part] = c.EncodeObjectKey([]byte(fmt.Sprintf("/many/%02d/", part)), 0)
	}
	for part := range partitions {
		partitions[part] = storage.Partition{Start: borders[part], End: borders[part+1]}
	}
	store := &partitionedTestStorage{KvStorage: kv, partitions: partitions, delayStart: start}
	sc := NewScanner(store, c, Config{CompactKey: []byte("/compact"), Tombstone: []byte("tomb")}, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	count := 0
	for resp := range sc.RangeStream(ctx, start, end, 1000, false) {
		require.Empty(t, resp.Err)
		count += len(resp.RangeResponse.Kvs)
	}
	require.NoError(t, ctx.Err(), "ordered stream deadlocked while later partitions held every worker slot")
	require.Equal(t, partitionCount*keysPerPart, count)
}

// TestStreamReceiverNotRetriableAfterEmit locks the k8s-1.37-review fix: a
// per-partition streaming fork that has pushed a chunk into the channel must
// refuse retry — re-scanning the partition would re-send those keys and break
// RangeStream's disjoint-chunks contract.
func TestStreamReceiverNotRetriableAfterEmit(t *testing.T) {
	stream := make(chan *proto.StreamRangeResponse, 16)
	root := newStreamReceiver(7, stream)
	require.True(t, root.retriable())

	// Batch-full emit path.
	f1 := root.fork().(*streamResultReceiver)
	require.True(t, f1.retriable())
	for i := 0; i < rangeStreamBatch; i++ {
		f1.append([]byte("k"), []byte("v"), 1)
	}
	require.False(t, f1.retriable(), "fork must be non-retriable once a chunk is emitted")
	require.True(t, root.retriable(), "sibling/parent receivers are unaffected")
	f1.reset()
	require.False(t, f1.retriable(), "reset clears the pending batch, not the emitted mark")

	// Flush emit path.
	f2 := root.fork().(*streamResultReceiver)
	f2.append([]byte("k"), []byte("v"), 1)
	require.True(t, f2.retriable(), "buffered-only data is recallable via reset — still retriable")
	f2.flush()
	require.False(t, f2.retriable())
}

// TestStreamReceiverByteAdaptiveChunking locks the dual flush threshold: big
// values must cut chunks by accumulated bytes (etcd sizes RangeStream chunks
// against MaxRequestBytes) so per-chunk memory is a constant, not
// 300×valueSize; small values still batch up to the key-count threshold.
func TestStreamReceiverByteAdaptiveChunking(t *testing.T) {
	stream := make(chan *proto.StreamRangeResponse, 1024)
	r := newStreamReceiver(7, stream)

	// 200KB values: the byte cap (1.5MiB) must trip every 8 appends, far below
	// the 300-key count threshold.
	big := make([]byte, 200*1024)
	const n = 40
	for i := 0; i < n; i++ {
		r.append([]byte("k"), big, 1)
	}
	r.flush()
	close(stream)

	keys, chunks := 0, 0
	for resp := range stream {
		chunks++
		keys += len(resp.RangeResponse.Kvs)
		require.LessOrEqual(t, len(resp.RangeResponse.Kvs), rangeStreamBatch)
		bytes := 0
		for _, kv := range resp.RangeResponse.Kvs {
			bytes += len(kv.Key) + len(kv.Value)
		}
		// A chunk may exceed the cap by at most the one value that tripped it.
		require.Less(t, bytes, rangeStreamBatchBytes+len(big)+1)
	}
	require.Equal(t, n, keys, "no key lost across chunk boundaries")
	require.Equal(t, 5, chunks, "40 x 200KB at a 1.5MiB cap = ceil(40/8) chunks")

	// Small values are untouched by the byte cap: exactly the count threshold.
	stream2 := make(chan *proto.StreamRangeResponse, 16)
	r2 := newStreamReceiver(7, stream2)
	for i := 0; i < rangeStreamBatch; i++ {
		r2.append([]byte("k"), []byte("v"), 1)
	}
	require.Len(t, stream2, 1, "count threshold still cuts at rangeStreamBatch")
}
