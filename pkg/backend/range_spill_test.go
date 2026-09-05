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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExternalKeySorterUsesExplicitWorkspaceInsteadOfProcessTemp(t *testing.T) {
	processTemp := t.TempDir()
	spillRoot := t.TempDir()
	t.Setenv("TMPDIR", processTemp)

	sorter, err := newExternalKeySorter(externalKeySorterConfig{Root: spillRoot})
	require.NoError(t, err)
	directory := sorter.dir
	require.Equal(t, spillRoot, filepath.Dir(directory))
	require.NotEqual(t, processTemp, filepath.Dir(directory))
	require.NoError(t, sorter.Close())
}

func TestExternalKeySorterEnforcesPeakDiskQuotaBeforeMerge(t *testing.T) {
	sorter, err := newExternalKeySorter(externalKeySorterConfig{
		Root:     t.TempDir(),
		MaxBytes: 16 << 10,
	})
	require.NoError(t, err)
	directory := sorter.dir
	defer func() {
		require.NoError(t, sorter.Close())
		_, statErr := os.Stat(directory)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}()

	// Each 300-key run fits by itself and both input runs fit concurrently.
	// The merge would need an output run alongside them and must be rejected
	// before it can consume more than the configured workspace budget.
	for index := 599; index >= 0; index-- {
		require.NoError(t, sorter.Add(context.Background(), []byte(fmt.Sprintf("quota-key/%06d", index))))
	}
	_, err = sorter.Finish(context.Background())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.ErrorContains(t, err, "range ordering spill quota")
	entries, readErr := os.ReadDir(directory)
	require.NoError(t, readErr)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), ".partial")
	}
}

func TestExternalKeySorterCancellationBeforeSingleRunFlush(t *testing.T) {
	spillRoot := t.TempDir()
	sorter, err := newExternalKeySorter(externalKeySorterConfig{Root: spillRoot})
	require.NoError(t, err)
	directory := sorter.dir
	require.NoError(t, sorter.Add(context.Background(), []byte("buffered-key")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = sorter.Finish(ctx)
	require.ErrorIs(t, err, context.Canceled)
	entries, readErr := os.ReadDir(directory)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a canceled single-run flush must not write a spill file")
	require.NoError(t, sorter.Close())
	_, err = os.Stat(directory)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestValidateRangeStreamSpillDirProbesAndCleansWorkspace(t *testing.T) {
	spillRoot := t.TempDir()
	stale := filepath.Join(spillRoot, ".kubebrain-range-order-stale")
	require.NoError(t, os.Mkdir(stale, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "run"), []byte("stale"), 0o600))
	foreign := filepath.Join(spillRoot, "operator-owned")
	require.NoError(t, os.WriteFile(foreign, []byte("keep"), 0o600))
	require.NoError(t, ValidateRangeStreamSpillDir(spillRoot))
	entries, err := os.ReadDir(spillRoot)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "operator-owned", entries[0].Name())
	_, err = os.Stat(stale)
	require.ErrorIs(t, err, os.ErrNotExist)

	notDirectory := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notDirectory, []byte("not a directory"), 0o600))
	err = ValidateRangeStreamSpillDir(notDirectory)
	require.ErrorContains(t, err, "validate range-stream spill directory")
}

func TestExternalKeySorterMultiPassOrdersDeduplicatesAndSupportsLongKeys(t *testing.T) {
	spillRoot := t.TempDir()
	t.Setenv("TMPDIR", spillRoot)
	sorter, err := newExternalKeySorter(externalKeySorterConfig{})
	require.NoError(t, err)
	directory := sorter.dir

	// More than mergeFan*runKeys forces at least two merge passes. Reverse
	// insertion order and periodic duplicates ensure neither input order nor run
	// boundaries leak into the result.
	want := make([][]byte, 0, decodedRangeSpillMergeFan*decodedRangeSpillRunKeys+17)
	for index := 0; index < cap(want); index++ {
		want = append(want, []byte(fmt.Sprintf("key/%06d", index)))
	}
	want[len(want)-1] = append([]byte("key/zzzz/"), bytes.Repeat([]byte{'x'}, 40<<10)...)
	sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i], want[j]) < 0 })
	for index := len(want) - 1; index >= 0; index-- {
		require.NoError(t, sorter.Add(context.Background(), want[index]))
		if index%97 == 0 {
			require.NoError(t, sorter.Add(context.Background(), want[index]))
		}
	}
	finalRun, err := sorter.Finish(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, finalRun)

	reader, err := openKeyRun(finalRun)
	require.NoError(t, err)
	var got [][]byte
	for {
		key, readErr := reader.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
		got = append(got, key)
	}
	require.NoError(t, reader.Close())
	require.Equal(t, want, got)
	require.NoError(t, sorter.Close())
	_, err = os.Stat(directory)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestExternalKeySorterBoundsRunRecordsWithoutCappingConfiguredKeySize(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	sorter, err := newExternalKeySorter(externalKeySorterConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sorter.Close()) })

	largeKey := bytes.Repeat([]byte{'x'}, decodedRangeSpillRunBytes+1)
	require.NoError(t, sorter.Add(context.Background(), largeKey))
	largeRun, err := sorter.Finish(context.Background())
	require.NoError(t, err)
	largeReader, err := openKeyRun(largeRun)
	require.NoError(t, err)
	got, err := largeReader.Next()
	require.NoError(t, err)
	require.Equal(t, largeKey, got)
	_, err = largeReader.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, largeReader.Close())

	path := filepath.Join(sorter.dir, "malformed-run")
	var prefix [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(prefix[:], uint64(decodedRangeSpillRunBytes+1))
	require.NoError(t, os.WriteFile(path, append(prefix[:n], make([]byte, sha256.Size)...), 0o600))
	reader, err := openKeyRun(path)
	require.NoError(t, err)
	_, err = reader.Next()
	require.ErrorContains(t, err, "exceeds remaining run bytes")
	require.NoError(t, reader.Close())
}

func TestKeyRunReaderRejectsChecksumPreservingOrderTamper(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run")
	writer, err := createKeyRun(path)
	require.NoError(t, err)
	require.NoError(t, writer.Write([]byte("aa")))
	require.NoError(t, writer.Write([]byte("cc")))
	require.NoError(t, writer.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte('a'), contents[2])
	contents[2] = 'b'
	require.NoError(t, os.WriteFile(path, contents, 0o600))

	reader, err := openKeyRun(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	key, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("ab"), key)
	key, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("cc"), key)
	_, err = reader.Next()
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestKeyRunReaderRejectsEmptyOrNonIncreasingRecords(t *testing.T) {
	for _, test := range []struct {
		name string
		keys [][]byte
		want string
	}{
		{name: "empty", keys: [][]byte{nil}, want: "empty key"},
		{name: "descending", keys: [][]byte{[]byte("b"), []byte("a")}, want: "not strictly increasing"},
		{name: "duplicate", keys: [][]byte{[]byte("a"), []byte("a")}, want: "not strictly increasing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "run")
			writer, err := createKeyRun(path)
			require.NoError(t, err)
			for _, key := range test.keys {
				require.NoError(t, writer.Write(key))
			}
			require.NoError(t, writer.Close())

			reader, err := openKeyRun(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			for range len(test.keys) - 1 {
				_, err = reader.Next()
				require.NoError(t, err)
			}
			_, err = reader.Next()
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestExternalKeySorterCancellationCleansPartialMerge(t *testing.T) {
	spillRoot := t.TempDir()
	t.Setenv("TMPDIR", spillRoot)
	sorter, err := newExternalKeySorter(externalKeySorterConfig{})
	require.NoError(t, err)
	directory := sorter.dir
	for index := decodedRangeSpillRunKeys * 2; index >= 0; index-- {
		require.NoError(t, sorter.Add(context.Background(), []byte(fmt.Sprintf("key/%06d", index))))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = sorter.Finish(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, sorter.Close())
	_, err = os.Stat(directory)
	require.ErrorIs(t, err, os.ErrNotExist)
}
