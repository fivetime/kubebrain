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
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalKeySorterMultiPassOrdersDeduplicatesAndSupportsLongKeys(t *testing.T) {
	spillRoot := t.TempDir()
	t.Setenv("TMPDIR", spillRoot)
	sorter, err := newExternalKeySorter()
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
		require.NoError(t, sorter.Add(want[index]))
		if index%97 == 0 {
			require.NoError(t, sorter.Add(want[index]))
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

func TestExternalKeySorterCancellationCleansPartialMerge(t *testing.T) {
	spillRoot := t.TempDir()
	t.Setenv("TMPDIR", spillRoot)
	sorter, err := newExternalKeySorter()
	require.NoError(t, err)
	directory := sorter.dir
	for index := decodedRangeSpillRunKeys * 2; index >= 0; index-- {
		require.NoError(t, sorter.Add([]byte(fmt.Sprintf("key/%06d", index))))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = sorter.Finish(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, sorter.Close())
	_, err = os.Stat(directory)
	require.ErrorIs(t, err, os.ErrNotExist)
}
