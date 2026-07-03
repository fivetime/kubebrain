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

package tikv

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockTiKvIter is a minimal in-memory tiKvIterator for exercising iter border logic.
type mockTiKvIter struct {
	keys [][]byte
	vals [][]byte
	pos  int
}

func (m *mockTiKvIter) Valid() bool   { return m.pos < len(m.keys) }
func (m *mockTiKvIter) Key() []byte   { return m.keys[m.pos] }
func (m *mockTiKvIter) Value() []byte { return m.vals[m.pos] }
func (m *mockTiKvIter) Next() error   { m.pos++; return nil }
func (m *mockTiKvIter) Close()        {}

// TestReverseIterFirstKeyIsBorderChecked pins #25: the first position of a reverse
// iterator must be range-checked. IterReverse is not bounded by `end` (the lower
// bound), so the first key can already be at/below end; returning it unchecked
// leaked a key outside [end, start).
func TestReverseIterFirstKeyIsBorderChecked(t *testing.T) {
	// reverse range: end is the exclusive lower bound; checkBorder returns EOF when
	// key <= end.
	end := []byte("m")

	t.Run("first_key_out_of_range_is_EOF", func(t *testing.T) {
		// first (and only) key "a" <= end "m" -> out of range -> EOF, not returned.
		m := &mockTiKvIter{keys: [][]byte{[]byte("a")}, vals: [][]byte{[]byte("v")}}
		it := &iter{iter: m, reverse: true, end: end}
		require.Equal(t, io.EOF, it.Next(context.Background()),
			"first key at/below the lower bound must be EOF, not leaked")
	})

	t.Run("first_key_in_range_is_returned", func(t *testing.T) {
		// first key "z" > end "m" -> in range -> returned.
		m := &mockTiKvIter{keys: [][]byte{[]byte("z")}, vals: [][]byte{[]byte("v")}}
		it := &iter{iter: m, reverse: true, end: end}
		require.NoError(t, it.Next(context.Background()))
		require.Equal(t, []byte("z"), it.Key())
	})

	t.Run("empty_iter_is_EOF", func(t *testing.T) {
		m := &mockTiKvIter{}
		it := &iter{iter: m, reverse: true, end: end}
		require.Equal(t, io.EOF, it.Next(context.Background()))
	})
}
