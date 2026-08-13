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

package scanner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilteredCountReceiverBoundsResetAndMerge(t *testing.T) {
	receiver := &filteredCountReceiver{start: []byte("$b"), end: []byte("%")}
	left := receiver.fork().(*filteredCountReceiver)
	right := receiver.fork().(*filteredCountReceiver)
	for _, key := range [][]byte{[]byte("$a"), []byte("$b"), []byte("$z"), []byte("%"), []byte("&")} {
		left.append(key, []byte("large-value-not-retained"), 1)
	}
	right.append([]byte("$c"), nil, 2)
	receiver.merge(left)
	receiver.merge(right)
	require.Equal(t, 3, receiver.count)

	left.reset()
	require.Zero(t, left.count)
	fromKey := &filteredCountReceiver{start: []byte("$z")}
	fromKey.append([]byte("$z"), nil, 1)
	fromKey.append([]byte{0xff}, nil, 1)
	require.Equal(t, 2, fromKey.count)
}

func TestFilteredResultReceiverBoundsResetAndMerge(t *testing.T) {
	receiver := &filteredResultReceiver{start: []byte("$b"), end: []byte("%")}
	left := receiver.fork().(*filteredResultReceiver)
	right := receiver.fork().(*filteredResultReceiver)
	left.append([]byte("$a"), []byte("outside"), 1)
	left.append([]byte("$b"), []byte("one"), 2)
	right.append([]byte("$c"), []byte("two"), 3)
	right.append([]byte("$d"), []byte("capped"), 4)
	receiver.merge(left)
	receiver.merge(right)
	require.Equal(t, []string{"$b", "$c", "$d"}, []string{string(receiver.result[0].Key), string(receiver.result[1].Key), string(receiver.result[2].Key)})

	right.reset()
	require.Empty(t, right.result)
}
