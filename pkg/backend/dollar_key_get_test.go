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
	"testing"

	"github.com/stretchr/testify/require"
)

// A legacy object key is {magic}{raw user key}${revision}. Therefore versions
// of "a$target" sort inside the reverse-scan interval historically used by an
// exact Get("a"). Exact lookup must skip those overlapping foreign versions.
func TestGetDollarExtensionDoesNotHideShorterKey(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()

	lower := []byte("/registry/items/a")
	target := []byte("/registry/items/a$target")

	results, _, err := s.backend.TxnApply(s.ctx, []TxnWriteOp{
		{Key: lower, Value: []byte("lower")},
		{Key: target, Value: []byte("target-v1")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	lowerRevision := results[0].Revision

	_, _, err = s.backend.TxnApply(s.ctx, []TxnWriteOp{
		{Key: target, Value: []byte("target-v2")},
	}, nil)
	require.NoError(t, err)
	_, _, err = s.backend.TxnApply(s.ctx, []TxnWriteOp{
		{Delete: true, Key: target},
	}, nil)
	require.NoError(t, err)

	current, err := s.backend.Get(s.ctx, newGetRequest(0, string(lower)))
	require.NoError(t, err)
	require.NotNil(t, current.Kv)
	require.Equal(t, lower, current.Kv.Key)
	require.Equal(t, []byte("lower"), current.Kv.Value)
	require.Equal(t, lowerRevision, current.Kv.Revision)

	historical, err := s.backend.Get(s.ctx, newGetRequest(lowerRevision, string(lower)))
	require.NoError(t, err)
	require.NotNil(t, historical.Kv)
	require.Equal(t, lower, historical.Kv.Key)
	require.Equal(t, []byte("lower"), historical.Kv.Value)
	require.Equal(t, lowerRevision, historical.Kv.Revision)
}
