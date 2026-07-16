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

package coder

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// rangesDisjoint reports whether [aStart,aEnd) and [bStart,bEnd) do not overlap.
func rangesDisjoint(aStart, aEnd, bStart, bEnd []byte) bool {
	return bytes.Compare(aEnd, bStart) <= 0 || bytes.Compare(bEnd, aStart) <= 0
}

// TestKeyspacesAreDisjoint pins the #76 isolation invariant: the legacy
// keyspace and every named keyspace occupy pairwise-disjoint key ranges, so
// one tenant's whole-keyspace scans (GC, count-index rebuild, event log) can
// never reach another tenant's rows.
func TestKeyspacesAreDisjoint(t *testing.T) {
	def := DefaultKeyspace()
	a, err := NewKeyspace("cilium")
	require.NoError(t, err)
	b, err := NewKeyspace("k8s-prod")
	require.NoError(t, err)

	spaces := []*Keyspace{def, a, b}
	for i := range spaces {
		for j := range spaces {
			if i == j {
				continue
			}
			require.True(t, rangesDisjoint(
				spaces[i].ObjectKeyspaceStart(), spaces[i].ObjectKeyspaceEnd(),
				spaces[j].ObjectKeyspaceStart(), spaces[j].ObjectKeyspaceEnd()),
				"keyspace %q overlaps %q", spaces[i].Name(), spaces[j].Name())
		}
	}

	// Every key family a tenant writes must fall inside its own borders.
	for _, ks := range spaces {
		for _, key := range [][]byte{
			ks.NewCoder().EncodeObjectKey([]byte("/registry/pods/p"), 42),
			ks.EncodeEventLogKey(42, []byte("/registry/pods/p")),
			ks.ElogMetaStartKey(),
		} {
			require.True(t, bytes.Compare(key, ks.ObjectKeyspaceStart()) >= 0 &&
				bytes.Compare(key, ks.ObjectKeyspaceEnd()) < 0,
				"keyspace %q key %q outside its borders", ks.Name(), key)
		}
	}
}

// TestKeyspaceDeterministicAndLegacyStable pins that replicas configured with
// the same name independently derive identical magics, and that the default
// keyspace's bytes are exactly the pre-#76 constants (existing data stays
// readable).
func TestKeyspaceDeterministicAndLegacyStable(t *testing.T) {
	a1, err := NewKeyspace("tenant-a")
	require.NoError(t, err)
	a2, err := NewKeyspace("tenant-a")
	require.NoError(t, err)
	require.Equal(t, a1.ObjectKeyspaceStart(), a2.ObjectKeyspaceStart())
	require.Equal(t, a1.EncodeEventLogKey(7, []byte("k")), a2.EncodeEventLogKey(7, []byte("k")))

	require.Equal(t, []byte("\x57\xfb\x80\x8b"), DefaultKeyspace().ObjectKeyspaceStart())
	require.Equal(t,
		append([]byte("\x57\xfb\x80\x8b"), []byte("\x00elogmeta")...),
		DefaultKeyspace().ElogMetaStartKey())
}

func TestKeyspaceNameValidation(t *testing.T) {
	for _, bad := range []string{"UPPER", "has space", "-lead", "trail-", "a/b", string(make([]byte, 80))} {
		_, err := NewKeyspace(bad)
		require.Error(t, err, "name %q should be rejected", bad)
	}
	for _, good := range []string{"a", "cilium", "k8s-prod-1"} {
		_, err := NewKeyspace(good)
		require.NoError(t, err, "name %q should be accepted", good)
	}
}

// TestKeyspaceCoderRejectsForeignKeys pins that a tenant's decoder refuses
// another tenant's rows instead of misreading them.
func TestKeyspaceCoderRejectsForeignKeys(t *testing.T) {
	a, _ := NewKeyspace("tenant-a")
	b, _ := NewKeyspace("tenant-b")
	foreign := b.NewCoder().EncodeObjectKey([]byte("/registry/pods/p"), 42)
	_, _, err := a.NewCoder().Decode(foreign)
	require.Error(t, err)
	_, ok := a.NewCoder().RevisionBoundaryForBorder(foreign)
	require.False(t, ok)
	_, _, err = a.DecodeEventLogKey(b.EncodeEventLogKey(42, []byte("k")))
	require.Error(t, err)
}

func TestIsInternalStorageKey(t *testing.T) {
	ks, err := NewKeyspace("hash-test")
	require.NoError(t, err)

	require.True(t, ks.IsInternalStorageKey(ks.EncodeEventLogKey(42, []byte("/key"))))
	require.True(t, ks.IsInternalStorageKey(ks.ElogMetaStartKey()))
	require.True(t, ks.IsInternalStorageKey(ks.EncodeInternalKey([]byte("lease/meta"))))
	require.False(t, ks.IsInternalStorageKey(ks.NewCoder().EncodeObjectKey([]byte("/key"), 42)))
}
