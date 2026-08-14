// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLargeKeyRoundTripTiKV proves KubeBrain's patched client-go transports an
// etcd-sized physical key through an actual TiKV Put/Get/Iter byte-for-byte.
// Stock client-go v2.0.7 commits only len(key) mod 65536 bytes here.
func TestLargeKeyRoundTripTiKV(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PD")
	if pd == "" {
		t.Skip("set KUBEBRAIN_TIKV_PD=<pd-addrs> to run the TiKV large-key transport test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv, err := NewKvStorage(strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })

	prefix := []byte("\x58large-key-transport/")
	deletePrefix := func() {
		it, iterErr := kv.Iter(context.Background(), prefix, append(append([]byte(nil), prefix...), 0xff), 0, 0)
		require.NoError(t, iterErr)
		var found [][]byte
		for {
			iterErr = it.Next(context.Background())
			if iterErr == io.EOF {
				break
			}
			require.NoError(t, iterErr)
			found = append(found, append([]byte(nil), it.Key()...))
		}
		require.NoError(t, it.Close())
		if len(found) != 0 {
			cleanup := kv.BeginBatchWrite()
			for _, foundKey := range found {
				cleanup.Del(foundKey)
			}
			require.NoError(t, cleanup.Commit(context.Background()))
		}
	}
	deletePrefix()
	t.Cleanup(deletePrefix)

	key := append(append([]byte(nil), prefix...), bytes.Repeat([]byte{'z'}, 600<<10)...)
	key = append(key, '$')
	var revision [8]byte
	binary.BigEndian.PutUint64(revision[:], uint64(time.Now().UnixNano()))
	key = append(key, revision[:]...)
	value := []byte("large-key-round-trip")
	batch := kv.BeginBatchWrite()
	batch.Put(key, value, 0)
	require.NoError(t, batch.Commit(ctx))

	got, err := kv.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, got)

	it, err := kv.Iter(ctx, key, append(append([]byte(nil), key...), 0), 0, 0)
	require.NoError(t, err)
	require.NoError(t, it.Next(ctx))
	require.Equal(t, key, it.Key())
	require.Equal(t, value, it.Val())
	require.Equal(t, io.EOF, it.Next(ctx))
	require.NoError(t, it.Close())
}
