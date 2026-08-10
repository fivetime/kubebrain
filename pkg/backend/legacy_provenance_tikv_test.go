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

package backend

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

// TestLegacyCurrentLeaseProvenanceTiKV proves the A4313 compound CAS on an
// actual TiKV transaction. CI without a cluster skips it; release validation
// can point KUBEBRAIN_TIKV_PD at an isolated PD endpoint. A unique named
// keyspace prevents cross-test reads and is physically removed before close.
func TestLegacyCurrentLeaseProvenanceTiKV(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PD")
	if pd == "" {
		t.Skip("set KUBEBRAIN_TIKV_PD=<pd-addrs> to run the TiKV provenance transaction test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv, err := tikv.NewKvStorage(strings.Split(pd, ","), 1, tikv.Security{})
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	keyspace := "legacy-provenance-" + time.Now().UTC().Format("20060102t150405000000000")
	b := NewBackend(kv, Config{
		Prefix: "/integration/legacy-provenance", Keyspace: keyspace,
		Identity: "legacy-provenance-test", EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		iter, iterErr := kv.Iter(cleanupCtx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), 0, 0)
		require.NoError(t, iterErr)
		var keys [][]byte
		for {
			iterErr = iter.Next(cleanupCtx)
			if iterErr == io.EOF {
				break
			}
			require.NoError(t, iterErr)
			keys = append(keys, append([]byte(nil), iter.Key()...))
		}
		require.NoError(t, iter.Close())
		if len(keys) != 0 {
			batch := kv.BeginBatchWrite()
			for _, key := range keys {
				batch.Del(key)
			}
			require.NoError(t, batch.Commit(cleanupCtx))
		}
		require.NoError(t, b.Close())
	}()

	makeLegacy := func(key, value []byte) uint64 {
		created, createErr := b.Create(ctx, &proto.CreateRequest{Key: key, Value: value})
		require.NoError(t, createErr)
		waitCommitted(t, b, created.Header.Revision)
		v1 := make([]byte, valueMetaHeaderLen+len(value))
		copy(v1, valueMetaMagic)
		binary.BigEndian.PutUint64(v1[4:], created.Header.Revision)
		binary.BigEndian.PutUint64(v1[12:], 1)
		copy(v1[valueMetaHeaderLen:], value)
		batch := kv.BeginBatchWrite()
		batch.CAS(b.coder.EncodeObjectKey(key, created.Header.Revision), v1,
			encodeValueWithMeta(value, EtcdMetadata{CreateRevision: created.Header.Revision, Version: 1}), 0)
		require.NoError(t, batch.Commit(ctx))
		return created.Header.Revision
	}

	updateKey := []byte("/integration/legacy-provenance/update")
	updateOldRevision := makeLegacy(updateKey, []byte("leased-v1"))
	_, updateRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: updateKey, Value: []byte("unleased-v2"), PrevLeaseKnown: true, PrevLease: 4313,
	}}, nil)
	require.NoError(t, err)
	waitCommitted(t, b, updateRevision)
	oldUpdated, err := kv.Get(ctx, b.coder.EncodeObjectKey(updateKey, updateOldRevision))
	require.NoError(t, err)
	meta, raw, ok, err := DecodeInlineValueChecked(oldUpdated)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 4313, meta.Lease)
	require.Equal(t, []byte("leased-v1"), raw)

	deleteKey := []byte("/integration/legacy-provenance/delete")
	deleteOldRevision := makeLegacy(deleteKey, []byte("leased-before-delete"))
	deleted, err := b.Delete(WithPreviousLease(ctx, 4314), &proto.DeleteRequest{
		Key: deleteKey, Revision: deleteOldRevision,
	})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)
	waitCommitted(t, b, deleted.Header.Revision)
	oldDeleted, err := kv.Get(ctx, b.coder.EncodeObjectKey(deleteKey, deleteOldRevision))
	require.NoError(t, err)
	meta, raw, ok, err = DecodeInlineValueChecked(oldDeleted)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 4314, meta.Lease)
	require.Equal(t, []byte("leased-before-delete"), raw)
}
