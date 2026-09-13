package backend

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTxnOrphanRecoveryRejectsActiveCorrupt(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "put"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			key := []byte(prefix + "/txn-orphan-active-corrupt")
			_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("old")}}, nil)
			require.NoError(t, err)
			index := b.coder.EncodeRevisionKey(key)
			object := b.coder.EncodeObjectKey(key, revision)
			before, err := b.kv.Get(ctx, object)
			require.NoError(t, err)
			require.NoError(t, b.kv.Del(ctx, index))
			require.NoError(t, b.ArmCorrupt(ctx, 4505001))
			_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("new"), Delete: deleting}}, nil)
			require.ErrorIs(t, err, ErrCorruptAlarmActive)
			require.Equal(t, revision, b.GetCurrentRevision())
			_, err = b.kv.Get(ctx, index)
			require.ErrorIs(t, err, storage.ErrKeyNotFound, "blocked write must not heal its orphan")
			after, err := b.kv.Get(ctx, object)
			require.NoError(t, err)
			require.Equal(t, before, after)
			alarms, err := b.CorruptAlarms(ctx)
			require.NoError(t, err)
			require.Equal(t, []uint64{4505001}, alarms)
		})
	}
}
