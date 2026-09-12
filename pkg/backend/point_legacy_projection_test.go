package backend

import (
	"testing"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/stretchr/testify/require"
)

func TestPointMetadataProjectionPreservesLegacyKeyPresence(t *testing.T) {
	for _, value := range []string{"legacy-value", ""} {
		t.Run("value="+value, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			key := []byte(prefix + "/legacy-point-projection")
			revision := b.GetCurrentRevision() + 1
			batch := b.kv.BeginBatchWrite()
			batch.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(revision), 0)
			batch.Put(b.coder.EncodeObjectKey(key, revision), []byte(value), 0)
			b.putEtcdMetadata(batch, key, revision, EtcdMetadata{CreateRevision: revision, Version: 1})
			require.NoError(t, batch.Commit(ctx))
			b.SetCurrentRevision(revision)
			full, err := b.Get(ctx, &proto.GetRequest{Key: key})
			require.NoError(t, err)
			require.NotNil(t, full.Kv)
			require.Equal(t, []byte(value), full.Kv.Value)
			projected, err := b.GetKeysOnly(ctx, &proto.GetRequest{Key: key})
			require.NoError(t, err)
			require.NotNil(t, projected.Kv, "discarding payload cannot discard an existing legacy key")
			require.Equal(t, key, projected.Kv.Key)
			require.Equal(t, revision, projected.Kv.Revision)
			require.Empty(t, projected.Kv.Value)
			require.Equal(t, full.Header, projected.Header)
			missing, err := b.GetKeysOnly(ctx, &proto.GetRequest{Key: append(append([]byte(nil), key...), []byte("/absent")...)})
			require.NoError(t, err)
			require.Nil(t, missing.Kv, "genuinely absent keys must remain absent")
		})
	}
}
