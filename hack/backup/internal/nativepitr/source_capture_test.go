package nativepitr

import (
	"encoding/binary"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestInspectFencedSourceRevisionUsesObjectsAndDurableWatermark(t *testing.T) {
	for _, tc := range []struct {
		name    string
		durable uint64
		object  uint64
		want    uint64
	}{
		{name: "object ahead", durable: 7, object: 9, want: 9},
		{name: "compacted watermark ahead", durable: 11, object: 9, want: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memkv.NewKvStorage()
			defer store.Close()
			ks, err := coder.NewKeyspace("capture-a")
			require.NoError(t, err)
			watermark := make([]byte, 8)
			binary.BigEndian.PutUint64(watermark, tc.durable)
			batch := store.BeginBatchWrite()
			batch.Put(ks.EncodeInternalKey([]byte("revision/committed")), watermark, 0)
			batch.Put(ks.NewCoder().EncodeObjectKey([]byte("key"), tc.object), []byte("value"), 0)
			batch.Put(ks.EncodeInternalKey([]byte("other")), []byte("not-an-object"), 0)
			batch.Put(ks.EncodeEventLogKey(100, []byte("key")), []byte("event"), 0)
			require.NoError(t, batch.Commit(t.Context()))

			got, err := InspectFencedSourceRevision(t.Context(), store, "capture-a")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestInspectFencedSourceRevisionRejectsUnknownPhysicalEncoding(t *testing.T) {
	store := memkv.NewKvStorage()
	defer store.Close()
	ks, err := coder.NewKeyspace("capture-a")
	require.NoError(t, err)
	malformed := append(ks.ObjectKeyspaceStart(), []byte("malformed")...)
	batch := store.BeginBatchWrite()
	batch.Put(malformed, []byte("value"), 0)
	require.NoError(t, batch.Commit(t.Context()))

	_, err = InspectFencedSourceRevision(t.Context(), store, "capture-a")
	require.ErrorContains(t, err, "decode fenced source object key")
}
