package nativepitr

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestSourceCaptureReceiptsBindContinuousFenceWindow(t *testing.T) {
	task, _ := readyTask(t)
	witness := validWitness()
	fence, _, err := BuildSourceCaptureFenceReceipt(task, digest, digest, "capture-1", witness, witness.Revision, 125, 110, false)
	require.NoError(t, err)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	receipt, err := BuildSourceCaptureReceipt(task, digest, fence, digest, full, digest, 140, 111)
	require.NoError(t, err)
	require.True(t, receipt.ContinuousSourceExclusion)
	require.True(t, receipt.AllFenceKeysReopened)

	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)
	decoded, err := DecodeSourceCaptureReceipt(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, receipt, decoded)
	_, err = DecodeSourceCaptureReceipt(strings.NewReader(string(encoded) + `{}`))
	require.ErrorContains(t, err, "trailing JSON")
}

func TestSourceCaptureReceiptsRejectBrokenRevisionOrTSOChain(t *testing.T) {
	task, _ := readyTask(t)
	witness := validWitness()
	_, _, err := BuildSourceCaptureFenceReceipt(task, digest, digest, "capture-1", witness, witness.Revision+1, 125, 110, false)
	require.ErrorContains(t, err, "invalid source capture fence")

	fence, _, err := BuildSourceCaptureFenceReceipt(task, digest, digest, "capture-1", witness, witness.Revision, 125, 110, false)
	require.NoError(t, err)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	_, err = BuildSourceCaptureReceipt(task, digest, fence, digest, full, digest, 124, 111)
	require.ErrorContains(t, err, "continuous fenced snapshot")
}

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
