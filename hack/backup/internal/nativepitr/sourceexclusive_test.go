package nativepitr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/stretchr/testify/require"
)

type fakeSourceRangeProbe struct {
	pdID, txnID uint64
	foundAt     int
	calls       int
	err         error
	closed      bool
}

func (p *fakeSourceRangeProbe) PDClusterID(context.Context) uint64 { return p.pdID }
func (p *fakeSourceRangeProbe) TxnClusterID() uint64               { return p.txnID }
func (p *fakeSourceRangeProbe) HasVisibleKey(context.Context, uint64, []byte, []byte) (bool, error) {
	p.calls++
	return p.calls == p.foundAt, p.err
}
func (p *fakeSourceRangeProbe) Close() { p.closed = true }

func validSourceExclusive(t *testing.T) SourceRangeExclusiveReceipt {
	t.Helper()
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	coordStart, coordEnd := coordinationRange("tenant-a")
	return SourceRangeExclusiveReceipt{Format: SourceRangeExclusiveFormat, ClusterID: 11, PDAddrs: []string{"pd:2379"}, Keyspace: "tenant-a", StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), CoordinationStartKeyHex: hex.EncodeToString(coordStart), CoordinationEndKeyHex: hex.EncodeToString(coordEnd), CoordinationRangeExcluded: true, SnapshotTS: 120, FullSnapshotReceiptSHA256: digest, CheckedAtUnix: 2_000_000_000, ReadOnly: true}
}

func TestSourceRangeExclusiveReceiptIsStrictAndBounded(t *testing.T) {
	r := validSourceExclusive(t)
	require.NoError(t, r.Validate())
	b, err := json.Marshal(r)
	require.NoError(t, err)
	_, err = DecodeSourceRangeExclusive(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeSourceRangeExclusive(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
	r.OutsideVisibleKeyCount = 1
	require.ErrorContains(t, r.Validate(), "bounded")
	r = validSourceExclusive(t)
	r.HistoricalMVCCAbsenceProven = true
	require.ErrorContains(t, r.Validate(), "bounded")
	r = validSourceExclusive(t)
	r.CoordinationRangeExcluded = false
	require.ErrorContains(t, r.Validate(), "coordination")
}

func TestInspectSourceRangeExclusiveScansBothOutsideRanges(t *testing.T) {
	task, _ := readyTask(t)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full", fullMeta(t, task))
	require.NoError(t, err)
	p := &fakeSourceRangeProbe{pdID: task.ClusterID, txnID: task.ClusterID}
	receipt, err := InspectSourceRangeExclusive(context.Background(), p, full, digest, []string{"pd:2379"}, 1)
	require.NoError(t, err)
	require.Equal(t, 3, p.calls)
	require.True(t, p.closed)
	require.Equal(t, full.BackupTS, receipt.SnapshotTS)
	for _, foundAt := range []int{1, 2, 3} {
		p = &fakeSourceRangeProbe{pdID: task.ClusterID, txnID: task.ClusterID, foundAt: foundAt}
		_, err = InspectSourceRangeExclusive(context.Background(), p, full, digest, []string{"pd:2379"}, 1)
		require.ErrorContains(t, err, "outside")
		require.True(t, p.closed)
	}
	p = &fakeSourceRangeProbe{pdID: task.ClusterID, txnID: task.ClusterID + 1}
	_, err = InspectSourceRangeExclusive(context.Background(), p, full, digest, []string{"pd:2379"}, 1)
	require.ErrorContains(t, err, "mismatch")
}
