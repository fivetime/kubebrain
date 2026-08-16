package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type fakeProbe struct {
	pdID, txnID uint64
	stores      []*metapb.Store
	ts          uint64
	found       bool
	err         error
	closeErr    error
	closed      bool
}

func (p *fakeProbe) PDClusterID(context.Context) uint64 { return p.pdID }
func (p *fakeProbe) TxnClusterID() uint64               { return p.txnID }
func (p *fakeProbe) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return p.stores, p.err
}
func (p *fakeProbe) SnapshotTSAndFirstKey(context.Context) (uint64, bool, error) {
	return p.ts, p.found, p.err
}
func (p *fakeProbe) Close()            { p.closed = true }
func (p *fakeProbe) CloseError() error { return p.closeErr }

func goodProbe() *fakeProbe {
	return &fakeProbe{pdID: 22, txnID: 22, ts: 123, stores: []*metapb.Store{
		{Id: 9, Address: "tikv-b:20160", State: metapb.StoreState_Up},
		{Id: 3, Address: "tikv-a:20160", State: metapb.StoreState_Up},
		{Id: 1, Address: "old:20160", State: metapb.StoreState_Tombstone},
	}}
}

func TestInspectWritesBoundedSnapshotEmptyReceipt(t *testing.T) {
	p := goodProbe()
	var out bytes.Buffer
	require.NoError(t, inspect(context.Background(), p, []string{"pd-a:2379", "pd-b:2379"}, 1_700_000_000, &out))
	require.True(t, p.closed)
	var got nativepitr.TargetSnapshotEmptyReceipt
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	require.NoError(t, got.Validate())
	require.Equal(t, uint64(22), got.ClusterID)
	require.Equal(t, uint64(123), got.SnapshotTS)
	require.Len(t, got.Stores, 2)
	require.Equal(t, uint64(3), got.Stores[0].ID)
	require.False(t, got.HistoricalMVCCAbsenceProven)
	require.False(t, got.RawKVAbsenceProven)
}

func TestInspectFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(*fakeProbe)
		want string
	}{
		{"cluster mismatch", func(p *fakeProbe) { p.txnID = 23 }, "mismatch"},
		{"visible key", func(p *fakeProbe) { p.found = true }, "not empty"},
		{"scan failure", func(p *fakeProbe) { p.err = errors.New("unavailable") }, "unavailable"},
		{"no stores", func(p *fakeProbe) { p.stores = nil }, "no Up"},
		{"zero tso", func(p *fakeProbe) { p.ts = 0 }, "observation time"},
		{"probe close failure", func(p *fakeProbe) { p.closeErr = errors.New("close failed") }, "close failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := goodProbe()
			tc.edit(p)
			err := inspect(context.Background(), p, []string{"pd:2379"}, 1, &bytes.Buffer{})
			require.ErrorContains(t, err, tc.want)
			require.True(t, p.closed)
		})
	}
}

func TestValidateOptions(t *testing.T) {
	addrs, err := validateOptions(options{pdAddrs: "pd-b:2379,pd-a:2379", timeout: time.Second})
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, addrs)
	for _, o := range []options{
		{timeout: time.Second},
		{pdAddrs: "http://pd:2379", timeout: time.Second},
		{pdAddrs: "pd:2379,pd:2379", timeout: time.Second},
		{pdAddrs: "pd:2379"},
		{pdAddrs: "pd:2379", timeout: time.Second, cert: "cert"},
	} {
		_, err := validateOptions(o)
		require.Error(t, err)
	}
}
