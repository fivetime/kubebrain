package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/credentials"
)

type fakeGet int64

func (f fakeGet) GetCount() int64 { return int64(f) }

type fakePD struct {
	clusterID uint64
	stores    []*metapb.Store
	counts    map[string]int64
	getErr    error
	storesErr error
	closed    bool
}

func (f *fakePD) GetClusterID(context.Context) uint64 { return f.clusterID }
func (f *fakePD) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return f.stores, f.storesErr
}
func (f *fakePD) Get(_ context.Context, key []byte, _ ...pd.OpOption) (interfaceGetResponse, error) {
	return fakeGet(f.counts[string(key)]), f.getErr
}
func (f *fakePD) Close() { f.closed = true }

func TestValidateOptions(t *testing.T) {
	good := options{pdAddrs: "pd-b:2379,pd-a:2379", task: "kubebrain-a", timeout: 1}
	addrs, err := validateOptions(good)
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, addrs)
	for _, tc := range []options{
		{task: "x", timeout: 1},
		{pdAddrs: "pd:2379", timeout: 1},
		{pdAddrs: "pd:2379", task: "a/b", timeout: 1},
		{pdAddrs: "pd:2379", task: "Upper", timeout: 1},
		{pdAddrs: "http://pd:2379", task: "x", timeout: 1},
		{pdAddrs: "pd:2379,pd:2379", task: "x", timeout: 1},
		{pdAddrs: "pd:2379", task: "x", timeout: 0},
		{pdAddrs: "pd:2379", task: "x", timeout: 1, cert: "cert"},
	} {
		_, err := validateOptions(tc)
		require.Error(t, err, "%+v", tc)
	}
}

func TestInspectBuildsTenantRangeAndProbesEveryUpStore(t *testing.T) {
	old := probeLogBackupFn
	t.Cleanup(func() { probeLogBackupFn = old })
	var probed []string
	probeLogBackupFn = func(_ context.Context, address string, _ credentials.TransportCredentials) error {
		probed = append(probed, address)
		return nil
	}
	p := &fakePD{clusterID: 42, counts: map[string]int64{}, stores: []*metapb.Store{
		{Id: 9, Address: "tikv-b:20160", State: metapb.StoreState_Up},
		{Id: 3, Address: "tikv-a:20160", State: metapb.StoreState_Up},
		{Id: 1, Address: "gone:20160", State: metapb.StoreState_Tombstone},
	}}
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	var out bytes.Buffer
	err = inspect(context.Background(), p, options{task: "kb-tenant-a"}, []string{"pd:2379"}, ks, &out)
	require.NoError(t, err)
	require.True(t, p.closed)
	require.ElementsMatch(t, []string{"tikv-a:20160", "tikv-b:20160"}, probed)
	var got receipt
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	require.Equal(t, "kubebrain.native-pitr-preflight.v1", got.Format)
	require.Equal(t, uint64(42), got.ClusterID)
	require.Equal(t, "tenant-a", got.Keyspace)
	require.Equal(t, "58", got.StartKeyHex[:2])
	require.Len(t, got.Stores, 2)
	require.Equal(t, uint64(3), got.Stores[0].ID)
	require.Len(t, got.OwnershipKeys, 6)
	require.True(t, got.TaskAvailable)
	require.True(t, got.ReadOnly)
}

func TestInspectFailsClosed(t *testing.T) {
	old := probeLogBackupFn
	t.Cleanup(func() { probeLogBackupFn = old })
	probeLogBackupFn = func(context.Context, string, credentials.TransportCredentials) error {
		return errors.New("unimplemented")
	}
	ks := coder.DefaultKeyspace()
	t.Run("task collision", func(t *testing.T) {
		p := &fakePD{clusterID: 1, counts: map[string]int64{metaPrefix + "/info/t": 1}}
		err := inspect(context.Background(), p, options{task: "t"}, nil, ks, &bytes.Buffer{})
		require.ErrorContains(t, err, "already has metadata")
	})
	t.Run("orphan checkpoint collision", func(t *testing.T) {
		p := &fakePD{clusterID: 1, counts: map[string]int64{metaPrefix + "/checkpoint/t/": 1}}
		err := inspect(context.Background(), p, options{task: "t"}, nil, ks, &bytes.Buffer{})
		require.ErrorContains(t, err, "/checkpoint/t/")
	})
	t.Run("service unavailable", func(t *testing.T) {
		p := &fakePD{clusterID: 1, counts: map[string]int64{}, stores: []*metapb.Store{{Id: 1, Address: "tikv:20160", State: metapb.StoreState_Up}}}
		err := inspect(context.Background(), p, options{task: "t"}, nil, ks, &bytes.Buffer{})
		require.ErrorContains(t, err, "unimplemented")
	})
	t.Run("no up stores", func(t *testing.T) {
		p := &fakePD{clusterID: 1, counts: map[string]int64{}, stores: []*metapb.Store{{Id: 1, State: metapb.StoreState_Tombstone}}}
		err := inspect(context.Background(), p, options{task: "t"}, nil, ks, &bytes.Buffer{})
		require.ErrorContains(t, err, "no Up TiKV stores")
	})
}
