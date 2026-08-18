package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	meta_storagepb "github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/credentials"
)

type fakePD struct {
	clusterID uint64
	stores    []*metapb.Store
	counts    map[string]int64
	responses map[string]*meta_storagepb.GetResponse
	getErr    error
	storesErr error
	closed    bool
}

func (f *fakePD) GetClusterID(context.Context) uint64 { return f.clusterID }
func (f *fakePD) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return f.stores, f.storesErr
}
func (f *fakePD) Get(_ context.Context, key []byte, _ ...pd.OpOption) (*meta_storagepb.GetResponse, error) {
	if response, ok := f.responses[string(key)]; ok {
		return response, f.getErr
	}
	count := f.counts[string(key)]
	response := &meta_storagepb.GetResponse{Header: &meta_storagepb.ResponseHeader{ClusterId: f.clusterID, Revision: 1}, Count: count}
	for range count {
		response.Kvs = append(response.Kvs, &meta_storagepb.KeyValue{Key: append([]byte(nil), key...), CreateRevision: 1, ModRevision: 1, Version: 1})
	}
	return response, f.getErr
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

func TestTransportCredentialsRejectsOversizedCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, make([]byte, maxTLSPEMBytes+1), 0o600))

	_, err := transportCredentials(options{ca: path})
	require.ErrorContains(t, err, "TLS PEM exceeds")
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
	t.Run("different task collision", func(t *testing.T) {
		p := &fakePD{clusterID: 1, counts: map[string]int64{metaPrefix + "/info/": 1}}
		err := inspect(context.Background(), p, options{task: "t"}, nil, ks, &bytes.Buffer{})
		require.ErrorContains(t, err, "supports one")
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

func TestValidateEmptyMetadataGet(t *testing.T) {
	valid := func(keys ...string) *meta_storagepb.GetResponse {
		response := &meta_storagepb.GetResponse{Header: &meta_storagepb.ResponseHeader{ClusterId: 7, Revision: 9}, Count: int64(len(keys))}
		for _, key := range keys {
			response.Kvs = append(response.Kvs, &meta_storagepb.KeyValue{Key: []byte(key), CreateRevision: 2, ModRevision: 3, Version: 2})
		}
		return response
	}
	require.NoError(t, validateEmptyMetadataGet(valid(), 7, []byte("prefix/"), true))
	require.NoError(t, validateEmptyMetadataGet(valid("prefix/a", "prefix/b"), 7, []byte("prefix/"), true))
	require.NoError(t, validateEmptyMetadataGet(valid("exact"), 7, []byte("exact"), false))

	badCount := valid()
	badCount.Count = 1
	negativeCount := valid()
	negativeCount.Count = -1
	more := valid()
	more.More = true
	nilKV := valid("prefix/a")
	nilKV.Kvs[0] = nil
	badMetadata := valid("prefix/a")
	badMetadata.Kvs[0].Version = 0
	tests := []*meta_storagepb.GetResponse{
		nil,
		{},
		{Header: &meta_storagepb.ResponseHeader{ClusterId: 8, Revision: 9}},
		{Header: &meta_storagepb.ResponseHeader{ClusterId: 7}},
		{Header: &meta_storagepb.ResponseHeader{ClusterId: 7, Revision: 9, Error: &meta_storagepb.Error{}}},
		badCount,
		negativeCount,
		more,
		nilKV,
		badMetadata,
		valid("outside"),
		valid("prefix/b", "prefix/a"),
		valid("exact", "exact"),
	}
	for i, response := range tests {
		require.Error(t, validateEmptyMetadataGet(response, 7, []byte("prefix/"), true), i)
	}
	require.Error(t, validateEmptyMetadataGet(valid("exact", "exact"), 7, []byte("exact"), false))
}

func TestInspectRejectsMalformedEmptyMetadataResponse(t *testing.T) {
	p := &fakePD{clusterID: 7, responses: map[string]*meta_storagepb.GetResponse{
		metaPrefix + "/info/": {Header: &meta_storagepb.ResponseHeader{ClusterId: 7, Revision: 1}, Count: 0, Kvs: []*meta_storagepb.KeyValue{{Key: []byte(metaPrefix + "/info/hidden"), CreateRevision: 1, ModRevision: 1, Version: 1}}},
	}}
	err := inspect(context.Background(), p, options{task: "t"}, nil, coder.DefaultKeyspace(), &bytes.Buffer{})
	require.ErrorContains(t, err, "invalid range envelope")
	require.True(t, p.closed)
}
