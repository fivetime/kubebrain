package main

import (
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func smokeHeader(cluster, member uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: cluster, MemberId: member, Revision: revision}
}

func smokeKV(key string, create, mod, version int64) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{Key: []byte(key), Value: []byte("value"), CreateRevision: create, ModRevision: mod, Version: version}
}

func TestSmokeResponseAdmissionWriteReadChain(t *testing.T) {
	a := &smokeResponseAdmission{}
	require.NoError(t, a.admitPut(&clientv3.PutResponse{Header: smokeHeader(7, 9, 10)}))
	kv, err := a.admitPointGet(&clientv3.GetResponse{Header: smokeHeader(7, 11, 10), Count: 1, Kvs: []*mvccpb.KeyValue{smokeKV("key", 10, 10, 1)}}, "key")
	require.NoError(t, err)
	require.NotNil(t, kv)
	kv, err = a.admitPointGet(&clientv3.GetResponse{Header: smokeHeader(7, 9, 11)}, "missing")
	require.NoError(t, err)
	require.Nil(t, kv)
}

func TestSmokeResponseAdmissionRejectsInvalidPut(t *testing.T) {
	for name, response := range map[string]*clientv3.PutResponse{
		"nil response":  nil,
		"nil header":    {},
		"zero cluster":  {Header: smokeHeader(0, 9, 10)},
		"zero member":   {Header: smokeHeader(7, 0, 10)},
		"zero revision": {Header: smokeHeader(7, 9, 0)},
		"previous kv":   {Header: smokeHeader(7, 9, 10), PrevKv: smokeKV("key", 1, 1, 1)},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&smokeResponseAdmission{}).admitPut(response))
		})
	}
	a := &smokeResponseAdmission{}
	require.NoError(t, a.admitPut(&clientv3.PutResponse{Header: smokeHeader(7, 9, 10)}))
	require.ErrorContains(t, a.admitPut(&clientv3.PutResponse{Header: smokeHeader(7, 9, 10)}), "did not advance")
	require.ErrorContains(t, a.admitPut(&clientv3.PutResponse{Header: smokeHeader(8, 9, 11)}), "cluster ID changed")
}

func TestSmokeResponseAdmissionRejectsInvalidPointGet(t *testing.T) {
	header := smokeHeader(7, 9, 10)
	valid := smokeKV("key", 4, 8, 2)
	for name, response := range map[string]*clientv3.GetResponse{
		"nil response":   nil,
		"nil header":     {},
		"negative count": {Header: header, Count: -1},
		"hidden count":   {Header: header, Count: 1},
		"hidden kv":      {Header: header, Kvs: []*mvccpb.KeyValue{valid}},
		"multiple":       {Header: header, Count: 2, Kvs: []*mvccpb.KeyValue{valid, valid}},
		"more":           {Header: header, More: true},
		"nil kv":         {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{nil}},
		"wrong key":      {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{smokeKV("other", 4, 8, 2)}},
		"zero create":    {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{smokeKV("key", 0, 8, 2)}},
		"mod before create": {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{
			smokeKV("key", 8, 7, 1),
		}},
		"future mod": {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{
			smokeKV("key", 4, 11, 2),
		}},
		"zero version": {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{
			smokeKV("key", 4, 8, 0),
		}},
		"impossible version": {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{
			smokeKV("key", 8, 8, 2),
		}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (&smokeResponseAdmission{}).admitPointGet(response, "key")
			require.Error(t, err)
		})
	}
}

func TestSmokeResponseAdmissionRejectsIdentityDriftAndStaleRead(t *testing.T) {
	a := &smokeResponseAdmission{}
	require.NoError(t, a.admitPut(&clientv3.PutResponse{Header: smokeHeader(7, 9, 10)}))
	_, err := a.admitPointGet(&clientv3.GetResponse{Header: smokeHeader(8, 9, 11)}, "key")
	require.ErrorContains(t, err, "cluster ID changed")
	_, err = a.admitPointGet(&clientv3.GetResponse{Header: smokeHeader(7, 9, 9)}, "key")
	require.ErrorContains(t, err, "regressed")
}

func TestSmokeResponseAdmissionAcceptsEmbeddedEtcdLifecycle(t *testing.T) {
	cli := smokeTestClient(t)
	ctx := t.Context()
	a := &smokeResponseAdmission{}
	put, err := cli.Put(ctx, "/persistence/key", "value")
	require.NoError(t, err)
	require.NoError(t, a.admitPut(put))
	get, err := cli.Get(ctx, "/persistence/key")
	require.NoError(t, err)
	kv, err := a.admitPointGet(get, "/persistence/key")
	require.NoError(t, err)
	require.Equal(t, "value", string(kv.Value))

	_, err = cli.Delete(ctx, "/persistence/key")
	require.NoError(t, err)
	absent, err := cli.Get(ctx, "/persistence/key")
	require.NoError(t, err)
	kv, err = (&smokeResponseAdmission{}).admitPointGet(absent, "/persistence/key")
	require.NoError(t, err)
	require.Nil(t, kv)
}

func smokeTestClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	peerURL := smokeFreeURL(t)
	clientURL := smokeFreeURL(t)
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peerURL}, []url.URL{peerURL}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{clientURL}, []url.URL{clientURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		server.Close()
		t.Fatal("embedded etcd did not become ready")
	}
	t.Cleanup(server.Close)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{clientURL.String()}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	return cli
}

func smokeFreeURL(t *testing.T) url.URL {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return url.URL{Scheme: "http", Host: address}
}
