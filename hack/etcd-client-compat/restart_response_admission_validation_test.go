package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func restartHeader(cluster, member uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: cluster, MemberId: member, Revision: revision}
}

func restartKV(key, value string, create, mod, version int64, lease clientv3.LeaseID) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{Key: []byte(key), Value: []byte(value), CreateRevision: create, ModRevision: mod, Version: version, Lease: int64(lease)}
}

func TestRestartResponseAdmissionKVChain(t *testing.T) {
	a := &restartResponseAdmission{}
	putRevision, err := a.admitPut(&clientv3.PutResponse{Header: restartHeader(7, 9, 10)})
	require.NoError(t, err)
	require.Equal(t, int64(10), putRevision)
	kv, err := a.admitRange(&clientv3.GetResponse{
		Header: restartHeader(7, 11, 10), Count: 1, Kvs: []*mvccpb.KeyValue{restartKV("key", "value", 10, 10, 1, 0)},
	}, restartRangeExpectation{key: []byte("key"), value: []byte("value"), present: true, exactCreate: 10, exactModRevision: 10})
	require.NoError(t, err)
	require.NotNil(t, kv)
	_, err = a.admitDelete(&clientv3.DeleteResponse{Header: restartHeader(7, 9, 11), Deleted: 1}, 1)
	require.NoError(t, err)
	_, err = a.admitRange(&clientv3.GetResponse{Header: restartHeader(7, 11, 11)}, restartRangeExpectation{key: []byte("key")})
	require.NoError(t, err)
}

func TestRestartResponseAdmissionRejectsMalformedKVResponses(t *testing.T) {
	for name, response := range map[string]*clientv3.PutResponse{
		"nil":          nil,
		"nil header":   {},
		"previous key": {Header: restartHeader(7, 9, 10), PrevKv: restartKV("key", "old", 1, 1, 1, 0)},
	} {
		t.Run("put "+name, func(t *testing.T) {
			_, err := (&restartResponseAdmission{}).admitPut(response)
			require.Error(t, err)
		})
	}
	for name, response := range map[string]*clientv3.DeleteResponse{
		"nil":            nil,
		"wrong count":    {Header: restartHeader(7, 9, 10)},
		"previous value": {Header: restartHeader(7, 9, 10), Deleted: 1, PrevKvs: []*mvccpb.KeyValue{restartKV("key", "old", 1, 1, 1, 0)}},
	} {
		t.Run("delete "+name, func(t *testing.T) {
			_, err := (&restartResponseAdmission{}).admitDelete(response, 1)
			require.Error(t, err)
		})
	}
	header := restartHeader(7, 9, 10)
	valid := restartKV("key", "value", 4, 8, 2, 0)
	for name, response := range map[string]*clientv3.GetResponse{
		"nil":             nil,
		"hidden count":    {Header: header, Count: 1},
		"hidden kv":       {Header: header, Kvs: []*mvccpb.KeyValue{valid}},
		"more":            {Header: header, More: true},
		"wrong key":       {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{restartKV("other", "value", 4, 8, 2, 0)}},
		"wrong value":     {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{restartKV("key", "other", 4, 8, 2, 0)}},
		"future revision": {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{restartKV("key", "value", 4, 11, 2, 0)}},
		"impossible version": {Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{
			restartKV("key", "value", 8, 8, 2, 0),
		}},
	} {
		t.Run("range "+name, func(t *testing.T) {
			_, err := (&restartResponseAdmission{}).admitRange(response, restartRangeExpectation{key: []byte("key"), value: []byte("value"), present: true})
			require.Error(t, err)
		})
	}
}

func TestRestartResponseAdmissionLeaseWatchAndMaintenance(t *testing.T) {
	a := &restartResponseAdmission{}
	grant := &clientv3.LeaseGrantResponse{ResponseHeader: restartHeader(7, 9, 10), ID: 12, TTL: 900}
	require.NoError(t, a.admitGrant(grant, 900))
	require.NoError(t, a.admitTTL(&clientv3.LeaseTimeToLiveResponse{
		ResponseHeader: restartHeader(7, 11, 10), ID: 12, TTL: 800, GrantedTTL: 900, Keys: [][]byte{[]byte("leased")},
	}, 12, 900, [][]byte{[]byte("leased")}))
	require.NoError(t, a.admitWatch(clientv3.WatchResponse{
		Header: restartHeader(7, 9, 11), Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: restartKV("key", "value", 11, 11, 1, 0)}},
	}))
	require.NoError(t, a.admitStatus(&etcdserverpb.StatusResponse{Header: restartHeader(7, 11, 11), Leader: 9}))
	require.NoError(t, a.admitAlarm(&etcdserverpb.AlarmResponse{Header: restartHeader(7, 9, 11), Alarms: []*etcdserverpb.AlarmMember{{MemberID: 9, Alarm: etcdserverpb.AlarmType_CORRUPT}}}))
	require.NoError(t, a.admitRevoke(&clientv3.LeaseRevokeResponse{Header: restartHeader(7, 11, 12)}))
}

func TestRestartResponseAdmissionRejectsIdentityAndLeaseWatchDrift(t *testing.T) {
	a := &restartResponseAdmission{}
	_, err := a.admitPut(&clientv3.PutResponse{Header: restartHeader(7, 9, 10)})
	require.NoError(t, err)
	_, err = a.admitRange(&clientv3.GetResponse{Header: restartHeader(8, 9, 11)}, restartRangeExpectation{})
	require.ErrorContains(t, err, "cluster ID changed")
	_, err = a.admitRange(&clientv3.GetResponse{Header: restartHeader(7, 9, 9)}, restartRangeExpectation{})
	require.ErrorContains(t, err, "regressed")
	require.Error(t, (&restartResponseAdmission{}).admitGrant(nil, 900))
	require.Error(t, (&restartResponseAdmission{}).admitTTL(nil, 12, 900, nil))
	require.Error(t, (&restartResponseAdmission{}).admitWatch(clientv3.WatchResponse{}))
	require.Error(t, (&restartResponseAdmission{}).admitStatus(nil))
	require.Error(t, (&restartResponseAdmission{}).admitAlarm(nil))
	require.Error(t, (&restartResponseAdmission{}).admitRevoke(nil))
}

func TestRestartResponseAdmissionReferenceLifecycle(t *testing.T) {
	endpoint := os.Getenv("REFERENCE_RESTART_ADMISSION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set REFERENCE_RESTART_ADMISSION_ENDPOINT to a disposable reference etcd")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	prefix := "/restart-admission-reference/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	a := &restartResponseAdmission{}
	durable, err := cli.Put(ctx, prefix+"durable", "value")
	require.NoError(t, err)
	durableRevision, err := a.admitPut(durable)
	require.NoError(t, err)
	deletedPut, err := cli.Put(ctx, prefix+"deleted", "old")
	require.NoError(t, err)
	deletedRevision, err := a.admitPut(deletedPut)
	require.NoError(t, err)
	deleted, err := cli.Delete(ctx, prefix+"deleted")
	require.NoError(t, err)
	_, err = a.admitDelete(deleted, 1)
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 900)
	require.NoError(t, err)
	require.NoError(t, a.admitGrant(lease, 900))
	leased, err := cli.Put(ctx, prefix+"leased", "leased", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	leasedRevision, err := a.admitPut(leased)
	require.NoError(t, err)

	history, err := cli.Put(ctx, prefix+"history", "event")
	require.NoError(t, err)
	_, err = a.admitPut(history)
	require.NoError(t, err)
	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := cli.Watch(watchCtx, prefix+"history", clientv3.WithRev(history.Header.Revision))
	select {
	case response := <-watch:
		require.NoError(t, a.admitWatch(response))
		require.Len(t, response.Events, 1)
		require.Equal(t, []byte(prefix+"history"), response.Events[0].Kv.Key)
		require.Equal(t, []byte("event"), response.Events[0].Kv.Value)
		require.Equal(t, history.Header.Revision, response.Events[0].Kv.CreateRevision)
		require.Equal(t, history.Header.Revision, response.Events[0].Kv.ModRevision)
		require.Equal(t, int64(1), response.Events[0].Kv.Version)
		require.Zero(t, response.Events[0].Kv.Lease)
	case <-watchCtx.Done():
		t.Fatal("timed out waiting for reference watch")
	}

	current, err := cli.Get(ctx, prefix+"durable")
	require.NoError(t, err)
	_, err = a.admitRange(current, restartRangeExpectation{
		key: []byte(prefix + "durable"), value: []byte("value"), present: true,
		exactCreate: durableRevision, exactModRevision: durableRevision,
	})
	require.NoError(t, err)
	historical, err := cli.Get(ctx, prefix+"deleted", clientv3.WithRev(deletedRevision))
	require.NoError(t, err)
	_, err = a.admitRange(historical, restartRangeExpectation{
		key: []byte(prefix + "deleted"), value: []byte("old"), present: true,
		maxKVRevision: deletedRevision, exactCreate: deletedRevision, exactModRevision: deletedRevision,
	})
	require.NoError(t, err)
	leaseRange, err := cli.Get(ctx, prefix+"leased")
	require.NoError(t, err)
	_, err = a.admitRange(leaseRange, restartRangeExpectation{
		key: []byte(prefix + "leased"), value: []byte("leased"), present: true, leaseID: lease.ID,
		exactCreate: leasedRevision, exactModRevision: leasedRevision,
	})
	require.NoError(t, err)
	ttl, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.NoError(t, a.admitTTL(ttl, lease.ID, lease.TTL, [][]byte{[]byte(prefix + "leased")}))
	statusResponse, err := clientv3.NewMaintenance(cli).Status(ctx, endpoint)
	require.NoError(t, err)
	require.NoError(t, a.admitStatus((*etcdserverpb.StatusResponse)(statusResponse)))
	revoked, err := cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	require.NoError(t, a.admitRevoke(revoked))
}
