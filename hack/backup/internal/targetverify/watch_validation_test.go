package targetverify

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func probeWatchEvent(eventType mvccpb.Event_EventType, kv *mvccpb.KeyValue) clientv3.WatchResponse {
	return clientv3.WatchResponse{Events: []*clientv3.Event{{Type: eventType, Kv: kv}}}
}

func TestValidateProbeWatchCreated(t *testing.T) {
	require.NoError(t, ValidateProbeWatchCreated(clientv3.WatchResponse{Created: true}))
	for name, response := range map[string]clientv3.WatchResponse{
		"not created": {},
		"canceled":    {Created: true, Canceled: true},
		"compacted":   {Created: true, CompactRevision: 1},
		"event":       {Created: true, Events: []*clientv3.Event{{}}},
	} {
		t.Run(name, func(t *testing.T) { require.Error(t, ValidateProbeWatchCreated(response)) })
	}
}

func TestValidateProbeWatchEvent(t *testing.T) {
	key, value := []byte("key"), []byte("value")
	putKV := func() *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: key, Value: value, CreateRevision: 5, ModRevision: 5, Version: 1, Lease: 7}
	}
	require.NoError(t, ValidateProbeWatchEvent(probeWatchEvent(mvccpb.PUT, putKV()), mvccpb.PUT, key, value, 7, 5))
	require.NoError(t, ValidateProbeWatchEvent(probeWatchEvent(mvccpb.DELETE, &mvccpb.KeyValue{Key: key, ModRevision: 6}), mvccpb.DELETE, key, nil, 0, 6))

	wrongValue := putKV()
	wrongValue.Value = []byte("wrong")
	wrongKey := putKV()
	wrongKey.Key = []byte("wrong")
	wrongCreate := putKV()
	wrongCreate.CreateRevision = 4
	wrongMod := putKV()
	wrongMod.ModRevision = 6
	wrongVersion := putKV()
	wrongVersion.Version = 2
	wrongLease := putKV()
	wrongLease.Lease = 8
	withPrevious := probeWatchEvent(mvccpb.PUT, putKV())
	withPrevious.Events[0].PrevKv = &mvccpb.KeyValue{Key: key}
	pollutedDelete := &mvccpb.KeyValue{Key: key, Value: []byte("old"), ModRevision: 6}
	for name, response := range map[string]clientv3.WatchResponse{
		"empty":          {},
		"created":        {Created: true, Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: putKV()}}},
		"canceled":       {Canceled: true, Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: putKV()}}},
		"compacted":      {CompactRevision: 4, Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: putKV()}}},
		"multiple":       {Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: putKV()}, {Type: mvccpb.PUT, Kv: putKV()}}},
		"nil event":      {Events: []*clientv3.Event{nil}},
		"nil key-value":  probeWatchEvent(mvccpb.PUT, nil),
		"wrong type":     probeWatchEvent(mvccpb.DELETE, putKV()),
		"wrong key":      probeWatchEvent(mvccpb.PUT, wrongKey),
		"wrong value":    probeWatchEvent(mvccpb.PUT, wrongValue),
		"wrong create":   probeWatchEvent(mvccpb.PUT, wrongCreate),
		"wrong mod":      probeWatchEvent(mvccpb.PUT, wrongMod),
		"wrong version":  probeWatchEvent(mvccpb.PUT, wrongVersion),
		"wrong lease":    probeWatchEvent(mvccpb.PUT, wrongLease),
		"previous value": withPrevious,
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, ValidateProbeWatchEvent(response, mvccpb.PUT, key, value, 7, 5))
		})
	}
	require.Error(t, ValidateProbeWatchEvent(probeWatchEvent(mvccpb.DELETE, pollutedDelete), mvccpb.DELETE, key, nil, 0, 6))
}
