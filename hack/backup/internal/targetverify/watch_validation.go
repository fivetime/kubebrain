package targetverify

import (
	"bytes"
	"errors"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func ValidateProbeWatchCreated(response clientv3.WatchResponse) error {
	if response.Err() != nil || response.Canceled || !response.Created || response.CompactRevision != 0 || len(response.Events) != 0 {
		return errors.New("probe watch returned an invalid creation acknowledgement")
	}
	return nil
}

func ValidateProbeWatchEvent(response clientv3.WatchResponse, eventType mvccpb.Event_EventType, key, value []byte, leaseID clientv3.LeaseID, revision int64) error {
	if response.Err() != nil || response.Canceled || response.Created || response.CompactRevision != 0 || response.IsProgressNotify() || len(response.Events) != 1 {
		return errors.New("probe watch returned an invalid event envelope")
	}
	event := response.Events[0]
	if event == nil || event.Type != eventType || event.Kv == nil || event.PrevKv != nil || !bytes.Equal(event.Kv.Key, key) || event.Kv.ModRevision != revision {
		return errors.New("probe watch returned an invalid event payload")
	}
	switch eventType {
	case mvccpb.PUT:
		if !bytes.Equal(event.Kv.Value, value) || event.Kv.CreateRevision != revision || event.Kv.Version != 1 || event.Kv.Lease != int64(leaseID) {
			return errors.New("probe watch returned an invalid put event")
		}
	case mvccpb.DELETE:
		if len(event.Kv.Value) != 0 || event.Kv.CreateRevision != 0 || event.Kv.Version != 0 || event.Kv.Lease != 0 {
			return errors.New("probe watch returned an invalid delete event")
		}
	default:
		return errors.New("probe watch expected an unsupported event type")
	}
	return nil
}
