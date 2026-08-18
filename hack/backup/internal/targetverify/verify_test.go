package targetverify

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestValidateRangePage(t *testing.T) {
	header := func(revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{Revision: revision}
	}
	kv := func(key string, modRevision int64) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), CreateRevision: 1, ModRevision: modRevision, Version: 1}
	}
	fullPage := make([]*mvccpb.KeyValue, rangePageLimit)
	for i := range fullPage {
		fullPage[i] = kv(fmt.Sprintf("a%04d", i), 5)
	}
	overLimit := append(append([]*mvccpb.KeyValue(nil), fullPage...), kv("b", 5))
	tests := map[string]struct {
		response      *clientv3.GetResponse
		snapshot      int64
		start         string
		end           string
		expectedCount int64
		wantSnapshot  int64
		wantRemaining int64
		wantErr       string
	}{
		"pin current revision": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 5)}},
			start:    "a", end: "z", expectedCount: -1, wantSnapshot: 5,
		},
		"historical forward header": {
			response: &clientv3.GetResponse{Header: header(6), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 5)}},
			snapshot: 5, start: "a", end: "z", expectedCount: 1, wantSnapshot: 5,
		},
		"unbounded from-key range": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("b", 5)}},
			start:    "a", end: "\x00", expectedCount: -1, wantSnapshot: 5,
		},
		"full continuation": {
			response: &clientv3.GetResponse{Header: header(5), Count: rangePageLimit + 1, More: true, Kvs: fullPage},
			snapshot: 5, start: "a", end: "z", expectedCount: -1, wantSnapshot: 5, wantRemaining: 1,
		},
		"empty response": {expectedCount: -1, wantErr: "empty response"},
		"missing header": {
			response: &clientv3.GetResponse{}, expectedCount: -1, wantErr: "no valid response revision",
		},
		"stale header": {
			response: &clientv3.GetResponse{Header: header(4)}, snapshot: 5, expectedCount: -1,
			wantErr: "behind snapshot revision 5",
		},
		"count below payload": {
			response: &clientv3.GetResponse{Header: header(5), Kvs: []*mvccpb.KeyValue{kv("a", 5)}},
			start:    "a", end: "z", expectedCount: -1, wantErr: "count 0 for 1 records",
		},
		"broken count continuity": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 5)}},
			start:    "a", end: "z", expectedCount: 2, wantErr: "previous remaining count 2",
		},
		"above page limit": {
			response: &clientv3.GetResponse{Header: header(5), Count: rangePageLimit + 1, Kvs: overLimit},
			start:    "a", end: "z", expectedCount: -1, wantErr: "above page limit",
		},
		"inconsistent more": {
			response: &clientv3.GetResponse{Header: header(5), Count: 2, Kvs: []*mvccpb.KeyValue{kv("a", 5)}},
			start:    "a", end: "z", expectedCount: -1, wantErr: "inconsistent count/more",
		},
		"non-full continuation": {
			response: &clientv3.GetResponse{Header: header(5), Count: 2, More: true, Kvs: []*mvccpb.KeyValue{kv("a", 5)}},
			start:    "a", end: "z", expectedCount: -1, wantErr: "non-full page",
		},
		"nil record": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{nil}},
			start:    "a", end: "z", expectedCount: -1, wantErr: "nil record",
		},
		"outside range": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("z", 5)}},
			start:    "a", end: "z", expectedCount: -1, wantErr: "outside requested range",
		},
		"duplicate key": {
			response: &clientv3.GetResponse{Header: header(5), Count: 2, Kvs: []*mvccpb.KeyValue{kv("a", 5), kv("a", 5)}},
			start:    "a", end: "z", expectedCount: -1, wantErr: "not in strict ascending",
		},
		"future MVCC": {
			response: &clientv3.GetResponse{Header: header(6), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 6)}},
			snapshot: 5, start: "a", end: "z", expectedCount: -1, wantErr: "invalid MVCC metadata",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot, remaining, err := ValidateRangePage(tc.response, tc.snapshot, []byte(tc.start), []byte(tc.end), tc.expectedCount)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantSnapshot, snapshot)
			require.Equal(t, tc.wantRemaining, remaining)
		})
	}
}

func TestValidateLease(t *testing.T) {
	header := func(revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{Revision: revision}
	}
	tests := map[string]struct {
		response *clientv3.LeaseTimeToLiveResponse
		wantErr  string
	}{
		"valid unsorted keys": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, TTL: 30, GrantedTTL: 60, Keys: [][]byte{[]byte("b"), []byte("a")}},
		},
		"promotion extension": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, TTL: 63, GrantedTTL: 60, Keys: [][]byte{[]byte("a"), []byte("b")}},
		},
		"empty response": {wantErr: "empty TTL response"},
		"wrong ID": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 11, TTL: 30, GrantedTTL: 60}, wantErr: "mismatched ID 11",
		},
		"missing header": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 10, TTL: 30, GrantedTTL: 60}, wantErr: "no valid response revision",
		},
		"stale header": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(4), ID: 10, TTL: 30, GrantedTTL: 60}, wantErr: "behind current range revision 5",
		},
		"expired": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, GrantedTTL: 60}, wantErr: "identity/TTL mismatch",
		},
		"wrong grant": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, TTL: 30, GrantedTTL: 61}, wantErr: "identity/TTL mismatch",
		},
		"empty key": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, TTL: 30, GrantedTTL: 60, Keys: [][]byte{nil}}, wantErr: "empty attached key",
		},
		"duplicate key": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, TTL: 30, GrantedTTL: 60, Keys: [][]byte{[]byte("a"), []byte("a")}}, wantErr: "duplicate attached key",
		},
		"key mismatch": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 10, TTL: 30, GrantedTTL: 60, Keys: [][]byte{[]byte("a")}}, wantErr: "attached keys mismatch",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateLease(tc.response, 10, 60, 5, []string{"b", "a"})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
	err := ValidateLease(&clientv3.LeaseTimeToLiveResponse{
		ResponseHeader: header(6), ID: 10, TTL: 30, GrantedTTL: clientv3.MaxLeaseTTL + 1,
	}, 10, clientv3.MaxLeaseTTL+1, 5, nil)
	require.ErrorContains(t, err, "identity/TTL mismatch")
}
