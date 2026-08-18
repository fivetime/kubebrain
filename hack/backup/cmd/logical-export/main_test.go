package main

import (
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNextKeyDoesNotMutateInput(t *testing.T) {
	in := []byte("abc")
	out := nextKey(in)

	require.Equal(t, []byte("abc"), in)
	require.Equal(t, []byte{'a', 'b', 'c', 0}, out)
}

func TestExportLeaseRecord(t *testing.T) {
	tests := map[string]struct {
		response *clientv3.LeaseTimeToLiveResponse
		want     record.Lease
		wantErr  string
	}{
		"current lease": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: 30, GrantedTTL: 60},
			want:     record.Lease{ID: 123, TTL: 30, GrantedTTL: 60},
		},
		"promotion extension": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: 63, GrantedTTL: 60},
			want:     record.Lease{ID: 123, TTL: 63, GrantedTTL: 60},
		},
		"exact maximum": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: clientv3.MaxLeaseTTL, GrantedTTL: clientv3.MaxLeaseTTL},
			want:     record.Lease{ID: 123, TTL: clientv3.MaxLeaseTTL, GrantedTTL: clientv3.MaxLeaseTTL},
		},
		"empty response": {
			wantErr: "empty TTL response",
		},
		"mismatched ID": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 124, TTL: 30, GrantedTTL: 60},
			wantErr:  "mismatched ID 124",
		},
		"expired": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: 0, GrantedTTL: 60},
			wantErr:  "expired while exporting snapshot revision 42",
		},
		"missing grant": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: 30},
			wantErr:  "invalid granted TTL 0",
		},
		"oversized grant": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: 30, GrantedTTL: clientv3.MaxLeaseTTL + 1},
			wantErr:  "invalid granted TTL 9000000001",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lease, err := exportLeaseRecord(123, tc.response, 42)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, lease)
		})
	}
}

func TestRunReturnsEnvironmentValidationError(t *testing.T) {
	t.Setenv("BATCH_SIZE", "zero")

	require.ErrorContains(t, run(), `invalid BATCH_SIZE: "zero"`)
}
