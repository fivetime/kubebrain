package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMutationResponseHeadersAcrossDirectReplicas(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_DIRECT_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_DIRECT_ENDPOINTS to comma-separated direct replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.GreaterOrEqual(t, len(endpoints), 3)
	requireDistinctDirectReplicaTopology(t, endpoints)
	identity := newLiveResponseIdentityAdmission(t)

	for index, rawEndpoint := range endpoints {
		endpoint := strings.TrimSpace(rawEndpoint)
		t.Run(endpoint, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			statusResponse, err := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
			require.NoError(t, err)
			require.NotNil(t, statusResponse.Header)
			require.NoError(t, identity.admitHeader(index, statusResponse.Header, 1))
			assertLocal := func(name string, header *etcdserverpb.ResponseHeader) {
				t.Helper()
				require.NoError(t, identity.admitHeader(index, header, statusResponse.Header.Revision), name)
				require.NotNil(t, header, name)
				require.Equal(t, statusResponse.Header.ClusterId, header.ClusterId, name)
				require.Equal(t, statusResponse.Header.MemberId, header.MemberId, name)
				require.Positive(t, header.RaftTerm, name)
			}

			lease := etcdserverpb.NewLeaseClient(conn)
			grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
			require.NoError(t, err)
			assertLocal("lease grant", grant.Header)
			require.Positive(t, grant.ID)
			key := []byte(fmt.Sprintf("/registry/etcd-client-compat/direct-mutation-header/%d/%d", time.Now().UnixNano(), index))
			kv := etcdserverpb.NewKVClient(conn)
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				ttl, ttlErr := lease.LeaseTimeToLive(cleanupCtx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID})
				require.NoError(t, ttlErr)
				assertLocal("lease cleanup ttl", ttl.Header)
				if ttl.TTL != -1 {
					revoked, revokeErr := lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
					require.NoError(t, revokeErr)
					assertLocal("lease cleanup", revoked.Header)
				}
				deleted, deleteErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
				require.NoError(t, deleteErr)
				assertLocal("key cleanup", deleted.Header)
			}()

			put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("one"), Lease: grant.ID})
			require.NoError(t, err)
			assertLocal("put", put.Header)
			txn, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key: key, Target: etcdserverpb.Compare_VERSION,
					Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
				}},
				Success: []*etcdserverpb.RequestOp{
					{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("two"), Lease: grant.ID}}},
					{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
				},
			})
			require.NoError(t, err)
			require.True(t, txn.Succeeded)
			assertLocal("txn", txn.Header)
			require.Len(t, txn.Responses, 2)
			require.Equal(t, txn.Header.Revision, txn.Responses[0].GetResponsePut().Header.Revision)
			require.Equal(t, txn.Header.Revision, txn.Responses[1].GetResponseRange().Header.Revision)
			require.Len(t, txn.Responses[1].GetResponseRange().Kvs, 1)
			require.Equal(t, []byte("two"), txn.Responses[1].GetResponseRange().Kvs[0].Value)
			deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
			require.NoError(t, err)
			assertLocal("delete range", deleted.Header)
			require.Equal(t, int64(1), deleted.Deleted)
			require.Len(t, deleted.PrevKvs, 1)
			require.Equal(t, []byte("two"), deleted.PrevKvs[0].Value)
			restored, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("three"), Lease: grant.ID})
			require.NoError(t, err)
			assertLocal("restored put", restored.Header)

			ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
			require.NoError(t, err)
			assertLocal("lease ttl", ttl.Header)
			require.Equal(t, [][]byte{key}, ttl.Keys)
			listed, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
			require.NoError(t, err)
			assertLocal("lease list", listed.Header)
			require.True(t, leaseListContainsRaw(listed, grant.ID))

			revoked, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
			require.NoError(t, err)
			assertLocal("lease revoke", revoked.Header)
			missing, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
			require.NoError(t, err)
			assertLocal("missing lease ttl", missing.Header)
			require.Equal(t, int64(-1), missing.TTL)
			require.Empty(t, missing.Keys)
		})
	}
}

func leaseListContainsRaw(response *etcdserverpb.LeaseLeasesResponse, id int64) bool {
	for _, lease := range response.Leases {
		if lease.ID == id {
			return true
		}
	}
	return false
}
