package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestControlResponseHeadersAcrossDirectReplicas(t *testing.T) {
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
			connection, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer connection.Close()

			maintenance := etcdserverpb.NewMaintenanceClient(connection)
			statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
			require.NoError(t, err)
			require.NotNil(t, statusResponse.Header)
			require.NoError(t, identity.admitHeader(index, statusResponse.Header, 1))
			assertLocal := func(name string, header *etcdserverpb.ResponseHeader) {
				t.Helper()
				require.NoError(t, identity.admitIdentityHeader(index, header), name)
				require.NotNil(t, header, name)
				require.Equal(t, statusResponse.Header.ClusterId, header.ClusterId, name)
				require.Equal(t, statusResponse.Header.MemberId, header.MemberId, name)
				require.Positive(t, header.RaftTerm, name)
			}

			members, err := etcdserverpb.NewClusterClient(connection).MemberList(ctx, &etcdserverpb.MemberListRequest{})
			require.NoError(t, err)
			assertLocal("member list", members.Header)
			require.Len(t, members.Members, 3)
			require.Contains(t, memberIDs(members.Members), statusResponse.Header.MemberId)

			hash, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
			require.NoError(t, err)
			assertLocal("hash", hash.Header)
			require.NoError(t, identity.admitHeader(index, hash.Header, statusResponse.Header.Revision))
			require.Positive(t, hash.Header.Revision)

			hashKV, err := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{})
			require.NoError(t, err)
			assertLocal("hashkv", hashKV.Header)
			require.NoError(t, identity.admitHeader(index, hashKV.Header, statusResponse.Header.Revision))
			require.Positive(t, hashKV.HashRevision)
			require.LessOrEqual(t, hashKV.CompactRevision, hashKV.HashRevision)

			alarms, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
				Action: etcdserverpb.AlarmRequest_GET,
				Alarm:  etcdserverpb.AlarmType_NONE,
			})
			require.NoError(t, err)
			assertLocal("alarm get", alarms.Header)
			require.NoError(t, identity.admitHeader(index, alarms.Header, statusResponse.Header.Revision))

			authStatus, err := etcdserverpb.NewAuthClient(connection).AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
			require.NoError(t, err)
			assertLocal("auth status", authStatus.Header)
			require.NoError(t, identity.admitHeader(index, authStatus.Header, statusResponse.Header.Revision))
		})
	}
}

func memberIDs(members []*etcdserverpb.Member) []uint64 {
	ids := make([]uint64, 0, len(members))
	for _, member := range members {
		ids = append(ids, member.ID)
	}
	return ids
}
