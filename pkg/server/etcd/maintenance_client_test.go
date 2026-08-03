// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientSnapshotAPIsReturnPlatformUnsupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	versioned, err := client.SnapshotWithVersion(ctx)
	if versioned != nil && versioned.Snapshot != nil {
		require.NoError(t, versioned.Snapshot.Close())
	}
	requirePlatformReplacementError(t, err, snapshotUnsupportedMessage)

	legacy, err := client.Snapshot(ctx)
	require.NoError(t, err)
	require.NotNil(t, legacy)
	_, err = io.ReadAll(legacy)
	requirePlatformReplacementError(t, err, snapshotUnsupportedMessage)
	require.NoError(t, legacy.Close())
}

func TestRawGRPCSnapshotReturnsPlatformUnsupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///snapshot-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := maintenance.Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	requirePlatformReplacementError(t, err, snapshotUnsupportedMessage)
	info := requirePlatformManagedErrorInfo(t, err)
	require.Equal(t, "Backup", info.Metadata["operation_type"])
	require.Equal(t, "kubebrain.logical.v2", info.Metadata["artifact_format"])
	require.Equal(t, "false", info.Metadata["etcd_snapshot_restore_usable"])
	require.Equal(t, "kubebrain-logical-etcd-snapshot", info.Metadata["conversion_tool"])
}

func TestClientPlatformManagedOperationsReturnActionableErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 1, Name: "learner", PeerURLs: []string{"http://127.0.0.1:2380"}, IsLearner: true},
		{ID: 2, Name: "voter", PeerURLs: []string{"http://127.0.0.2:2380"}},
	})

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterClusterServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.MemberAdd(ctx, []string{"http://127.0.0.1:12380"})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = client.MemberAddAsLearner(ctx, []string{"http://127.0.0.1:12381"})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = client.MemberRemove(ctx, 1)
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = client.MemberUpdate(ctx, 1, []string{"http://127.0.0.1:12380"})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = client.MemberPromote(ctx, 1)
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = client.MoveLeader(ctx, 2)
	requirePlatformReplacementError(t, err, moveLeaderUnsupportedMessage)
	downgradeResponse, err := client.Downgrade(ctx, clientv3.DowngradeValidate, "3.6.0")
	require.NoError(t, err)
	require.Equal(t, ClusterVersion, downgradeResponse.Version)
}

func TestRawGRPCPlatformManagedMaintenanceReturnsActionableErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 1, Name: "voter"},
		{ID: 2, Name: "learner", IsLearner: true},
	})

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///platform-maintenance-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, targetID := range []uint64{0, 2, math.MaxUint64} {
		_, err = maintenance.MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: targetID})
		require.ErrorIs(t, err, rpctypes.ErrGRPCBadLeaderTransferee)
	}
	_, err = maintenance.MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: 1})
	requirePlatformReplacementError(t, err, moveLeaderUnsupportedMessage)
	validateResponse, err := maintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: "3.6.0",
	})
	require.NoError(t, err)
	require.Equal(t, ClusterVersion, validateResponse.GetVersion())
	require.NotNil(t, validateResponse.GetHeader())
	_, err = maintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_ENABLE, Version: "3.6.0",
	})
	requirePlatformReplacementError(t, err, downgradeUnsupportedMessage)
	cancelResponse, err := maintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_CANCEL, Version: "not-semver",
	})
	require.NoError(t, err)
	require.Equal(t, "3.7", cancelResponse.GetVersion())
	require.NotNil(t, cancelResponse.GetHeader())
	require.Positive(t, cancelResponse.GetHeader().GetRevision())
	_, err = maintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_ENABLE, Version: "not-semver",
	})
	require.ErrorIs(t, err, rpctypes.ErrGRPCWrongDowngradeVersionFormat)
	for _, version := range []string{"3.7.0", "3.5", "4.0"} {
		_, err = maintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
			Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: version,
		})
		require.ErrorIs(t, err, rpctypes.ErrGRPCInvalidDowngradeTargetVersion)
	}
	_, err = maintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_DowngradeAction(127), Version: "3.7.0",
	})
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Equal(t, "etcdserver: unknown method", status.Convert(err).Message())
}

func TestDowngradeValidateUsesReadBarrierBeforeTargetValidation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	barrierErr := errors.New("downgrade barrier failed")
	barrierCalls := 0
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		barrierCalls++
		return barrierErr
	}}

	for _, version := range []string{"3.6", "3.7"} {
		response, err := server.Downgrade(context.Background(), &etcdserverpb.DowngradeRequest{
			Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: version,
		})
		require.Nil(t, response)
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Equal(t, barrierErr.Error(), status.Convert(err).Message())
	}
	require.Equal(t, 2, barrierCalls)

	response, err := server.Downgrade(context.Background(), &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: "not-semver",
	})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrGRPCWrongDowngradeVersionFormat)
	require.Equal(t, 2, barrierCalls)
}

func TestMoveLeaderToCurrentLeaderIsIdempotent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.peers = testPeerService{leaderInfo: "leader.test:3380"}
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 7, Name: "leader", PeerURLs: []string{"http://leader.test:3380"}},
		{ID: 8, Name: "other", PeerURLs: []string{"http://other.test:3380"}},
	})

	response, err := server.MoveLeader(context.Background(), &etcdserverpb.MoveLeaderRequest{TargetID: 7})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Nil(t, response.GetHeader())

	response, err = server.MoveLeader(context.Background(), &etcdserverpb.MoveLeaderRequest{TargetID: 8})
	require.Nil(t, response)
	requirePlatformReplacementError(t, err, moveLeaderUnsupportedMessage)
}

func TestRawGRPCPlatformManagedMemberMutationsReturnActionableErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{{
		ID: 1, Name: "learner", PeerURLs: []string{"http://127.0.0.1:2380"}, IsLearner: true,
	}})

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterClusterServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///platform-cluster-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := etcdserverpb.NewClusterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cluster.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{PeerURLs: []string{"http://127.0.0.1:12380"}})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = cluster.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{
		PeerURLs:  []string{"http://127.0.0.1:12381"},
		IsLearner: true,
	})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = cluster.MemberRemove(ctx, &etcdserverpb.MemberRemoveRequest{ID: 1})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = cluster.MemberUpdate(ctx, &etcdserverpb.MemberUpdateRequest{
		ID:       1,
		PeerURLs: []string{"http://127.0.0.1:12380"},
	})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
	_, err = cluster.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{ID: 1})
	requirePlatformReplacementError(t, err, memberMutationUnsupportedMessage)
}

func TestClientDefragmentReturnsEtcdNilHeaderNoOp(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.Defragment(ctx, "bufnet")
	require.NoError(t, err)
	require.Nil(t, response.Header)
}

func TestClientDefragmentNoOpPreservesHashKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "/a1182/defrag/key", "value")
	require.NoError(t, err)
	before, err := client.HashKV(ctx, "bufnet", 0)
	require.NoError(t, err)
	requireRawHashKVHeaderAtOrAfter(t, before.Header, put.Header.Revision)

	defrag, err := client.Defragment(ctx, "bufnet")
	require.NoError(t, err)
	require.Nil(t, defrag.Header)

	after, err := client.HashKV(ctx, "bufnet", 0)
	require.NoError(t, err)
	require.Equal(t, before.Hash, after.Hash)
	require.Equal(t, before.CompactRevision, after.CompactRevision)
	require.Equal(t, before.HashRevision, after.HashRevision)
	require.Equal(t, before.Header.Revision, after.Header.Revision)
}

func TestClientStatusProtocolMetadataMatchesEtcdContract(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "/a1110/status-client/probe", "value")
	require.NoError(t, err)
	response, err := client.Status(ctx, "bufnet")
	require.NoError(t, err)
	require.NotNil(t, response.Header)
	require.GreaterOrEqual(t, response.Header.Revision, put.Header.Revision)
	require.NotZero(t, response.Header.ClusterId)
	require.NotZero(t, response.Header.MemberId)
	require.Positive(t, response.Header.RaftTerm)
	require.Equal(t, response.Header.MemberId, response.Leader)
	require.Equal(t, response.Header.RaftTerm, response.RaftTerm)
	require.Equal(t, response.RaftIndex, response.RaftAppliedIndex)
	require.GreaterOrEqual(t, response.RaftAppliedIndex, uint64(put.Header.Revision))
	require.Equal(t, Version, response.Version)
	require.Equal(t, Version, response.StorageVersion)
	require.Equal(t, defaultEtcdBackendQuota, response.DbSizeQuota)
	require.False(t, response.IsLearner)
	require.NotNil(t, response.DowngradeInfo)
	require.False(t, response.DowngradeInfo.Enabled)
	require.Empty(t, response.DowngradeInfo.TargetVersion)
	require.Empty(t, response.Errors)
}

func TestClientStatusReportsConfiguredQuota(t *testing.T) {
	const quota = int64(300010002000)
	server := newQuotaRPCServer(t, quota)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.Status(ctx, "bufnet")
	require.NoError(t, err)
	require.NotNil(t, response.Header)
	require.Equal(t, quota, response.DbSizeQuota)
	require.Empty(t, response.Errors)
}

func TestRawGRPCAlarmGetAndZeroMemberRoundTrip(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///alarm-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a997/alarm-client/header"), Value: []byte("value"),
	})
	require.NoError(t, err)
	for _, request := range []*etcdserverpb.AlarmRequest{
		{Action: etcdserverpb.AlarmRequest_GET},
		{Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NOSPACE},
		{Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_CORRUPT},
		{Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType(127)},
		{Action: etcdserverpb.AlarmRequest_GET, MemberID: math.MaxUint64},
	} {
		resp, callErr := maintenance.Alarm(ctx, request)
		require.NoError(t, callErr)
		require.NotNil(t, resp.Header)
		require.Equal(t, put.Header.Revision, resp.Header.Revision)
		require.NotZero(t, resp.Header.ClusterId)
		require.NotZero(t, resp.Header.MemberId)
		require.Positive(t, resp.Header.RaftTerm)
		require.Empty(t, resp.Alarms)
	}

	members := func(action etcdserverpb.AlarmRequest_AlarmAction) []uint64 {
		t.Helper()
		resp, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		})
		require.NoError(t, callErr)
		got := make([]uint64, 0, len(resp.Alarms))
		for _, alarm := range resp.Alarms {
			require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarm.Alarm)
			got = append(got, alarm.MemberID)
		}
		return got
	}
	_ = members(etcdserverpb.AlarmRequest_DEACTIVATE)
	require.Equal(t, []uint64{0}, members(etcdserverpb.AlarmRequest_ACTIVATE))
	require.Equal(t, []uint64{0}, members(etcdserverpb.AlarmRequest_GET))
	require.Equal(t, []uint64{0}, members(etcdserverpb.AlarmRequest_DEACTIVATE))
	require.Empty(t, members(etcdserverpb.AlarmRequest_GET))

	const corruptMemberID = uint64(42)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: corruptMemberID,
	})
	require.NoError(t, err)
	quotaErr := errors.New("quota metadata unavailable")
	server.backend = &quotaStatusErrorBackendShim{BackendShim: server.backend, err: quotaErr}

	corrupt, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: corruptMemberID,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
	}}, corrupt.Alarms)
	unknown, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType(127),
	})
	require.NoError(t, err)
	require.Empty(t, unknown.Alarms)
	for _, filter := range []etcdserverpb.AlarmType{
		etcdserverpb.AlarmType_NONE,
		etcdserverpb.AlarmType_NOSPACE,
	} {
		_, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET,
			Alarm:  filter,
		})
		require.Equal(t, codes.Unknown, status.Code(callErr))
		require.Equal(t, quotaErr.Error(), status.Convert(callErr).Message())
	}
}

func TestRawGRPCAlarmRejectsUnknownAction(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///alarm-unknown-action",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := etcdserverpb.NewMaintenanceClient(conn).Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_AlarmAction(99),
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.Nil(t, response)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: invalid alarm action", status.Convert(err).Message())
}

func TestClientAlarmListDisarmZeroMemberRoundTrip(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	rawConn, err := grpc.NewClient("passthrough:///alarm-clientv3-zero-member",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rawConn.Close()) })
	rawMaintenance := etcdserverpb.NewMaintenanceClient(rawConn)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deactivateZero := func() {
		_, _ = rawMaintenance.Alarm(context.Background(), &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		})
	}
	deactivateZero()
	t.Cleanup(deactivateZero)

	activated, err := rawMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	require.Equal(t, uint64(0), activated.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, activated.Alarms[0].Alarm)

	listed, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.NotNil(t, listed.Header)
	require.GreaterOrEqual(t, listed.Header.Revision, activated.Header.Revision)
	require.Len(t, listed.Alarms, 1)
	require.Equal(t, uint64(0), listed.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, listed.Alarms[0].Alarm)

	disarmed, err := client.AlarmDisarm(ctx, &clientv3.AlarmMember{
		MemberID: 0,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, disarmed.Alarms, 1)
	require.Equal(t, uint64(0), disarmed.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, disarmed.Alarms[0].Alarm)

	final, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.Empty(t, final.Alarms)
	require.NotNil(t, final.Header)
}

func TestClientAlarmListEmptyHeaderTracksCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "/a1059/alarm-list-empty-header/key", "value")
	require.NoError(t, err)
	alarms, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.Empty(t, alarms.Alarms)
	require.NotNil(t, alarms.Header)
	require.GreaterOrEqual(t, alarms.Header.Revision, put.Header.Revision)
	require.NotZero(t, alarms.Header.ClusterId)
	require.NotZero(t, alarms.Header.MemberId)
	require.Positive(t, alarms.Header.RaftTerm)
}

func TestClientStatusAlarmErrorsRoundTrip(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	rawConn, err := grpc.NewClient("passthrough:///status-alarm-clientv3",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rawConn.Close()) })
	rawMaintenance := etcdserverpb.NewMaintenanceClient(rawConn)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const memberID = uint64(424242)
	activated, err := rawMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	require.Equal(t, memberID, activated.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, activated.Alarms[0].Alarm)
	t.Cleanup(func() {
		_, _ = rawMaintenance.Alarm(context.Background(), &etcdserverpb.AlarmRequest{
			Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
			MemberID: memberID,
			Alarm:    etcdserverpb.AlarmType_NOSPACE,
		})
	})

	active, err := client.Status(ctx, "bufnet")
	require.NoError(t, err)
	require.Equal(t, []string{alarmStatusError(activated.Alarms[0])}, active.Errors)

	disarmed, err := client.AlarmDisarm(ctx, &clientv3.AlarmMember{
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, disarmed.Alarms, 1)
	require.Equal(t, memberID, disarmed.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, disarmed.Alarms[0].Alarm)
	cleared, err := client.Status(ctx, "bufnet")
	require.NoError(t, err)
	require.Empty(t, cleared.Errors)
}

func TestRawGRPCAlarmMemberSetRoundTrip(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///alarm-member-set-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a3418/alarm-member-set-header"), Value: []byte("value"),
	})
	require.NoError(t, err)
	requireAlarmHeader := func(response *etcdserverpb.AlarmResponse) {
		t.Helper()
		require.NotNil(t, response.Header)
		require.Equal(t, seed.Header.Revision, response.Header.Revision)
		require.NotZero(t, response.Header.ClusterId)
		require.NotZero(t, response.Header.MemberId)
		require.Positive(t, response.Header.RaftTerm)
	}
	const (
		first  = uint64(0xa1003)
		second = uint64(0xa1004)
	)
	nospaceMembers := func(response *etcdserverpb.AlarmResponse) []uint64 {
		t.Helper()
		members := make([]uint64, 0, len(response.Alarms))
		for _, alarm := range response.Alarms {
			require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarm.Alarm)
			members = append(members, alarm.MemberID)
		}
		return members
	}
	alarmCall := func(action etcdserverpb.AlarmRequest_AlarmAction, memberID uint64) []uint64 {
		t.Helper()
		resp, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: memberID,
		})
		require.NoError(t, callErr)
		requireAlarmHeader(resp)
		return nospaceMembers(resp)
	}
	list := func() []uint64 {
		t.Helper()
		resp, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NOSPACE,
		})
		require.NoError(t, callErr)
		requireAlarmHeader(resp)
		return nospaceMembers(resp)
	}

	require.Empty(t, alarmCall(etcdserverpb.AlarmRequest_DEACTIVATE, first))
	require.Empty(t, alarmCall(etcdserverpb.AlarmRequest_DEACTIVATE, second))
	require.Equal(t, []uint64{first}, alarmCall(etcdserverpb.AlarmRequest_ACTIVATE, first))
	require.Equal(t, []uint64{first}, alarmCall(etcdserverpb.AlarmRequest_ACTIVATE, first))
	require.Equal(t, []uint64{second}, alarmCall(etcdserverpb.AlarmRequest_ACTIVATE, second))
	require.ElementsMatch(t, []uint64{first, second}, list())
	require.Equal(t, []uint64{first}, alarmCall(etcdserverpb.AlarmRequest_DEACTIVATE, first))
	require.Equal(t, []uint64{second}, list())
	require.Empty(t, alarmCall(etcdserverpb.AlarmRequest_DEACTIVATE, first))
	require.Equal(t, []uint64{second}, alarmCall(etcdserverpb.AlarmRequest_DEACTIVATE, second))
	require.Empty(t, list())
}

func TestRawGRPCAlarmUnknownTypeLifecycleMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///alarm-unknown-type-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a3420/alarm-unknown-type"), Value: []byte("value"),
	})
	require.NoError(t, err)
	const (
		memberID = uint64(0xa342001)
		alarm    = etcdserverpb.AlarmType(127)
	)
	call := func(action etcdserverpb.AlarmRequest_AlarmAction, requestedMember uint64) []uint64 {
		t.Helper()
		response, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action, MemberID: requestedMember, Alarm: alarm,
		})
		require.NoError(t, callErr)
		require.NotNil(t, response.Header)
		require.Equal(t, seed.Header.Revision, response.Header.Revision)
		members := make([]uint64, 0, len(response.Alarms))
		for _, active := range response.Alarms {
			require.Equal(t, alarm, active.Alarm)
			members = append(members, active.MemberID)
		}
		return members
	}
	statusErrors := func() []string {
		t.Helper()
		response, callErr := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
		require.NoError(t, callErr)
		return response.Errors
	}

	require.Equal(t, []uint64{memberID}, call(etcdserverpb.AlarmRequest_ACTIVATE, memberID))
	alarmError := alarmStatusError(&etcdserverpb.AlarmMember{MemberID: memberID, Alarm: alarm})
	require.Equal(t, []string{alarmError}, statusErrors())
	require.Equal(t, []uint64{memberID}, call(etcdserverpb.AlarmRequest_ACTIVATE, memberID))
	require.Equal(t, []uint64{memberID}, call(etcdserverpb.AlarmRequest_GET, 0))
	all, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, all.Alarms)
	require.Equal(t, seed.Header.Revision, all.Header.Revision)
	require.Empty(t, call(etcdserverpb.AlarmRequest_DEACTIVATE, memberID+1))
	require.Equal(t, []string{alarmError}, statusErrors())
	require.Equal(t, []uint64{memberID}, call(etcdserverpb.AlarmRequest_GET, 0))
	require.Equal(t, []uint64{memberID}, call(etcdserverpb.AlarmRequest_DEACTIVATE, memberID))
	require.Empty(t, statusErrors())
	require.Empty(t, call(etcdserverpb.AlarmRequest_GET, 0))
}

func TestRawGRPCCombinedAlarmBlocksWritesAndRecovers(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///combined-alarm-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const (
		noSpaceOwner = uint64(0xa1001)
		corruptOwner = uint64(0xa1002)
	)
	type alarmSummary struct {
		memberID uint64
		alarm    etcdserverpb.AlarmType
	}
	summarizeAlarms := func(alarms []*etcdserverpb.AlarmMember) []alarmSummary {
		summaries := make([]alarmSummary, 0, len(alarms))
		for _, alarm := range alarms {
			summaries = append(summaries, alarmSummary{memberID: alarm.MemberID, alarm: alarm.Alarm})
		}
		return summaries
	}
	key := []byte("/a1001/combined-alarm-client/key")
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)

	noSpace, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	noSpaceSummary := []alarmSummary{{memberID: noSpaceOwner, alarm: etcdserverpb.AlarmType_NOSPACE}}
	require.Equal(t, noSpaceSummary, summarizeAlarms(noSpace.Alarms))
	noSpaceAgain, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	require.Equal(t, noSpaceSummary, summarizeAlarms(noSpaceAgain.Alarms))

	corrupt, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
	})
	require.NoError(t, err)
	corruptSummary := []alarmSummary{{memberID: corruptOwner, alarm: etcdserverpb.AlarmType_CORRUPT}}
	require.Equal(t, corruptSummary, summarizeAlarms(corrupt.Alarms))
	all, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NONE,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []alarmSummary{
		{memberID: noSpaceOwner, alarm: etcdserverpb.AlarmType_NOSPACE},
		{memberID: corruptOwner, alarm: etcdserverpb.AlarmType_CORRUPT},
	}, summarizeAlarms(all.Alarms))
	statusResp, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		alarmStatusError(noSpace.Alarms[0]), alarmStatusError(corrupt.Alarms[0]),
	}, statusResp.Errors)

	_, err = kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	readOnly, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		},
	}}})
	require.NoError(t, err)
	require.Len(t, readOnly.Responses, 1)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked-by-corrupt")})
	requireMaintenanceClientError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	requireMaintenanceClientError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	requireMaintenanceClientError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")

	deactivatedCorrupt, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
	})
	require.NoError(t, err)
	require.Equal(t, corruptSummary, summarizeAlarms(deactivatedCorrupt.Alarms))
	remaining, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NONE,
	})
	require.NoError(t, err)
	require.Equal(t, noSpaceSummary, summarizeAlarms(remaining.Alarms))
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked-by-nospace")})
	requireMaintenanceClientError(t, err, rpctypes.ErrGRPCNoSpace, codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded")
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	require.NoError(t, err)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	requireMaintenanceClientError(t, err, rpctypes.ErrGRPCNoSpace, codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded")

	deactivatedNoSpace, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	require.Equal(t, noSpaceSummary, summarizeAlarms(deactivatedNoSpace.Alarms))
	empty, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NONE,
	})
	require.NoError(t, err)
	require.Empty(t, empty.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func TestRawGRPCHashStabilityAndWriteSensitivity(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///hash-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := []byte("/a1094/hash-client/key")
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)

	first, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.NotZero(t, first.Hash)
	requireRawHashKVHeaderAtOrAfter(t, first.Header, put.Header.Revision)

	second, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.Equal(t, first.Header.Revision, second.Header.Revision)
	require.Equal(t, first.Hash, second.Hash)

	update, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after")})
	require.NoError(t, err)
	third, err := maintenance.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	requireRawHashKVHeaderAtOrAfter(t, third.Header, update.Header.Revision)
	require.Greater(t, third.Header.Revision, second.Header.Revision)
	require.NotEqual(t, second.Hash, third.Hash)
}

func TestRawGRPCHashKVRevisionBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///hashkv-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a999/hashkv-client/key"), Value: []byte("value"),
	})
	require.NoError(t, err)

	negative, err := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: -1})
	require.NoError(t, err)
	require.Equal(t, int64(-1), negative.HashRevision)
	require.Equal(t, uint32(0x40a4756d), negative.Hash)
	requireRawHashKVHeaderAtOrAfter(t, negative.Header, put.Header.Revision)
	require.True(t, negative.CompactRevision == -1 || negative.CompactRevision > 0)

	latest, err := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, latest.HashRevision, put.Header.Revision)
	requireRawHashKVHeaderAtOrAfter(t, latest.Header, put.Header.Revision)
	require.True(t, latest.CompactRevision == -1 || latest.CompactRevision > 0)

	current, err := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: put.Header.Revision})
	require.NoError(t, err)
	require.Equal(t, put.Header.Revision, current.HashRevision)
	requireRawHashKVHeaderAtOrAfter(t, current.Header, put.Header.Revision)
	require.True(t, current.CompactRevision == -1 || current.CompactRevision > 0)

	for _, revision := range []int64{put.Header.Revision + 100, math.MaxInt64} {
		_, hashErr := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: revision})
		requireMaintenanceClientError(t, hashErr, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
	}

	update, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a999/hashkv-client/key"), Value: []byte("updated"),
	})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: update.Header.Revision})
	require.NoError(t, err)
	_, hashErr := maintenance.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: put.Header.Revision})
	requireMaintenanceClientError(t, hashErr, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
}

func TestClientHashKVRevisionBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "/a1044/hashkv-client/key", "value")
	require.NoError(t, err)

	negative, err := client.HashKV(ctx, "bufnet", -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), negative.HashRevision)
	require.Equal(t, uint32(0x40a4756d), negative.Hash)
	requireRawHashKVHeaderAtOrAfter(t, negative.Header, put.Header.Revision)
	require.True(t, negative.CompactRevision == -1 || negative.CompactRevision > 0)

	latest, err := client.HashKV(ctx, "bufnet", 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, latest.HashRevision, put.Header.Revision)
	requireRawHashKVHeaderAtOrAfter(t, latest.Header, put.Header.Revision)
	require.True(t, latest.CompactRevision == -1 || latest.CompactRevision > 0)

	current, err := client.HashKV(ctx, "bufnet", put.Header.Revision)
	require.NoError(t, err)
	require.Equal(t, put.Header.Revision, current.HashRevision)
	requireRawHashKVHeaderAtOrAfter(t, current.Header, put.Header.Revision)
	require.True(t, current.CompactRevision == -1 || current.CompactRevision > 0)

	for _, revision := range []int64{put.Header.Revision + 100, math.MaxInt64} {
		_, hashErr := client.HashKV(ctx, "bufnet", revision)
		requireClientHashKVError(t, hashErr, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)
	}

	update, err := client.Put(ctx, "/a1044/hashkv-client/key", "updated")
	require.NoError(t, err)
	_, err = client.Compact(ctx, update.Header.Revision)
	require.NoError(t, err)
	_, hashErr := client.HashKV(ctx, "bufnet", put.Header.Revision)
	requireClientHashKVError(t, hashErr, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)
}

func TestClientHashKVLatestHeaderTracksHashedSnapshotUnderWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "/a1111/hashkv-snapshot/"
	base, err := client.Put(ctx, prefix+"key", "base")
	require.NoError(t, err)

	writerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 64; i++ {
			if _, putErr := client.Put(ctx, prefix+"key", strconv.Itoa(i)); putErr != nil {
				writerDone <- putErr
				return
			}
		}
		writerDone <- nil
	}()

	for i := 0; i < 64; i++ {
		resp, hashErr := client.HashKV(ctx, "bufnet", 0)
		require.NoError(t, hashErr)
		requireRawHashKVHeaderAtOrAfter(t, resp.Header, base.Header.Revision)
		require.Equal(t, resp.HashRevision, resp.Header.Revision)
	}
	require.NoError(t, <-writerDone)
}

func TestClientHashKVStableAcrossCompactionProgress(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := "/a1112/hashkv-compact/key"
	for i := 0; i < 8; i++ {
		_, err = client.Put(ctx, key, strconv.Itoa(i))
		require.NoError(t, err)
	}
	before, err := client.HashKV(ctx, "bufnet", 0)
	require.NoError(t, err)
	require.Equal(t, before.Header.Revision, before.HashRevision)

	_, err = client.Compact(ctx, before.HashRevision)
	require.NoError(t, err)
	afterLogical, err := client.HashKV(ctx, "bufnet", 0)
	require.NoError(t, err)
	require.Equal(t, before.HashRevision, afterLogical.CompactRevision)
	require.NotEqual(t, before.Hash, afterLogical.Hash)

	for i := 0; i < 4; i++ {
		time.Sleep(25 * time.Millisecond)
		afterProgress, hashErr := client.HashKV(ctx, "bufnet", 0)
		require.NoError(t, hashErr)
		require.Equal(t, afterLogical.Hash, afterProgress.Hash)
		require.Equal(t, afterLogical.CompactRevision, afterProgress.CompactRevision)
		require.Equal(t, afterProgress.Header.Revision, afterProgress.HashRevision)
		require.GreaterOrEqual(t, afterProgress.HashRevision, afterProgress.CompactRevision)
	}
}

func requireRawHashKVHeaderAtOrAfter(t *testing.T, header *etcdserverpb.ResponseHeader, revision int64) {
	t.Helper()
	require.NotNil(t, header)
	require.NotZero(t, header.ClusterId)
	require.NotZero(t, header.MemberId)
	require.Positive(t, header.RaftTerm)
	require.GreaterOrEqual(t, header.Revision, revision)
}

func requireClientHashKVError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, message)
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireMaintenanceClientError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requirePlatformReplacementError(t *testing.T, err error, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(codes.Unimplemented, message).Error())
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
