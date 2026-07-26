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
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

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
		require.GreaterOrEqual(t, resp.Header.Revision, put.Header.Revision)
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
	require.ElementsMatch(t, []string{noSpace.Alarms[0].String(), corrupt.Alarms[0].String()}, statusResp.Errors)

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
	require.Equal(t, codes.DataLoss, status.Code(err))
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	require.Equal(t, codes.DataLoss, status.Code(err))
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.Equal(t, codes.DataLoss, status.Code(err))

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
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	require.NoError(t, err)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

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
		require.Error(t, hashErr)
		require.Equal(t, codes.OutOfRange, status.Code(hashErr))
		require.Equal(t, "etcdserver: mvcc: required revision is a future revision", status.Convert(hashErr).Message())
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
