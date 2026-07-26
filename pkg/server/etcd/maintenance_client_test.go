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
	"google.golang.org/grpc/credentials/insecure"
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
