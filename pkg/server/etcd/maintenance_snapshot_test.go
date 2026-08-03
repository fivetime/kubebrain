// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestMaintenanceSnapshotStatePreservesCurrentKVAuthAndAlarms(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	put, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{Key: []byte("/allowed/snapshot"), Value: []byte("value")})
	require.NoError(t, err)
	_, err = server.backend.ArmNoSpace(context.Background(), 29)
	require.NoError(t, err)
	require.NoError(t, server.backend.ArmCorrupt(context.Background(), 31))
	_, err = server.mutateGenericAlarm(context.Background(), etcdserverpb.AlarmType(127), 37, true)
	require.NoError(t, err)

	state, err := server.snapshotState(context.Background())
	require.NoError(t, err)
	require.Equal(t, put.Header.Revision, state.Revision)
	require.Len(t, state.Records, 1)
	require.Equal(t, []byte("/allowed/snapshot"), state.Records[0].Key)
	require.Equal(t, []byte("value"), state.Records[0].Value)
	require.True(t, state.Auth.Enabled)
	require.Positive(t, state.Auth.Revision)
	require.ElementsMatch(t, []string{"alice", "root"}, []string{string(state.Auth.Users[0].Name), string(state.Auth.Users[1].Name)})
	require.ElementsMatch(t, []string{"allowed", "root"}, []string{string(state.Auth.Roles[0].Name), string(state.Auth.Roles[1].Name)})
	require.ElementsMatch(t, []*etcdserverpb.AlarmMember{
		{MemberID: 29, Alarm: etcdserverpb.AlarmType_NOSPACE},
		{MemberID: 31, Alarm: etcdserverpb.AlarmType_CORRUPT},
		{MemberID: 37, Alarm: etcdserverpb.AlarmType(127)},
	}, state.Alarms)
}
