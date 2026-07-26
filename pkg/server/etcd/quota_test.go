// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package etcd

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newQuotaRPCServer(t *testing.T, quota int64) *RPCServer {
	t.Helper()
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "quota-test-peer",
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       quota,
	}, metrics)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	server := New(b, metrics, testPeerService{isLeader: true})
	t.Cleanup(func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	})
	return server
}

func TestQuotaRPCNoSpaceRecoveryAndStatus(t *testing.T) {
	server := newQuotaRPCServer(t, 6)
	ctx := context.Background()

	initial, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(1), initial.DbSize)
	require.Equal(t, int64(6), initial.DbSizeQuota)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("123")})
	require.NoError(t, err)
	statusResp, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(6), statusResp.DbSize)
	require.Equal(t, statusResp.DbSize, statusResp.DbSizeInUse)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("x"), Value: []byte("y")})
	requireQuotaNoSpaceError(t, err)
	alarmResp, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Len(t, alarmResp.Alarms, 1)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarmResp.Alarms[0].Alarm)
	statusResp, err = server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{alarmResp.Alarms[0].String()}, statusResp.Errors)

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	requireQuotaNoSpaceError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("1")})
	requireQuotaNoSpaceError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte("z"), Value: []byte("1")},
		},
	}}})
	requireQuotaNoSpaceError(t, err)

	deactivate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmResp.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, deactivate.Alarms, 1, "etcd allows disarm while usage is at the limit")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("1")})
	requireQuotaNoSpaceError(t, err)
	alarmResp, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Len(t, alarmResp.Alarms, 1, "the first Put after disarm must re-arm NOSPACE")

	deleteTxn, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte("key")},
		},
	}}})
	require.NoError(t, err)
	require.Len(t, deleteTxn.Responses, 1)
	deactivate, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmResp.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, deactivate.Alarms, 1)
	alarmResp, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Empty(t, alarmResp.Alarms)
	statusResp, err = server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Empty(t, statusResp.Errors)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.NoError(t, err)
	require.Empty(t, rangeResp.Kvs)

	activate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activate.Alarms, 1)
	require.Zero(t, activate.Alarms[0].MemberID)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("m"), Value: []byte("x")})
	requireQuotaNoSpaceError(t, err)
	deactivate, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: activate.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, deactivate.Alarms, 1)
	deactivate, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: activate.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, deactivate.Alarms)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("m"), Value: []byte("x")})
	require.NoError(t, err)
}

func TestQuotaRPCAlarmMutationNoOpsMatchEtcd(t *testing.T) {
	server := newQuotaRPCServer(t, 6)
	ctx := context.Background()

	for _, action := range []etcdserverpb.AlarmRequest_AlarmAction{
		etcdserverpb.AlarmRequest_ACTIVATE,
		etcdserverpb.AlarmRequest_DEACTIVATE,
	} {
		response, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action:   action,
			MemberID: 123,
			Alarm:    etcdserverpb.AlarmType_NONE,
		})
		require.NoError(t, err)
		require.Empty(t, response.Alarms)
	}

	activate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activate.Alarms, 1)
	wrongMember := ^uint64(0)
	deactivate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: wrongMember,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, deactivate.Alarms)
	list, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Len(t, list.Alarms, 1)

	foreignMember := activate.Alarms[0].MemberID + 1
	server.staticMembers = []*etcdserverpb.Member{{ID: foreignMember}}
	deactivate, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: foreignMember,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, deactivate.Alarms, "a known member that does not own the alarm must not disarm it")
	deactivate, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: activate.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, activate.Alarms, deactivate.Alarms)
}

func TestQuotaRPCZeroMemberAlarmRoundTripMatchesEtcd(t *testing.T) {
	server := newQuotaRPCServer(t, 0)
	ctx := context.Background()

	activate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, noSpaceAlarmMembers(activate.Alarms))

	list, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, noSpaceAlarmMembers(list.Alarms))

	deactivate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, noSpaceAlarmMembers(deactivate.Alarms))
	final, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, final.Alarms)
}

func noSpaceAlarmMembers(alarms []*etcdserverpb.AlarmMember) []uint64 {
	members := make([]uint64, 0, len(alarms))
	for _, alarm := range alarms {
		if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
			members = append(members, alarm.MemberID)
		}
	}
	return members
}

func TestQuotaRPCAlarmActivationPersistsRequestedMember(t *testing.T) {
	server := newQuotaRPCServer(t, 6)
	ctx := context.Background()
	const requestedOwner, secondOwner = uint64(424242), uint64(424243)

	activate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: requestedOwner,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: requestedOwner,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	}}, activate.Alarms)
	again, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: requestedOwner,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, activate.Alarms, again.Alarms)

	second, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: secondOwner,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, secondOwner, second.Alarms[0].MemberID)

	list, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{
		{MemberID: requestedOwner, Alarm: etcdserverpb.AlarmType_NOSPACE},
		{MemberID: secondOwner, Alarm: etcdserverpb.AlarmType_NOSPACE},
	}, list.Alarms)

	deactivate, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: requestedOwner,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, activate.Alarms, deactivate.Alarms)
	list, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Equal(t, second.Alarms, list.Alarms)
	deactivateAgain, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: requestedOwner,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, deactivateAgain.Alarms)
}

func TestQuotaRPCManualActivationCapsWritesWithoutConfiguredQuota(t *testing.T) {
	server := newQuotaRPCServer(t, 0)
	ctx := context.Background()
	const memberID uint64 = 424242
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("seed"), Value: []byte("value")})
	require.NoError(t, err)

	activated, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, memberID, activated.Alarms[0].MemberID)

	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("seed")})
	require.NoError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("seed")},
		},
	}}})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("blocked"), Value: []byte("value")})
	requireQuotaNoSpaceError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte("blocked-txn"), Value: []byte("value")},
		},
	}}})
	requireQuotaNoSpaceError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: []byte("seed"), Result: etcdserverpb.Compare_EQUAL,
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("blocked-failure"), Value: []byte("value")},
			},
		}},
	})
	requireQuotaNoSpaceError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	requireQuotaNoSpaceError(t, err)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID})
	require.NoError(t, err)
	require.Equal(t, lease.ID, ttl.ID)

	_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("blocked")})
	require.NoError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte("seed")},
		},
	}}})
	require.NoError(t, err)
	_, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("restored"), Value: []byte("value")})
	require.NoError(t, err)
}

func TestTxnContainsPutAcrossBranchesAndNesting(t *testing.T) {
	put := func() *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte("key")},
		}}
	}
	read := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte("key")},
	}}
	require.False(t, txnContainsPut(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{read}}))
	require.True(t, txnContainsPut(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{put()}}))
	require.True(t, txnContainsPut(&etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{put()}}))
	require.True(t, txnContainsPut(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
			Failure: []*etcdserverpb.RequestOp{put()},
		}},
	}}}))
}

func requireQuotaNoSpaceError(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, rpctypes.ErrGRPCNoSpace)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: mvcc: database space exceeded", status.Convert(err).Message())
}
