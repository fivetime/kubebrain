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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type quotaAlarmOutcome struct {
	Name       string
	Code       codes.Code
	AlarmCount int
}

type quotaAlarmLeaseRecoveryOutcome struct {
	GrantCode              codes.Code
	ExplicitRevokeDeleted  bool
	NaturalExpiryDeleted   bool
	RenewTTL               int64
	RenewedPastOldDeadline bool
	AlarmRemainedSticky    bool
}

func TestQuotaAlarmLeaseRecoveryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_QUOTA_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_QUOTA_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_QUOTA_ETCD_ENDPOINT and KUBEBRAIN_QUOTA_ENDPOINT")
	}

	want := runQuotaAlarmLeaseRecoveryScenario(t, reference)
	require.Equal(t, quotaAlarmLeaseRecoveryOutcome{
		GrantCode:              codes.ResourceExhausted,
		ExplicitRevokeDeleted:  true,
		NaturalExpiryDeleted:   true,
		RenewTTL:               3,
		RenewedPastOldDeadline: true,
		AlarmRemainedSticky:    true,
	}, want)
	require.Equal(t, want, runQuotaAlarmLeaseRecoveryScenario(t, kubebrain))
}

func TestQuotaAlarmCappedStateDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_QUOTA_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_QUOTA_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_QUOTA_ETCD_ENDPOINT and KUBEBRAIN_QUOTA_ENDPOINT")
	}

	want := runQuotaAlarmCappedScenario(t, reference)
	require.Equal(t, []quotaAlarmOutcome{
		{Name: "activate-none", Code: codes.OK},
		{Name: "deactivate-none", Code: codes.OK},
		{Name: "activate", Code: codes.OK, AlarmCount: 1},
		{Name: "deactivate-wrong-member", Code: codes.OK},
		{Name: "put-after-wrong-member", Code: codes.ResourceExhausted},
		{Name: "shrinking-put", Code: codes.ResourceExhausted},
		{Name: "put-in-unchosen-branch", Code: codes.ResourceExhausted},
		{Name: "delete-only-txn", Code: codes.OK},
		{Name: "read-only-txn", Code: codes.OK},
		{Name: "lease-grant", Code: codes.ResourceExhausted},
		{Name: "deactivate", Code: codes.OK, AlarmCount: 1},
		{Name: "deactivate-again", Code: codes.OK},
		{Name: "put-after-disarm", Code: codes.OK},
	}, want)
	require.Equal(t, want, runQuotaAlarmCappedScenario(t, kubebrain))
}

func TestQuotaAlarmCrossEndpointDisarm(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_MULTI_QUOTA_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_MULTI_QUOTA_ENDPOINTS to three comma-separated replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.Len(t, endpoints, 3)

	connections := make([]*grpc.ClientConn, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"))
		connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		connections = append(connections, connection)
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			require.NoError(t, connection.Close())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(connections[0])
	first := etcdserverpb.NewMaintenanceClient(connections[0])
	second := etcdserverpb.NewMaintenanceClient(connections[1])
	third := etcdserverpb.NewMaintenanceClient(connections[2])
	key := []byte(testPrefix(t) + "/cross-endpoint-alarm")
	_, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if alarms, getErr := first.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		}); getErr == nil {
			for _, alarm := range alarms.Alarms {
				_, _ = first.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
					Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
					MemberID: alarm.MemberID,
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
			}
		}
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	const requestedMemberID uint64 = 424242
	activated, err := first.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: requestedMemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	listed, err := second.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, listed.Alarms, 1)
	require.Equal(t, requestedMemberID, activated.Alarms[0].MemberID)
	require.Equal(t, requestedMemberID, listed.Alarms[0].MemberID)
	require.Equal(t, activated.Alarms, listed.Alarms, "alarm owner must remain stable across serving replicas")

	wrong, err := third.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: ^uint64(0),
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, wrong.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	disarmed, err := third.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: listed.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, listed.Alarms, disarmed.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func runQuotaAlarmCappedScenario(t *testing.T, endpoint string) []quotaAlarmOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := []byte(testPrefix(t) + "/quota-capped")
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("long-value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if alarms, getErr := maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		}); getErr == nil {
			for _, alarm := range alarms.Alarms {
				_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
					Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
					MemberID: alarm.MemberID,
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
			}
		}
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	outcomes := make([]quotaAlarmOutcome, 0, 13)
	record := func(name string, response *etcdserverpb.AlarmResponse, callErr error) {
		outcome := quotaAlarmOutcome{Name: name, Code: status.Code(callErr)}
		if response != nil {
			outcome.AlarmCount = len(response.Alarms)
		}
		outcomes = append(outcomes, outcome)
	}

	response, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: 123,
		Alarm:    etcdserverpb.AlarmType_NONE,
	})
	record("activate-none", response, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: 123,
		Alarm:    etcdserverpb.AlarmType_NONE,
	})
	record("deactivate-none", response, err)

	const requestedAlarmMemberID uint64 = 424242
	activate, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: requestedAlarmMemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("activate", activate, err)
	require.Len(t, activate.GetAlarms(), 1)
	require.Equal(t, requestedAlarmMemberID, activate.Alarms[0].MemberID)
	alarmMemberID := activate.Alarms[0].MemberID

	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmMemberID + 1,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate-wrong-member", response, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("x")})
	record("put-after-wrong-member", nil, err)

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("x")})
	record("shrinking-put", nil, err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Result:      etcdserverpb.Compare_NOT_EQUAL,
			Target:      etcdserverpb.Compare_VERSION,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("x")},
			},
		}},
	})
	record("put-in-unchosen-branch", nil, err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	record("delete-only-txn", nil, err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		},
	}}})
	record("read-only-txn", nil, err)

	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant", nil, err)

	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmMemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate", response, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmMemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate-again", response, err)

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	record("put-after-disarm", nil, err)
	return outcomes
}

func runQuotaAlarmLeaseRecoveryScenario(t *testing.T, endpoint string) quotaAlarmLeaseRecoveryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResponse.Header.MemberId
	require.NotZero(t, memberID)
	prefix := testPrefix(t) + "/quota-lease-recovery"
	expiryKey := []byte(prefix + "/expiry")
	renewKey := []byte(prefix + "/renew")
	revokeKey := []byte(prefix + "/revoke")
	expiry, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2})
	require.NoError(t, err)
	renew, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 3})
	require.NoError(t, err)
	revoke, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	for _, fixture := range []struct {
		key   []byte
		lease int64
	}{{expiryKey, expiry.ID}, {renewKey, renew.ID}, {revokeKey, revoke.ID}} {
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: fixture.key, Value: []byte("leased"), Lease: fixture.lease})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: memberID,
		})
		for _, key := range [][]byte{expiryKey, renewKey, revokeKey} {
			_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
		}
	})
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: memberID,
	})
	require.NoError(t, err)

	_, grantErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	outcome := quotaAlarmLeaseRecoveryOutcome{GrantCode: status.Code(grantErr)}
	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: revoke.ID})
	require.NoError(t, err)
	revokedRead, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: revokeKey})
	require.NoError(t, err)
	outcome.ExplicitRevokeDeleted = len(revokedRead.Kvs) == 0

	time.Sleep(1500 * time.Millisecond)
	renewStream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, renewStream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: renew.ID}))
	renewResponse, err := renewStream.Recv()
	require.NoError(t, err)
	require.NoError(t, renewStream.CloseSend())
	outcome.RenewTTL = renewResponse.TTL
	require.Eventually(t, func() bool {
		response, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: expiryKey})
		return rangeErr == nil && len(response.Kvs) == 0
	}, 10*time.Second, 100*time.Millisecond)
	outcome.NaturalExpiryDeleted = true

	time.Sleep(2 * time.Second)
	renewedRead, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: renewKey})
	require.NoError(t, err)
	outcome.RenewedPastOldDeadline = len(renewedRead.Kvs) == 1
	alarms, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	outcome.AlarmRemainedSticky = len(alarms.Alarms) == 1 && alarms.Alarms[0].MemberID == memberID
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: memberID,
	})
	require.NoError(t, err)
	return outcome
}
