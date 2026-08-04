package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type automaticQuotaOutcome struct {
	QuotaReported             bool
	OversizedCode             string
	OversizedTypedNoSpace     bool
	OversizedKeyAbsent        bool
	AlarmOwnedByMember        bool
	ReadAllowedWhileAlarmed   bool
	LeaseGrantCode            string
	LeaseGrantTypedNoSpace    bool
	DeleteAllowedWhileAlarmed bool
	AlarmStickyAfterDelete    bool
	CompactAndDefragSucceeded bool
	DisarmReturnedAlarm       bool
	RecoveryPutSucceeded      bool
}

func TestAutomaticQuotaAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	referenceEndpoint := os.Getenv("REFERENCE_AUTOMATIC_QUOTA_ENDPOINT")
	kubeBrainEndpoint := os.Getenv("KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT")
	if referenceEndpoint == "" || kubeBrainEndpoint == "" {
		t.Skip("set REFERENCE_AUTOMATIC_QUOTA_ENDPOINT and KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT")
	}
	referenceQuota := requiredPositiveInt64Env(t, "REFERENCE_AUTOMATIC_QUOTA_BYTES")
	kubeBrainQuota := requiredPositiveInt64Env(t, "KUBEBRAIN_AUTOMATIC_QUOTA_BYTES")
	referenceFill := requiredPositiveIntEnv(t, "REFERENCE_AUTOMATIC_QUOTA_FILL_BYTES")
	kubeBrainFill := requiredPositiveIntEnv(t, "KUBEBRAIN_AUTOMATIC_QUOTA_FILL_BYTES")

	want := runAutomaticQuotaScenario(t, referenceEndpoint, "reference", referenceQuota, referenceFill)
	require.Equal(t, automaticQuotaOutcome{
		QuotaReported:             true,
		OversizedCode:             codes.Unknown.String(),
		OversizedTypedNoSpace:     true,
		OversizedKeyAbsent:        true,
		AlarmOwnedByMember:        true,
		ReadAllowedWhileAlarmed:   true,
		LeaseGrantCode:            codes.Unknown.String(),
		LeaseGrantTypedNoSpace:    true,
		DeleteAllowedWhileAlarmed: true,
		AlarmStickyAfterDelete:    true,
		CompactAndDefragSucceeded: true,
		DisarmReturnedAlarm:       true,
		RecoveryPutSucceeded:      true,
	}, want)
	require.Equal(t, want,
		runAutomaticQuotaScenario(t, kubeBrainEndpoint, "kubebrain", kubeBrainQuota, kubeBrainFill))
}

func requiredPositiveInt64Env(t *testing.T, name string) int64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		t.Skipf("set %s", name)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err, name)
	require.Positive(t, value, name)
	return value
}

func requiredPositiveIntEnv(t *testing.T, name string) int {
	t.Helper()
	value := requiredPositiveInt64Env(t, name)
	require.LessOrEqual(t, value, int64(^uint(0)>>1), name)
	return int(value)
}

func runAutomaticQuotaScenario(
	t *testing.T,
	endpoint, instance string,
	expectedQuota int64,
	fillBytes int,
) automaticQuotaOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/automatic-quota/%s/%d/", instance, time.Now().UnixNano())
	smallKey := prefix + "small"
	oversizedKey := prefix + "oversized"
	recoveryKey := prefix + "recovery"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if alarms, listErr := client.AlarmList(cleanupCtx); listErr == nil {
			for _, alarm := range alarms.Alarms {
				if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
					_, _ = client.AlarmDisarm(cleanupCtx, &clientv3.AlarmMember{
						MemberID: alarm.MemberID, Alarm: alarm.Alarm,
					})
				}
			}
		}
		_, deleteErr := client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, deleteErr)
		remaining, getErr := client.Get(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
		alarms, listErr := client.AlarmList(cleanupCtx)
		require.NoError(t, listErr)
		for _, alarm := range alarms.Alarms {
			require.NotEqual(t, etcdserverpb.AlarmType_NOSPACE, alarm.Alarm)
		}
	})

	statusResponse, err := client.Status(ctx, endpoint)
	require.NoError(t, err)
	require.NotNil(t, statusResponse.Header)
	memberID := statusResponse.Header.MemberId
	require.NotZero(t, memberID)
	outcome := automaticQuotaOutcome{QuotaReported: statusResponse.DbSizeQuota == expectedQuota}

	_, err = client.Put(ctx, smallKey, strings.Repeat("s", 64))
	require.NoError(t, err)
	_, oversizedErr := client.Put(ctx, oversizedKey, strings.Repeat("x", fillBytes))
	outcome.OversizedCode = status.Code(oversizedErr).String()
	outcome.OversizedTypedNoSpace = errors.Is(oversizedErr, rpctypes.ErrNoSpace)
	oversizedRead, err := client.Get(ctx, oversizedKey)
	require.NoError(t, err)
	outcome.OversizedKeyAbsent = oversizedRead.Count == 0

	var noSpaceAlarm *etcdserverpb.AlarmMember
	require.Eventually(t, func() bool {
		alarms, listErr := client.AlarmList(ctx)
		if listErr != nil {
			return false
		}
		for _, alarm := range alarms.Alarms {
			if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
				noSpaceAlarm = alarm
				return alarm.MemberID == memberID
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond)
	outcome.AlarmOwnedByMember = noSpaceAlarm != nil && noSpaceAlarm.MemberID == memberID

	smallRead, err := client.Get(ctx, smallKey)
	require.NoError(t, err)
	outcome.ReadAllowedWhileAlarmed = smallRead.Count == 1
	_, leaseErr := client.Grant(ctx, 30)
	outcome.LeaseGrantCode = status.Code(leaseErr).String()
	outcome.LeaseGrantTypedNoSpace = errors.Is(leaseErr, rpctypes.ErrNoSpace)
	deleted, deleteErr := client.Delete(ctx, smallKey)
	require.NoError(t, deleteErr)
	require.NotNil(t, deleted.Header)
	outcome.DeleteAllowedWhileAlarmed = deleted.Deleted == 1
	alarmsAfterDelete, err := client.AlarmList(ctx)
	require.NoError(t, err)
	for _, alarm := range alarmsAfterDelete.Alarms {
		if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE && alarm.MemberID == memberID {
			outcome.AlarmStickyAfterDelete = true
		}
	}

	compactRevision := deleted.Header.Revision
	_, compactErr := client.Compact(ctx, compactRevision, clientv3.WithCompactPhysical())
	_, defragErr := client.Defragment(ctx, endpoint)
	outcome.CompactAndDefragSucceeded = compactErr == nil && defragErr == nil
	disarmed, disarmErr := client.AlarmDisarm(ctx, &clientv3.AlarmMember{
		MemberID: noSpaceAlarm.MemberID, Alarm: noSpaceAlarm.Alarm,
	})
	require.NoError(t, disarmErr)
	outcome.DisarmReturnedAlarm = len(disarmed.Alarms) == 1 &&
		disarmed.Alarms[0].Alarm == etcdserverpb.AlarmType_NOSPACE &&
		disarmed.Alarms[0].MemberID == memberID
	_, recoveryErr := client.Put(ctx, recoveryKey, "recovered")
	outcome.RecoveryPutSucceeded = recoveryErr == nil
	return outcome
}
