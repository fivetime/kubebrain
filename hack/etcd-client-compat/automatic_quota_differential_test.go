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

	"github.com/stretchr/testify/assert"
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
	AlarmCount                int
	ReadAllowedWhileAlarmed   bool
	LeaseGrantCode            string
	LeaseGrantTypedNoSpace    bool
	DeleteAllowedWhileAlarmed bool
	AlarmStickyAfterDelete    bool
	CompactAndDefragSucceeded bool
	DisarmReturnedAlarm       bool
	RecoveryPutSucceeded      bool
}

// automaticQuotaResponseAdmission binds the destructive quota lifecycle to one
// logical cluster and a non-regressing revision stream. The runner is commonly
// pointed at a Kubernetes Service; without this fence a selector change could
// splice responses from two pristine disposable instances into one false-green
// lifecycle.
type automaticQuotaResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *automaticQuotaResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) error {
	if header == nil || header.GetClusterId() == 0 || header.GetMemberId() == 0 || header.GetRevision() <= 0 {
		return errors.New("automatic quota response returned an invalid header")
	}
	if a.clusterID != 0 && header.GetClusterId() != a.clusterID {
		return fmt.Errorf("automatic quota response cluster ID changed from %d to %d", a.clusterID, header.GetClusterId())
	}
	if header.GetRevision() < a.revision {
		return fmt.Errorf("automatic quota response revision regressed from %d to %d", a.revision, header.GetRevision())
	}
	if mutation && header.GetRevision() <= a.revision {
		return fmt.Errorf("automatic quota mutation response revision did not advance from %d", a.revision)
	}
	if a.clusterID == 0 {
		a.clusterID = header.GetClusterId()
	}
	a.revision = header.GetRevision()
	return nil
}

// Keep the synthetic value below etcd's default max-request-bytes after
// protobuf framing. The scenario is meant to exercise quota admission, not the
// earlier client/transport request-size boundary.
const automaticQuotaMaxFillBytes = 1_500_000

func TestAutomaticQuotaResponseAdmissionRejectsIdentityAndRevisionDrift(t *testing.T) {
	header := func(clusterID, memberID uint64, revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID, Revision: revision}
	}

	admission := &automaticQuotaResponseAdmission{}
	require.NoError(t, admission.admitHeader(header(7, 11, 3), false))
	require.NoError(t, admission.admitHeader(header(7, 12, 4), true),
		"load balancing may change the serving member within one cluster")
	require.NoError(t, admission.admitHeader(header(7, 11, 4), false))
	require.ErrorContains(t, admission.admitHeader(header(8, 11, 5), false), "cluster ID changed")
	require.ErrorContains(t, admission.admitHeader(header(7, 11, 3), false), "revision regressed")
	require.ErrorContains(t, admission.admitHeader(header(7, 11, 4), true), "did not advance")

	for name, malformed := range map[string]*etcdserverpb.ResponseHeader{
		"nil":           nil,
		"zero cluster":  header(0, 11, 1),
		"zero member":   header(7, 0, 1),
		"zero revision": header(7, 11, 0),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&automaticQuotaResponseAdmission{}).admitHeader(malformed, false))
		})
	}
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
	require.Greater(t, int64(referenceFill), referenceQuota,
		"reference fill must exceed quota to exercise automatic NOSPACE")
	require.Greater(t, int64(kubeBrainFill), kubeBrainQuota,
		"KubeBrain fill must exceed quota to exercise automatic NOSPACE")
	require.LessOrEqual(t, referenceFill, automaticQuotaMaxFillBytes,
		"reference fill must stay below the default request-size boundary")
	require.LessOrEqual(t, kubeBrainFill, automaticQuotaMaxFillBytes,
		"KubeBrain fill must stay below the default request-size boundary")

	want := runAutomaticQuotaScenario(t, referenceEndpoint, "reference", referenceQuota, referenceFill)
	require.Equal(t, automaticQuotaOutcome{
		QuotaReported:             true,
		OversizedCode:             codes.Unknown.String(),
		OversizedTypedNoSpace:     true,
		OversizedKeyAbsent:        true,
		AlarmOwnedByMember:        true,
		AlarmCount:                1,
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
	admission := &automaticQuotaResponseAdmission{}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if alarms, listErr := client.AlarmList(cleanupCtx); listErr == nil {
			require.NoError(t, admission.admitHeader(alarms.Header, false))
			for _, alarm := range alarms.Alarms {
				if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
					disarmed, disarmErr := client.AlarmDisarm(cleanupCtx, &clientv3.AlarmMember{
						MemberID: alarm.MemberID, Alarm: alarm.Alarm,
					})
					require.NoError(t, disarmErr)
					require.NoError(t, admission.admitHeader(disarmed.Header, false))
				}
			}
		}
		deleted, deleteErr := client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, deleteErr)
		require.NoError(t, admission.admitHeader(deleted.Header, false))
		require.Empty(t, deleted.PrevKvs)
		remaining, getErr := client.Get(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, getErr)
		require.NoError(t, admission.admitHeader(remaining.Header, false))
		require.Zero(t, remaining.Count)
		require.Empty(t, remaining.Kvs)
		require.False(t, remaining.More)
		alarms, listErr := client.AlarmList(cleanupCtx)
		require.NoError(t, listErr)
		require.NoError(t, admission.admitHeader(alarms.Header, false))
		for _, alarm := range alarms.Alarms {
			require.NotEqual(t, etcdserverpb.AlarmType_NOSPACE, alarm.Alarm)
		}
	})

	statusResponse, err := client.Status(ctx, endpoint)
	require.NoError(t, err)
	require.NoError(t, admission.admitHeader(statusResponse.Header, false))
	require.NotZero(t, statusResponse.Leader)
	require.Empty(t, statusResponse.Errors)
	memberID := statusResponse.Header.MemberId
	outcome := automaticQuotaOutcome{QuotaReported: statusResponse.DbSizeQuota == expectedQuota}

	smallPut, err := client.Put(ctx, smallKey, strings.Repeat("s", 64))
	require.NoError(t, err)
	require.Nil(t, smallPut.PrevKv)
	require.NoError(t, admission.admitHeader(smallPut.Header, true))
	_, oversizedErr := client.Put(ctx, oversizedKey, strings.Repeat("x", fillBytes))
	outcome.OversizedCode = status.Code(oversizedErr).String()
	outcome.OversizedTypedNoSpace = errors.Is(oversizedErr, rpctypes.ErrNoSpace)
	oversizedRead, err := client.Get(ctx, oversizedKey)
	require.NoError(t, err)
	require.NoError(t, admission.admitHeader(oversizedRead.Header, false))
	require.False(t, oversizedRead.More)
	require.Empty(t, oversizedRead.Kvs)
	outcome.OversizedKeyAbsent = oversizedRead.Count == 0

	var noSpaceAlarm *etcdserverpb.AlarmMember
	var observedAlarmMembers []uint64
	var alarmAdmissionErr error
	foundOwnedAlarm := assert.Eventually(t, func() bool {
		alarms, listErr := client.AlarmList(ctx)
		if listErr != nil {
			return false
		}
		if alarmAdmissionErr = admission.admitHeader(alarms.Header, false); alarmAdmissionErr != nil {
			return true
		}
		observedAlarmMembers = observedAlarmMembers[:0]
		for _, alarm := range alarms.Alarms {
			if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
				observedAlarmMembers = append(observedAlarmMembers, alarm.MemberID)
				if alarm.MemberID == memberID {
					noSpaceAlarm = alarm
				}
			}
		}
		return noSpaceAlarm != nil
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, alarmAdmissionErr)
	require.Truef(t, foundOwnedAlarm,
		"NOSPACE owner mismatch: status member=%d observed alarm members=%v", memberID, observedAlarmMembers)
	outcome.AlarmOwnedByMember = noSpaceAlarm != nil && noSpaceAlarm.MemberID == memberID
	alarmsAfterActivation, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.NoError(t, admission.admitHeader(alarmsAfterActivation.Header, false))
	for _, alarm := range alarmsAfterActivation.Alarms {
		if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
			outcome.AlarmCount++
		}
	}

	smallRead, err := client.Get(ctx, smallKey)
	require.NoError(t, err)
	require.NoError(t, admission.admitHeader(smallRead.Header, false))
	require.False(t, smallRead.More)
	require.Equal(t, int64(len(smallRead.Kvs)), smallRead.Count)
	require.Len(t, smallRead.Kvs, 1)
	require.Equal(t, smallKey, string(smallRead.Kvs[0].Key))
	require.Equal(t, strings.Repeat("s", 64), string(smallRead.Kvs[0].Value))
	require.Positive(t, smallRead.Kvs[0].CreateRevision)
	require.GreaterOrEqual(t, smallRead.Kvs[0].ModRevision, smallRead.Kvs[0].CreateRevision)
	require.LessOrEqual(t, smallRead.Kvs[0].ModRevision, smallRead.Header.Revision)
	require.Positive(t, smallRead.Kvs[0].Version)
	outcome.ReadAllowedWhileAlarmed = true
	_, leaseErr := client.Grant(ctx, 30)
	outcome.LeaseGrantCode = status.Code(leaseErr).String()
	outcome.LeaseGrantTypedNoSpace = errors.Is(leaseErr, rpctypes.ErrNoSpace)
	deleted, deleteErr := client.Delete(ctx, smallKey)
	require.NoError(t, deleteErr)
	require.NoError(t, admission.admitHeader(deleted.Header, true))
	require.Empty(t, deleted.PrevKvs)
	outcome.DeleteAllowedWhileAlarmed = deleted.Deleted == 1
	alarmsAfterDelete, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.NoError(t, admission.admitHeader(alarmsAfterDelete.Header, false))
	for _, alarm := range alarmsAfterDelete.Alarms {
		if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE && alarm.MemberID == memberID {
			outcome.AlarmStickyAfterDelete = true
		}
	}

	compactRevision := deleted.Header.Revision
	compacted, compactErr := client.Compact(ctx, compactRevision, clientv3.WithCompactPhysical())
	if compactErr == nil {
		require.NoError(t, admission.admitHeader(compacted.Header, false))
	}
	defragmented, defragErr := client.Defragment(ctx, endpoint)
	if defragErr == nil {
		// upstream maintenanceServer.Defragment returns an intentionally empty
		// response; KubeBrain preserves that nil-Header no-op contract.
		require.NotNil(t, defragmented)
		require.Nil(t, defragmented.Header)
	}
	outcome.CompactAndDefragSucceeded = compactErr == nil && defragErr == nil
	disarmed, disarmErr := client.AlarmDisarm(ctx, &clientv3.AlarmMember{
		MemberID: noSpaceAlarm.MemberID, Alarm: noSpaceAlarm.Alarm,
	})
	require.NoError(t, disarmErr)
	require.NoError(t, admission.admitHeader(disarmed.Header, false))
	outcome.DisarmReturnedAlarm = len(disarmed.Alarms) == 1 &&
		disarmed.Alarms[0].Alarm == etcdserverpb.AlarmType_NOSPACE &&
		disarmed.Alarms[0].MemberID == memberID
	recovery, recoveryErr := client.Put(ctx, recoveryKey, "recovered")
	if recoveryErr == nil {
		require.Nil(t, recovery.PrevKv)
		require.NoError(t, admission.admitHeader(recovery.Header, true))
	}
	outcome.RecoveryPutSucceeded = recoveryErr == nil
	return outcome
}
