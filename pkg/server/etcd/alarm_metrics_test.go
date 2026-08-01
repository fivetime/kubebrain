package etcd

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestAlarmMetricMatchesEtcdMutationAccounting(t *testing.T) {
	alarm := &etcdserverpb.AlarmMember{MemberID: 0xa342601, Alarm: etcdserverpb.AlarmType(127)}
	serverID := fmt.Sprintf("%x", alarm.MemberID)
	alarmType := alarm.Alarm.String()
	etcdAlarmGauge.DeleteLabelValues(serverID, alarmType)
	t.Cleanup(func() { etcdAlarmGauge.DeleteLabelValues(serverID, alarmType) })
	server := &RPCServer{knownAlarmMetrics: make(map[alarmMetricKey]struct{})}

	require.Equal(t, float64(0), alarmMetricValue(t, serverID, alarmType))
	server.recordAlarmActivated(alarm)
	require.Equal(t, float64(1), alarmMetricValue(t, serverID, alarmType))
	server.recordAlarmActivated(alarm)
	require.Equal(t, float64(2), alarmMetricValue(t, serverID, alarmType),
		"upstream increments the debugging gauge for every idempotent activation response")
	server.recordAlarmDeactivated(alarm)
	require.Equal(t, float64(1), alarmMetricValue(t, serverID, alarmType))
}

func TestAlarmMetricRefreshConvergesSharedGenericState(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	alarm := &etcdserverpb.AlarmMember{MemberID: 0xa342701, Alarm: etcdserverpb.AlarmType(127)}
	labels := alarmMetricLabels(alarm)
	etcdAlarmGauge.DeleteLabelValues(labels.serverID, labels.alarmType)
	t.Cleanup(func() { etcdAlarmGauge.DeleteLabelValues(labels.serverID, labels.alarmType) })

	_, err := server.mutateGenericAlarm(ctx, alarm.Alarm, alarm.MemberID, true)
	require.NoError(t, err)
	require.Equal(t, float64(0), alarmMetricValue(t, labels.serverID, labels.alarmType),
		"a remote replica mutation does not update this process before refresh")
	require.NoError(t, server.RefreshAlarmMetrics(ctx))
	require.Equal(t, float64(1), alarmMetricValue(t, labels.serverID, labels.alarmType))

	removed, err := server.mutateGenericAlarm(ctx, alarm.Alarm, alarm.MemberID, false)
	require.NoError(t, err)
	require.True(t, removed)
	require.Equal(t, float64(1), alarmMetricValue(t, labels.serverID, labels.alarmType),
		"a remote replica disarm remains visible until refresh")
	require.NoError(t, server.RefreshAlarmMetrics(ctx))
	require.Equal(t, float64(0), alarmMetricValue(t, labels.serverID, labels.alarmType))
}

func alarmMetricValue(t *testing.T, serverID, alarmType string) float64 {
	t.Helper()
	metric := &io_prometheus_client.Metric{}
	require.NoError(t, etcdAlarmGauge.WithLabelValues(serverID, alarmType).Write(metric))
	require.NotNil(t, metric.Gauge)
	return metric.Gauge.GetValue()
}
