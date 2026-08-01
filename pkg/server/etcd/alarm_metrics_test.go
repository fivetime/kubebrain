package etcd

import (
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

	require.Equal(t, float64(0), alarmMetricValue(t, serverID, alarmType))
	recordAlarmActivated(alarm)
	require.Equal(t, float64(1), alarmMetricValue(t, serverID, alarmType))
	recordAlarmActivated(alarm)
	require.Equal(t, float64(2), alarmMetricValue(t, serverID, alarmType),
		"upstream increments the debugging gauge for every idempotent activation response")
	recordAlarmDeactivated(alarm)
	require.Equal(t, float64(1), alarmMetricValue(t, serverID, alarmType))
}

func alarmMetricValue(t *testing.T, serverID, alarmType string) float64 {
	t.Helper()
	metric := &io_prometheus_client.Metric{}
	require.NoError(t, etcdAlarmGauge.WithLabelValues(serverID, alarmType).Write(metric))
	require.NotNil(t, metric.Gauge)
	return metric.Gauge.GetValue()
}
