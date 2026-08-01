package etcd

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

var etcdAlarmGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "etcd_debugging",
		Subsystem: "server",
		Name:      "alarms",
		Help:      "Alarms for every member in cluster. 1 for 'server_id' label with current ID. 2 for 'alarm_type' label with type of this alarm",
	},
	[]string{"server_id", "alarm_type"},
)

func init() {
	prometheus.MustRegister(etcdAlarmGauge)
}

func recordAlarmActivated(alarm *etcdserverpb.AlarmMember) {
	if alarm == nil || alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return
	}
	etcdAlarmGauge.WithLabelValues(
		fmt.Sprintf("%x", alarm.GetMemberID()), alarm.GetAlarm().String(),
	).Inc()
}

func recordAlarmDeactivated(alarm *etcdserverpb.AlarmMember) {
	if alarm == nil || alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return
	}
	etcdAlarmGauge.WithLabelValues(
		fmt.Sprintf("%x", alarm.GetMemberID()), alarm.GetAlarm().String(),
	).Dec()
}
