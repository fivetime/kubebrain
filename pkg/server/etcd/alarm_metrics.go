package etcd

import (
	"context"
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

type alarmMetricKey struct {
	serverID  string
	alarmType string
}

func alarmMetricLabels(alarm *etcdserverpb.AlarmMember) alarmMetricKey {
	return alarmMetricKey{
		serverID:  fmt.Sprintf("%x", alarm.GetMemberID()),
		alarmType: alarm.GetAlarm().String(),
	}
}

func (s *RPCServer) recordAlarmActivated(alarm *etcdserverpb.AlarmMember) {
	if alarm == nil || alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return
	}
	labels := alarmMetricLabels(alarm)
	s.alarmMetricMu.Lock()
	defer s.alarmMetricMu.Unlock()
	etcdAlarmGauge.WithLabelValues(labels.serverID, labels.alarmType).Inc()
	if s.knownAlarmMetrics == nil {
		s.knownAlarmMetrics = make(map[alarmMetricKey]struct{})
	}
	s.knownAlarmMetrics[labels] = struct{}{}
}

func (s *RPCServer) recordAlarmDeactivated(alarm *etcdserverpb.AlarmMember) {
	if alarm == nil || alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return
	}
	labels := alarmMetricLabels(alarm)
	s.alarmMetricMu.Lock()
	defer s.alarmMetricMu.Unlock()
	etcdAlarmGauge.WithLabelValues(labels.serverID, labels.alarmType).Dec()
	delete(s.knownAlarmMetrics, labels)
}

// RefreshAlarmMetrics converges this process's debugging gauge with the
// tenant-scoped alarm set persisted in TiKV. etcd reaches the same state by
// applying each Raft alarm mutation on every member; KubeBrain replicas do not
// replay one another's RPCs, so they periodically observe the shared state.
func (s *RPCServer) RefreshAlarmMetrics(ctx context.Context) error {
	alarms := make([]*etcdserverpb.AlarmMember, 0)
	noSpaceMembers, err := s.backend.NoSpaceAlarms(ctx)
	if err != nil {
		return err
	}
	for _, memberID := range noSpaceMembers {
		alarms = append(alarms, &etcdserverpb.AlarmMember{
			MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
		})
	}
	corruptMembers, err := s.backend.CorruptAlarms(ctx)
	if err != nil {
		return err
	}
	for _, memberID := range corruptMembers {
		alarms = append(alarms, &etcdserverpb.AlarmMember{
			MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
		})
	}
	generic, err := s.genericAlarms(ctx, etcdserverpb.AlarmType_NONE)
	if err != nil {
		return err
	}
	alarms = append(alarms, generic...)

	current := make(map[alarmMetricKey]struct{}, len(alarms))
	for _, alarm := range alarms {
		current[alarmMetricLabels(alarm)] = struct{}{}
	}
	s.alarmMetricMu.Lock()
	defer s.alarmMetricMu.Unlock()
	for labels := range current {
		if _, known := s.knownAlarmMetrics[labels]; !known {
			etcdAlarmGauge.WithLabelValues(labels.serverID, labels.alarmType).Set(1)
		}
	}
	for labels := range s.knownAlarmMetrics {
		if _, active := current[labels]; !active {
			etcdAlarmGauge.WithLabelValues(labels.serverID, labels.alarmType).Set(0)
		}
	}
	s.knownAlarmMetrics = current
	return nil
}
