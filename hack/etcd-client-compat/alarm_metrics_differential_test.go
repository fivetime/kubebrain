package compat

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type alarmMetricOutcome struct {
	Name  string
	Delta float64
}

func TestUnknownAlarmMetricDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	candidateMetrics := os.Getenv("KUBEBRAIN_METRICS_ENDPOINT")
	if reference == "" || candidateMetrics == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_METRICS_ENDPOINT")
	}
	referenceMetrics := os.Getenv("REFERENCE_ETCD_METRICS_ENDPOINT")
	if referenceMetrics == "" {
		referenceMetrics = reference
	}

	want := []alarmMetricOutcome{
		{Name: "activate", Delta: 1},
		{Name: "get", Delta: 1},
		{Name: "deactivate", Delta: 0},
	}
	referenceOutcomes := runUnknownAlarmMetricScenario(t, reference, referenceMetrics)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes,
		runUnknownAlarmMetricScenario(t, compatEndpoint(), candidateMetrics))
}

func runUnknownAlarmMetricScenario(t *testing.T, endpoint, metricsEndpoint string) []alarmMetricOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const (
		memberID = uint64(0xa342601)
		alarm    = etcdserverpb.AlarmType(127)
	)
	disarm := func(callCtx context.Context) {
		_, _ = maintenance.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
		})
	}
	disarm(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		disarm(cleanupCtx)
	})

	baseline := readAlarmMetric(t, ctx, metricsEndpoint, memberID, alarm)
	outcomes := make([]alarmMetricOutcome, 0, 3)
	record := func(name string) {
		outcomes = append(outcomes, alarmMetricOutcome{
			Name: name, Delta: readAlarmMetric(t, ctx, metricsEndpoint, memberID, alarm) - baseline,
		})
	}
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	record("activate")
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: alarm,
	})
	require.NoError(t, err)
	record("get")
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	record("deactivate")
	return outcomes
}

func readAlarmMetric(
	t *testing.T,
	ctx context.Context,
	metricsEndpoint string,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
) float64 {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(metricsEndpoint, "/")+"/metrics", nil)
	require.NoError(t, err)
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)

	wantLabels := fmt.Sprintf(`server_id="%x",alarm_type="%s"`, memberID, alarm.String())
	reversedLabels := fmt.Sprintf(`alarm_type="%s",server_id="%x"`, alarm.String(), memberID)
	value := float64(0)
	found := false
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "etcd_debugging_server_alarms{") ||
			(!strings.Contains(line, wantLabels) && !strings.Contains(line, reversedLabels)) {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 2, line)
		require.False(t, found, "duplicate alarm metric series: %s", line)
		parsed, parseErr := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, parseErr, line)
		value = parsed
		found = true
	}
	require.NoError(t, scanner.Err())
	return value
}
