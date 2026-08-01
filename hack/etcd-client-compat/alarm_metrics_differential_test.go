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

func TestUnknownAlarmMetricConvergesAcrossKubeBrainReplicas(t *testing.T) {
	grpcEndpoints := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_ALARM_METRIC_ENDPOINTS"))
	metricsEndpoints := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_ALARM_METRICS_ENDPOINTS"))
	if len(grpcEndpoints) == 0 || len(metricsEndpoints) == 0 {
		t.Skip("set KUBEBRAIN_ALARM_METRIC_ENDPOINTS and KUBEBRAIN_ALARM_METRICS_ENDPOINTS")
	}
	require.Equal(t, len(grpcEndpoints), len(metricsEndpoints))
	require.GreaterOrEqual(t, len(grpcEndpoints), 2)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clients := make([]etcdserverpb.MaintenanceClient, 0, len(grpcEndpoints))
	for _, endpoint := range grpcEndpoints {
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		clients = append(clients, etcdserverpb.NewMaintenanceClient(conn))
	}

	const (
		memberID = uint64(0xa342701)
		alarm    = etcdserverpb.AlarmType(127)
	)
	disarm := func(client etcdserverpb.MaintenanceClient, callCtx context.Context) {
		_, _ = client.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
		})
	}
	disarm(clients[0], ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		disarm(clients[0], cleanupCtx)
	})
	baselines := make([]float64, len(metricsEndpoints))
	for i, endpoint := range metricsEndpoints {
		baselines[i] = readAlarmMetric(t, ctx, endpoint, memberID, alarm)
	}

	_, err := clients[0].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	for i, endpoint := range metricsEndpoints {
		require.Eventually(t, func() bool {
			return readAlarmMetric(t, ctx, endpoint, memberID, alarm)-baselines[i] == 1
		}, 5*time.Second, 100*time.Millisecond, "replica %d did not observe activation", i)
	}

	_, err = clients[len(clients)-1].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	for i, endpoint := range metricsEndpoints {
		require.Eventually(t, func() bool {
			return readAlarmMetric(t, ctx, endpoint, memberID, alarm)-baselines[i] == 0
		}, 5*time.Second, 100*time.Millisecond, "replica %d did not observe disarm", i)
	}
}

func splitNonEmptyCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
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
