package compat

import (
	"bufio"
	"context"
	"fmt"
	"io"
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

func TestAlarmMetricParserRejectsServingMemberDrift(t *testing.T) {
	alarm := etcdserverpb.AlarmType(127)
	metric := func(serverID string) string {
		return fmt.Sprintf("etcd_server_id{cluster=\"default\",server_id=\"%s\"} 1\n"+
			"etcd_debugging_server_alarms{server_id=\"a342701\",alarm_type=\"%s\"} 1\n", serverID, alarm.String())
	}
	value, err := parseAlarmMetric(strings.NewReader(metric("b")), 0xa342701, alarm, 0xb)
	require.NoError(t, err)
	require.Equal(t, float64(1), value)
	_, err = parseAlarmMetric(strings.NewReader(metric("c")), 0xa342701, alarm, 0xb)
	require.ErrorContains(t, err, "serving member")
	duplicateIdentity := `etcd_server_id{cluster="default",server_id="b"} 1` + "\n" + metric("b")
	_, err = parseAlarmMetric(strings.NewReader(duplicateIdentity), 0xa342701, alarm, 0xb)
	require.ErrorContains(t, err, "exactly one")
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
		runUnknownAlarmMetricScenario(t, compatEndpoint(t), candidateMetrics))
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
	identity := newLiveResponseIdentityAdmission(t)
	clients := make([]etcdserverpb.MaintenanceClient, 0, len(grpcEndpoints))
	for _, endpoint := range grpcEndpoints {
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		clients = append(clients, etcdserverpb.NewMaintenanceClient(conn))
	}
	for index, client := range clients {
		statusResponse, err := client.Status(ctx, &etcdserverpb.StatusRequest{})
		require.NoError(t, err)
		require.NoError(t, identity.admitHeader(index, statusResponse.Header, 1))
	}

	const (
		memberID = uint64(0xa342701)
		alarm    = etcdserverpb.AlarmType(127)
	)
	disarm := func(index int, client etcdserverpb.MaintenanceClient, callCtx context.Context) {
		response, err := client.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
		})
		require.NoError(t, err)
		require.NoError(t, identity.admitHeader(index, response.Header, 1))
	}
	disarm(0, clients[0], ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		disarm(0, clients[0], cleanupCtx)
	})
	baselines := make([]float64, len(metricsEndpoints))
	for i, endpoint := range metricsEndpoints {
		expectedMemberID, expectedErr := identity.expectedMemberID(i)
		require.NoError(t, expectedErr)
		baselines[i] = readAlarmMetricForReplica(t, ctx, endpoint, memberID, alarm, expectedMemberID)
	}

	activated, err := clients[0].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, activated.Header, 1))
	for i, endpoint := range metricsEndpoints {
		expectedMemberID, expectedErr := identity.expectedMemberID(i)
		require.NoError(t, expectedErr)
		require.Eventually(t, func() bool {
			return readAlarmMetricForReplica(t, ctx, endpoint, memberID, alarm, expectedMemberID)-baselines[i] == 1
		}, 5*time.Second, 100*time.Millisecond, "replica %d did not observe activation", i)
	}

	disarmed, err := clients[len(clients)-1].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(len(clients)-1, disarmed.Header, activated.Header.Revision))
	for i, endpoint := range metricsEndpoints {
		expectedMemberID, expectedErr := identity.expectedMemberID(i)
		require.NoError(t, expectedErr)
		require.Eventually(t, func() bool {
			return readAlarmMetricForReplica(t, ctx, endpoint, memberID, alarm, expectedMemberID)-baselines[i] == 0
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
	record := func(name string, wantDelta float64) {
		var delta float64
		require.Eventually(t, func() bool {
			delta = readAlarmMetric(t, ctx, metricsEndpoint, memberID, alarm) - baseline
			return delta == wantDelta
		}, 5*time.Second, 100*time.Millisecond, "%s metric did not converge", name)
		outcomes = append(outcomes, alarmMetricOutcome{
			Name: name, Delta: delta,
		})
	}
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	record("activate", 1)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: alarm,
	})
	require.NoError(t, err)
	record("get", 1)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	record("deactivate", 0)
	return outcomes
}

func readAlarmMetric(
	t *testing.T,
	ctx context.Context,
	metricsEndpoint string,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
) float64 {
	return readAlarmMetricForReplica(t, ctx, metricsEndpoint, memberID, alarm, 0)
}

func readAlarmMetricForReplica(
	t *testing.T,
	ctx context.Context,
	metricsEndpoint string,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
	expectedServingMemberID uint64,
) float64 {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		httpEndpointURL(metricsEndpoint)+"/metrics", nil)
	require.NoError(t, err)
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	value, err := parseAlarmMetric(response.Body, memberID, alarm, expectedServingMemberID)
	require.NoError(t, err)
	return value
}

func parseAlarmMetric(
	reader io.Reader,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
	expectedServingMemberID uint64,
) (float64, error) {
	wantLabels := fmt.Sprintf(`server_id="%x",alarm_type="%s"`, memberID, alarm.String())
	reversedLabels := fmt.Sprintf(`alarm_type="%s",server_id="%x"`, alarm.String(), memberID)
	value := float64(0)
	found := false
	serverIdentityLines := 0
	wantServerIdentity := fmt.Sprintf(`etcd_server_id{cluster="default",server_id="%x"} 1`, expectedServingMemberID)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "etcd_server_id{") {
			serverIdentityLines++
			if expectedServingMemberID != 0 && line != wantServerIdentity {
				return 0, fmt.Errorf("metrics response serving member differs from %x", expectedServingMemberID)
			}
		}
		if !strings.HasPrefix(line, "etcd_debugging_server_alarms{") ||
			(!strings.Contains(line, wantLabels) && !strings.Contains(line, reversedLabels)) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, fmt.Errorf("invalid alarm metric series: %s", line)
		}
		if found {
			return 0, fmt.Errorf("duplicate alarm metric series: %s", line)
		}
		parsed, parseErr := strconv.ParseFloat(fields[1], 64)
		if parseErr != nil {
			return 0, fmt.Errorf("invalid alarm metric value in %s: %w", line, parseErr)
		}
		value = parsed
		found = true
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if expectedServingMemberID != 0 && serverIdentityLines != 1 {
		return 0, fmt.Errorf("metrics response must identify exactly one serving member, got %d", serverIdentityLines)
	}
	return value, nil
}
