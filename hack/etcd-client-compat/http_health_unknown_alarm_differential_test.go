package compat

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type unknownAlarmHTTPHealthOutcome struct {
	Name   string
	Status int
	Body   string
}

func TestHTTPHealthUnknownAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := []unknownAlarmHTTPHealthOutcome{
		{Name: "healthy", Status: http.StatusOK, Body: `{"health":"true","reason":""}`},
		{Name: "active", Status: http.StatusServiceUnavailable, Body: `{"health":"false","reason":"ALARM UNKNOWN"}`},
		{Name: "active-serializable", Status: http.StatusServiceUnavailable, Body: `{"health":"false","reason":"ALARM UNKNOWN"}`},
		{Name: "active-excluded-number", Status: http.StatusOK, Body: `{"health":"true","reason":""}`},
		{Name: "active-excluded-name", Status: http.StatusServiceUnavailable, Body: `{"health":"false","reason":"ALARM UNKNOWN"}`},
		{Name: "disarmed", Status: http.StatusOK, Body: `{"health":"true","reason":""}`},
	}
	referenceOutcomes := runUnknownAlarmHTTPHealthScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runUnknownAlarmHTTPHealthScenario(t, compatEndpoint(t)))
}

func runUnknownAlarmHTTPHealthScenario(t *testing.T, endpoint string) []unknownAlarmHTTPHealthOutcome {
	t.Helper()
	grpcEndpoint := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	const (
		memberID = uint64(0xa342003)
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

	scheme := "http://"
	if strings.HasPrefix(endpoint, "https://") {
		scheme = "https://"
	}
	baseURL := scheme + grpcEndpoint
	client := &http.Client{Timeout: 5 * time.Second}
	outcomes := make([]unknownAlarmHTTPHealthOutcome, 0, 6)
	record := func(name, path string) {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		require.NoError(t, requestErr)
		response, callErr := client.Do(request)
		require.NoError(t, callErr)
		body, readErr := io.ReadAll(response.Body)
		require.NoError(t, response.Body.Close())
		require.NoError(t, readErr)
		outcomes = append(outcomes, unknownAlarmHTTPHealthOutcome{
			Name: name, Status: response.StatusCode, Body: strings.TrimSpace(string(body)),
		})
	}

	record("healthy", "/health")
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	record("active", "/health")
	record("active-serializable", "/health?serializable=true")
	record("active-excluded-number", "/health?exclude=127")
	record("active-excluded-name", "/health?exclude=UNKNOWN")
	disarm(ctx)
	record("disarmed", "/health")
	return outcomes
}
