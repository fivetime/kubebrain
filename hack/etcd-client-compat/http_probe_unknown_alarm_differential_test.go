package compat

import (
	"context"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type unknownAlarmProbeOutcome struct {
	Path   string
	Status int
	Body   string
}

// TestHTTPProbesIgnoreUnknownAlarmDifferentialAgainstReferenceEtcd pins the
// boundary between legacy /health (which rejects every active alarm) and the
// named livez/readyz checks (which only treat CORRUPT as data corruption).
func TestHTTPProbesIgnoreUnknownAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []unknownAlarmProbeOutcome{
		{Path: "/livez", Status: http.StatusOK, Body: "ok"},
		{Path: "/livez?verbose", Status: http.StatusOK, Body: "[+]serializable_read ok\nok"},
		{Path: "/livez/serializable_read?verbose", Status: http.StatusOK, Body: "[+]serializable_read ok\nok"},
		{Path: "/readyz", Status: http.StatusOK, Body: "ok"},
		{Path: "/readyz?verbose", Status: http.StatusOK, Body: "[+]data_corruption ok\n[+]linearizable_read ok\n[+]non_learner ok\n[+]serializable_read ok\nok"},
		{Path: "/readyz/data_corruption?verbose", Status: http.StatusOK, Body: "[+]data_corruption ok\nok"},
	}
	referenceOutcomes := runUnknownAlarmProbeScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runUnknownAlarmProbeScenario(t, compatEndpoint()))
}

func runUnknownAlarmProbeScenario(t *testing.T, endpoint string) []unknownAlarmProbeOutcome {
	t.Helper()
	grpcEndpoint := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const (
		memberID = uint64(0xa342401)
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
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)

	scheme := "http://"
	if strings.HasPrefix(endpoint, "https://") {
		scheme = "https://"
	}
	client := &http.Client{Timeout: 5 * time.Second}
	paths := []string{
		"/livez",
		"/livez?verbose",
		"/livez/serializable_read?verbose",
		"/readyz",
		"/readyz?verbose",
		"/readyz/data_corruption?verbose",
	}
	outcomes := make([]unknownAlarmProbeOutcome, 0, len(paths))
	for _, probePath := range paths {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, scheme+grpcEndpoint+probePath, nil)
		require.NoError(t, requestErr)
		response, callErr := client.Do(request)
		require.NoError(t, callErr, probePath)
		body, readErr := io.ReadAll(response.Body)
		require.NoError(t, response.Body.Close())
		require.NoError(t, readErr, probePath)
		outcomes = append(outcomes, unknownAlarmProbeOutcome{
			Path: probePath, Status: response.StatusCode, Body: canonicalProbeBody(string(body)),
		})
	}
	return outcomes
}

func canonicalProbeBody(body string) string {
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) > 1 && lines[len(lines)-1] == "ok" {
		sort.Strings(lines[:len(lines)-1])
	}
	return strings.Join(lines, "\n")
}
