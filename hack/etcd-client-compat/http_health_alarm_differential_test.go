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

type httpHealthAlarmOutcome struct {
	Name   string
	Status int
	Body   string
}

func TestHTTPHealthAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_QUOTA_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_QUOTA_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_QUOTA_ETCD_ENDPOINT and KUBEBRAIN_QUOTA_ENDPOINT")
	}

	require.Equal(t,
		runHTTPHealthAlarmScenario(t, reference),
		runHTTPHealthAlarmScenario(t, kubebrain),
	)
}

func runHTTPHealthAlarmScenario(t *testing.T, endpoint string) []httpHealthAlarmOutcome {
	t.Helper()
	grpcEndpoint := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const memberID uint64 = 626262
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
			MemberID: memberID,
			Alarm:    etcdserverpb.AlarmType_NOSPACE,
		})
	})

	scheme := "http://"
	if strings.HasPrefix(endpoint, "https://") {
		scheme = "https://"
	}
	baseURL := scheme + grpcEndpoint
	client := &http.Client{Timeout: 5 * time.Second}
	outcomes := make([]httpHealthAlarmOutcome, 0, 5)
	record := func(name, path string) {
		response, getErr := client.Get(baseURL + path)
		require.NoError(t, getErr, name)
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		require.NoError(t, readErr, name)
		require.NoError(t, closeErr, name)
		outcomes = append(outcomes, httpHealthAlarmOutcome{
			Name: name, Status: response.StatusCode, Body: strings.TrimSpace(string(body)),
		})
	}

	record("healthy", "/health")
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	record("active", "/health")
	record("active-serializable", "/health?serializable=true")
	record("active-excluded", "/health?exclude=NOSPACE")
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	record("disarmed", "/health")
	return outcomes
}
