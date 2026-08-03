package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestAlarmUnknownActionFailsClosed verifies KubeBrain's intentional safety
// divergence from upstream etcd: an unknown protobuf enum must not crash the
// server or be misclassified as a DBaaS platform-managed operation.
func TestAlarmUnknownActionFailsClosed(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the live alarm safety test")
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	response, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_AlarmAction(99),
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.Nil(t, response)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: invalid alarm action", status.Convert(err).Message())

	serverStatus, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, serverStatus.Header)
	require.Equal(t, "3.7.0", serverStatus.Version)
}
