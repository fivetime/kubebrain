package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestPlatformManagedErrorDetails(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the live platform boundary test")
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	stream, err := maintenance.Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	snapshot := platformErrorInfo(t, err, "maintenance.snapshot")
	require.Equal(t, "Backup", snapshot.Metadata["operation_type"])
	require.Equal(t, "kubebrain.logical.v2", snapshot.Metadata["artifact_format"])
	require.Equal(t, "false", snapshot.Metadata["etcd_snapshot_restore_usable"])

}

func platformErrorInfo(t *testing.T, err error, capability string) *errdetails.ErrorInfo {
	t.Helper()
	st := status.Convert(err)
	require.Equal(t, codes.Unimplemented, st.Code())
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			require.Equal(t, "KUBEBRAIN_PLATFORM_MANAGED", info.Reason)
			require.Equal(t, "dbaas.kubebrain.io", info.Domain)
			require.Equal(t, capability, info.Metadata["capability"])
			return info
		}
	}
	require.FailNow(t, "platform-managed ErrorInfo is missing")
	return nil
}
