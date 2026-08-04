package compat

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReferenceEtcdAlarmMutationColdHeaderAfterRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the cold-header restart oracle")
	}
	start := newReferenceColdHeaderServer(t, binary, "cold-alarm-header-oracle")
	stop, conn := start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seed, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a3527/reference-cold-alarm"), Value: []byte("durable"),
	})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn := start()
	defer restartedConn.Close()
	const (
		memberID = uint64(0xa352701)
		alarm    = etcdserverpb.AlarmType(125)
	)
	activated, err := etcdserverpb.NewMaintenanceClient(restartedConn).Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.NotNil(t, activated.Header)
	require.GreaterOrEqual(t, activated.Header.Revision, seed.GetHeader().GetRevision())
	require.NotZero(t, activated.Header.ClusterId)
	require.NotZero(t, activated.Header.MemberId)
	require.Positive(t, activated.Header.RaftTerm)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, activated.Alarms)
	_, err = etcdserverpb.NewMaintenanceClient(restartedConn).Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
}

func TestReferenceEtcdAuthStatusColdHeaderAfterRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the cold-header restart oracle")
	}
	start := newReferenceColdHeaderServer(t, binary, "cold-auth-header-oracle")
	stop, conn := start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seed, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a3527/reference-cold-auth"), Value: []byte("durable"),
	})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn := start()
	defer restartedConn.Close()
	statusResponse, err := etcdserverpb.NewAuthClient(restartedConn).AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResponse.Header)
	require.GreaterOrEqual(t, statusResponse.Header.Revision, seed.GetHeader().GetRevision())
	require.NotZero(t, statusResponse.Header.ClusterId)
	require.NotZero(t, statusResponse.Header.MemberId)
	require.Positive(t, statusResponse.Header.RaftTerm)
}

func newReferenceColdHeaderServer(t *testing.T, binary, name string) func() (func(), *grpc.ClientConn) {
	t.Helper()
	dataDir := t.TempDir()
	args := []string{
		"--name", name,
		"--data-dir", dataDir,
		"--listen-client-urls", "http://127.0.0.1:42379",
		"--advertise-client-urls", "http://127.0.0.1:42379",
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", name + "=http://127.0.0.1:42380",
	}
	return func() (func(), *grpc.ClientConn) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		client := &http.Client{Timeout: 250 * time.Millisecond}
		require.Eventually(t, func() bool {
			response, err := client.Get("http://127.0.0.1:42379/version")
			if err != nil {
				return false
			}
			defer response.Body.Close()
			return response.StatusCode == http.StatusOK
		}, 10*time.Second, 50*time.Millisecond)
		conn, err := grpc.NewClient("127.0.0.1:42379", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		return stop, conn
	}
}
