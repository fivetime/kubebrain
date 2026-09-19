package leasefault

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	serveretcd "github.com/kubewharf/kubebrain/pkg/server/etcd"
	"github.com/kubewharf/kubebrain/pkg/server/service"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Fixed single-leader fixture, not a real election or fault recovery proof.
type recoveryPeer struct{ service.PeerService }

func (recoveryPeer) IsLeader() bool                                 { return true }
func (recoveryPeer) HasLeader() bool                                { return true }
func (recoveryPeer) EpochAndLeadingFresh() (uint64, bool)           { return 0, true }
func (recoveryPeer) LeadershipTerm(context.Context) (uint64, error) { return 1, nil }
func (recoveryPeer) CurrentLeadershipTerm() uint64                  { return 1 }
func (recoveryPeer) GetLeaderInfo() string                          { return "recovery-test" }
func (recoveryPeer) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: "recovery-test", IsLeader: true}, nil
}
func (recoveryPeer) EtcdProxyEnabled() bool                 { return false }
func (recoveryPeer) SyncReadRevision(context.Context) error { return nil }
func (recoveryPeer) Ready() error                           { return nil }

func TestRestoreProtocolKubeBrainGRPC(t *testing.T) {
	for _, mode := range []string{"live", "expired-retained", "foreign-key", "child-timeout"} {
		t.Run(mode, func(t *testing.T) { testRestoreProtocolKubeBrainGRPC(t, mode) })
	}
}

func testRestoreProtocolKubeBrainGRPC(t *testing.T, mode string) {
	expired, foreignKey := mode == "expired-retained", mode == "foreign-key"
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	b := backend.NewBackend(memkv.NewKvStorage(), backend.Config{Identity: "recovery-test", EnableEtcdCompatibility: true}, metrics)
	server := serveretcd.New(b, metrics, recoveryPeer{})
	defer func() { require.NoError(t, server.Close()); require.NoError(t, b.(interface{ Close() error }).Close()) }()
	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	pb.RegisterMaintenanceServer(grpcServer, server)
	pb.RegisterLeaseServer(grpcServer, server)
	pb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	done := make(chan struct{})
	go func() { defer close(done); _ = grpcServer.Serve(listener) }()
	defer func() { grpcServer.Stop(); _ = listener.Close(); <-done }()
	conn, err := grpc.NewClient("passthrough:///kubebrain-recovery", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	maintenance, lease, kv := pb.NewMaintenanceClient(conn), pb.NewLeaseClient(conn), pb.NewKVClient(conn)
	status, err := maintenance.Status(ctx, &pb.StatusRequest{})
	require.NoError(t, err)
	plan := ProtocolRecovery{Owner: "in-memory-recovery", NamespaceUID: "fixture-namespace", StatefulSetUID: "fixture-sts", ClusterID: status.Header.ClusterId, AlarmMemberID: status.Header.MemberId, LeaseID: 5177, Key: "/acceptance/recovery-grpc"}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	require.NoError(t, ArmProtocolRecovery(dir, plan))
	grantTTL := int64(60)
	if expired {
		grantTTL = 1
	}
	_, err = lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{ID: plan.LeaseID, TTL: grantTTL})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte(plan.Key), Value: []byte("fixture"), Lease: plan.LeaseID})
	require.NoError(t, err)
	if foreignKey {
		_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("/business/not-owned"), Value: []byte("preserve"), Lease: plan.LeaseID})
		require.NoError(t, err)
	}
	_, err = maintenance.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, MemberID: plan.AlarmMemberID, Alarm: pb.AlarmType_CORRUPT})
	require.NoError(t, err)
	if expired {
		require.Eventually(t, func() bool {
			response, err := lease.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: plan.LeaseID, Keys: true})
			return err == nil && response.TTL < 0 && response.GrantedTTL > 0 && len(response.Keys) == 1
		}, 5*time.Second, 20*time.Millisecond)
	}
	require.Error(t, VerifyProtocolRecovery(ctx, plan, conn))
	// Kubernetes admission is intentionally synthetic: this fixture has no cluster.
	admit := func(context.Context) error { return nil }
	if mode == "child-timeout" {
		log, err := os.OpenFile(filepath.Join(dir, "child.stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		defer log.Close()
		pidPath := filepath.Join(dir, "child.pid")
		spec := metricsworker.Command{Executable: "/bin/bash", Stderr: log, Args: []string{"-c", `
set -eu
printf '%s\n' "$BASHPID" > "$1"
printf 'FAULT_READY\n'
IFS= read -r origin
while :; do /bin/sleep 1; done
`, "recovery-timeout-fixture", pidPath}}
		var expiredCtx context.Context
		err = metricsworker.WithPreparedFault(ctx, spec, func(runCtx context.Context, inject func(context.Context, time.Time) error) error {
			origin := time.Now()
			faultCtx, stop := context.WithDeadline(runCtx, origin.Add(100*time.Millisecond))
			defer stop()
			expiredCtx = faultCtx
			return inject(faultCtx, origin)
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotNil(t, expiredCtx)
		pidData, err := os.ReadFile(pidPath)
		require.NoError(t, err)
		pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
		require.NoError(t, err)
		require.Positive(t, pid)
		// Only the direct child's join is exercised; this is not cluster or
		// escaped-descendant admission. Protocol setup above ran in the parent.
		admit = func(context.Context) error {
			if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
				return fmt.Errorf("fixture child not reaped: %v", err)
			}
			return nil
		}
		require.NoError(t, admit(ctx))
		require.ErrorIs(t, RestoreProtocol(expiredCtx, dir, plan, conn, admit), context.DeadlineExceeded)
		alarms, err := maintenance.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET})
		require.NoError(t, err)
		require.Len(t, alarms.Alarms, 1)
		ttl, err := lease.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: plan.LeaseID, Keys: true})
		require.NoError(t, err)
		require.Equal(t, grantTTL, ttl.GrantedTTL)
		require.Equal(t, [][]byte{[]byte(plan.Key)}, ttl.Keys)
		// Fresh recovery budget is separate from the failed fault deadline;
		// successful cleanup must never turn that failed attempt into a pass.
		recoveryCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		ctx = recoveryCtx
	}
	if foreignKey {
		require.ErrorContains(t, RestoreProtocol(ctx, dir, plan, conn, admit), "unrelated key")
		alarms, err := maintenance.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET})
		require.NoError(t, err)
		require.Len(t, alarms.Alarms, 1)
		require.Equal(t, plan.AlarmMemberID, alarms.Alarms[0].MemberID)
		require.Equal(t, pb.AlarmType_CORRUPT, alarms.Alarms[0].Alarm)
		for _, key := range []string{plan.Key, "/business/not-owned"} {
			response, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte(key)})
			require.NoError(t, err)
			require.Len(t, response.Kvs, 1)
			require.Equal(t, plan.LeaseID, response.Kvs[0].Lease)
			if key == "/business/not-owned" {
				require.Equal(t, []byte("preserve"), response.Kvs[0].Value)
			}
		}
		return
	}
	require.NoError(t, RestoreProtocol(ctx, dir, plan, conn, admit))
	require.NoError(t, VerifyProtocolRecovery(ctx, plan, conn))
	// Already-recovered state must not require another fixture or record overwrite.
	require.NoError(t, RestoreProtocol(ctx, dir, plan, conn, admit))
}
