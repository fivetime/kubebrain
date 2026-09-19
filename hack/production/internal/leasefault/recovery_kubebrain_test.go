package leasefault

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
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
	t.Run("live", func(t *testing.T) { testRestoreProtocolKubeBrainGRPC(t, false) })
	t.Run("expired-retained", func(t *testing.T) { testRestoreProtocolKubeBrainGRPC(t, true) })
}

func testRestoreProtocolKubeBrainGRPC(t *testing.T, expired bool) {
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
	require.NoError(t, RestoreProtocol(ctx, dir, plan, conn, admit))
	require.NoError(t, VerifyProtocolRecovery(ctx, plan, conn))
	// Already-recovered state must not require another fixture or record overwrite.
	require.NoError(t, RestoreProtocol(ctx, dir, plan, conn, admit))
}
