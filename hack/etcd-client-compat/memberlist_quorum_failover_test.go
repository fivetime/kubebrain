package compat

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMemberListSerializableSurvivesBackendQuorumLoss mirrors upstream
// tests/common/member_test.go::TestMemberListSerializable (845cd3885) at the
// DBaaS coordination boundary. Static membership is locally readable while PD
// quorum is unavailable; requesting a linearizable list must still enter the
// shared read barrier and honor the caller deadline.
func TestMemberListSerializableSurvivesBackendQuorumLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_MEMBERLIST_QUORUM_FAILOVER_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_MEMBERLIST_QUORUM_FAILOVER_COMMAND to run destructive backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for MemberList backend failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	cluster := etcdserverpb.NewClusterClient(cli.ActiveConnection())

	baselineCtx, baselineCancel := context.WithTimeout(ctx, 5*time.Second)
	baseline, err := cluster.MemberList(baselineCtx, &etcdserverpb.MemberListRequest{})
	baselineCancel()
	require.NoError(t, err)
	require.NotEmpty(t, baseline.Members)
	require.NotNil(t, baseline.Header)

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	combinedReadyPath := filepath.Join(t.TempDir(), "combined-fault-ready")
	requireCombinedReady := strings.Contains(command, "KUBEBRAIN_COMBINED_FAULT_READY_FILE")
	if requireCombinedReady {
		t.Setenv("KUBEBRAIN_COMBINED_FAULT_READY_FILE", combinedReadyPath)
	}
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	if requireCombinedReady {
		require.Eventually(t, func() bool {
			_, statErr := os.Stat(combinedReadyPath)
			return statErr == nil
		}, time.Minute, 100*time.Millisecond, "combined backend fault must signal both quorums ready")
	}

	// The helper's 15-second hold is intentionally much longer than this call
	// deadline. Retry through setup and leader-lease transition until a request
	// is observed actually waiting on the unavailable coordination barrier.
	observedDeadline := false
	probeDeadline := time.NewTimer(30 * time.Second)
	defer probeDeadline.Stop()
	for !observedDeadline {
		select {
		case result := <-commandDone:
			require.NoErrorf(t, result.err, "backend failover command ended before linearizable MemberList blocked: %s",
				strings.TrimSpace(string(result.output)))
			t.Fatalf("backend failover command recovered before linearizable MemberList honored its deadline: %s",
				strings.TrimSpace(string(result.output)))
		case <-probeDeadline.C:
			t.Fatal("linearizable MemberList did not block on PD quorum loss")
		default:
		}

		callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		_, callErr := cluster.MemberList(callCtx, &etcdserverpb.MemberListRequest{Linearizable: true})
		callCancel()
		if status.Code(callErr) != codes.DeadlineExceeded {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		observedDeadline = true

		serializableCtx, serializableCancel := context.WithTimeout(ctx, time.Second)
		serializable, serializableErr := cluster.MemberList(serializableCtx, &etcdserverpb.MemberListRequest{})
		serializableCancel()
		require.NoError(t, serializableErr)
		require.Equal(t, baseline.Members, serializable.Members)
		require.NotNil(t, serializable.Header)
		require.Equal(t, baseline.Header.ClusterId, serializable.Header.ClusterId)
	}

	result := <-commandDone
	require.NoErrorf(t, result.err, "backend failover command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("backend failover command: %s", strings.TrimSpace(string(result.output)))

	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, listErr := cluster.MemberList(callCtx, &etcdserverpb.MemberListRequest{Linearizable: true})
		return listErr == nil && response != nil && len(response.Members) == len(baseline.Members)
	}, 45*time.Second, 100*time.Millisecond, "linearizable MemberList must recover after PD quorum returns")
}
