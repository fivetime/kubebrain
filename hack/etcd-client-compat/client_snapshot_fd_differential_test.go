package compat

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	clientsnapshot "go.etcd.io/etcd/client/v3/snapshot"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TestClientSnapshotFDDifferentialAgainstReferenceEtcd pins upstream etcd
// c504fed58. SaveWithVersion must close its temporary file on every early
// return, including a complete stream whose length cannot contain the required
// trailing SHA-256 digest. A valid snapshot is then saved from each dataplane.
func TestClientSnapshotFDDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	runClientSnapshotFDOracle(t)
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), name+".db")
			version, err := clientsnapshot.SaveWithVersion(ctx, zap.NewNop(), clientv3.Config{
				Endpoints:   []string{endpoint},
				DialTimeout: 5 * time.Second,
			}, path)
			require.NoError(t, err)
			require.NotEmpty(t, version)
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Greater(t, info.Size(), int64(32))
			_, err = os.Stat(path + ".part")
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

type incompleteSnapshotMaintenanceServer struct {
	etcdserverpb.UnimplementedMaintenanceServer
}

func (incompleteSnapshotMaintenanceServer) Snapshot(_ *etcdserverpb.SnapshotRequest, stream grpc.ServerStreamingServer[etcdserverpb.SnapshotResponse]) error {
	return stream.Send(&etcdserverpb.SnapshotResponse{
		Blob:           []byte("snapshot-without-checksum"),
		RemainingBytes: 0,
		Version:        "fd-leak-oracle",
	})
}

func runClientSnapshotFDOracle(t *testing.T) {
	t.Helper()
	if _, err := os.ReadDir("/proc/self/fd"); err != nil {
		t.Skipf("/proc/self/fd is required for the descriptor leak oracle: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	etcdserverpb.RegisterMaintenanceServer(server, incompleteSnapshotMaintenanceServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		server.Stop()
	})

	tempDir := t.TempDir()
	for iteration := 0; iteration < 25; iteration++ {
		path := filepath.Join(tempDir, "incomplete.db")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		version, saveErr := clientsnapshot.SaveWithVersion(ctx, zap.NewNop(), clientv3.Config{
			Endpoints:   []string{listener.Addr().String()},
			DialTimeout: 5 * time.Second,
		}, path)
		cancel()
		require.Equal(t, "fd-leak-oracle", version)
		require.EqualError(t, saveErr, "sha256 checksum not found [bytes: 25]")
		require.Empty(t, openDescriptorsBelow(t, tempDir), "iteration %d leaked a deleted .part descriptor", iteration)
		_, statErr := os.Stat(path + ".part")
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
}

func openDescriptorsBelow(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var matches []string
	for _, entry := range entries {
		target, readErr := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if readErr == nil && strings.Contains(target, directory) {
			matches = append(matches, target)
		}
	}
	return matches
}
