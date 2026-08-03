package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type snapshotStreamOutcome struct {
	Version       string
	BackendBytes  int
	DataResponses int
	FinalBytes    int
	DigestMatches bool
}

func TestSnapshotStreamProtocolMatchesReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	referenceOutcome := captureSnapshotStream(t, reference)
	kubebrainOutcome := captureSnapshotStream(t, kubebrain)
	for name, outcome := range map[string]snapshotStreamOutcome{"reference": referenceOutcome, "kubebrain": kubebrainOutcome} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, "3.7.0", outcome.Version)
			require.Positive(t, outcome.BackendBytes)
			require.Positive(t, outcome.DataResponses)
			require.Equal(t, sha256.Size, outcome.FinalBytes)
			require.True(t, outcome.DigestMatches)
		})
	}
}

func captureSnapshotStream(t *testing.T, endpoint string) snapshotStreamOutcome {
	t.Helper()
	target := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewMaintenanceClient(conn).Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
	require.NoError(t, err)
	var responses []*etcdserverpb.SnapshotResponse
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		responses = append(responses, response)
	}
	require.GreaterOrEqual(t, len(responses), 2)
	hash := sha256.New()
	backendBytes := 0
	version := ""
	for i, response := range responses[:len(responses)-1] {
		require.LessOrEqual(t, len(response.Blob), 32*1024)
		if version == "" {
			version = response.Version
		}
		require.Equal(t, version, response.Version)
		backendBytes += len(response.Blob)
		_, _ = hash.Write(response.Blob)
		if i+1 == len(responses)-1 {
			require.Zero(t, response.RemainingBytes)
		} else {
			require.Positive(t, response.RemainingBytes)
		}
	}
	final := responses[len(responses)-1]
	require.Zero(t, final.RemainingBytes)
	require.Equal(t, version, final.Version)
	return snapshotStreamOutcome{
		Version: version, BackendBytes: backendBytes, DataResponses: len(responses) - 1,
		FinalBytes: len(final.Blob), DigestMatches: string(hash.Sum(nil)) == string(final.Blob),
	}
}
