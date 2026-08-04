package compat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type snapshotHeaderOutcome struct {
	RangeHeaderPresent      bool
	HighLevelHeaderPresent  bool
	MultipleRawResponses    bool
	AnyRawHeaderPresent     bool
	AllRawVersionsPresent   bool
	HighLevelVersionPresent bool
	HighLevelDigestValid    bool
}

func TestSnapshotHeaderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := snapshotHeaderOutcome{
		RangeHeaderPresent: true, MultipleRawResponses: true,
		AllRawVersionsPresent: true, HighLevelVersionPresent: true, HighLevelDigestValid: true,
	}
	referenceOutcome := runSnapshotHeaderScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runSnapshotHeaderScenario(t, compatEndpoint(t)))
}

func runSnapshotHeaderScenario(t *testing.T, endpoint string) snapshotHeaderOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rangeResponse, err := cli.Get(ctx, "/dbaas-snapshot-header-absent")
	require.NoError(t, err)
	require.Empty(t, rangeResponse.Kvs)

	versioned, err := cli.SnapshotWithVersion(ctx)
	require.NoError(t, err)
	contents, err := io.ReadAll(versioned.Snapshot)
	require.NoError(t, err)
	require.NoError(t, versioned.Snapshot.Close())
	require.Greater(t, len(contents), sha256.Size)
	digest := sha256.Sum256(contents[:len(contents)-sha256.Size])

	stream, err := etcdserverpb.NewMaintenanceClient(cli.ActiveConnection()).Snapshot(
		ctx, &etcdserverpb.SnapshotRequest{},
	)
	require.NoError(t, err)
	var rawResponses, rawHeaders int
	allRawVersionsPresent := true
	for {
		response, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
		rawResponses++
		if response.Header != nil {
			rawHeaders++
		}
		allRawVersionsPresent = allRawVersionsPresent && response.Version != ""
	}

	return snapshotHeaderOutcome{
		RangeHeaderPresent: rangeResponse.Header != nil, HighLevelHeaderPresent: versioned.Header != nil,
		MultipleRawResponses: rawResponses >= 2, AnyRawHeaderPresent: rawHeaders != 0,
		AllRawVersionsPresent: allRawVersionsPresent, HighLevelVersionPresent: versioned.Version != "",
		HighLevelDigestValid: bytes.Equal(digest[:], contents[len(contents)-sha256.Size:]),
	}
}
