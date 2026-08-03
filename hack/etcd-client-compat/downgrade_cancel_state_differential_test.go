package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type downgradeCancelStateOutcome struct {
	InputVersion          string
	Code                  string
	Message               string
	VersionMatchesCluster bool
}

func TestDowngradeCancelStateDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := downgradeCancelStateOutcomes(t, reference)
	require.Equal(t, []downgradeCancelStateOutcome{
		{InputVersion: "", Code: "OK", VersionMatchesCluster: true},
		{InputVersion: "not-semver", Code: "OK", VersionMatchesCluster: true},
		{InputVersion: "999.999.999", Code: "OK", VersionMatchesCluster: true},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, downgradeCancelStateOutcomes(t, compatEndpoint(t)))
}

func downgradeCancelStateOutcomes(t *testing.T, endpoint string) []downgradeCancelStateOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	statusResponse, err := client.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	serverVersion, err := semver.NewVersion(statusResponse.GetVersion())
	require.NoError(t, err)
	clusterVersion := fmt.Sprintf("%d.%d", serverVersion.Major, serverVersion.Minor)

	versions := []string{"", "not-semver", "999.999.999"}
	outcomes := make([]downgradeCancelStateOutcome, 0, len(versions))
	for _, version := range versions {
		response, callErr := client.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
			Action:  etcdserverpb.DowngradeRequest_CANCEL,
			Version: version,
		})
		outcomes = append(outcomes, downgradeCancelStateOutcome{
			InputVersion: version,
			Code:         status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			VersionMatchesCluster: response != nil && response.GetVersion() == clusterVersion,
		})
	}
	return outcomes
}
