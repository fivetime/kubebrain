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

type downgradeValidateSuccessOutcome struct {
	TargetForm             string
	Code                   string
	Message                string
	VersionMatchesCluster  bool
	HeaderPresent          bool
	HeaderMatchesStatus    bool
	HeaderIDsNonZero       bool
	HeaderRaftTermPositive bool
}

func TestDowngradeValidateSuccessDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := downgradeValidateSuccessOutcomes(t, reference)
	require.Len(t, referenceOutcomes, 2)
	for _, outcome := range referenceOutcomes {
		require.Equal(t, "OK", outcome.Code)
		require.Empty(t, outcome.Message)
		require.True(t, outcome.VersionMatchesCluster)
		require.True(t, outcome.HeaderPresent)
		require.True(t, outcome.HeaderMatchesStatus)
		require.True(t, outcome.HeaderIDsNonZero)
		require.True(t, outcome.HeaderRaftTermPositive)
	}
	require.Equal(t, referenceOutcomes, downgradeValidateSuccessOutcomes(t, compatEndpoint()))
}

func downgradeValidateSuccessOutcomes(t *testing.T, endpoint string) []downgradeValidateSuccessOutcome {
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
	statusHeader := statusResponse.GetHeader()
	require.NotNil(t, statusHeader)
	serverVersion, err := semver.NewVersion(statusResponse.GetVersion())
	require.NoError(t, err)
	require.Positive(t, serverVersion.Minor)
	clusterVersion := fmt.Sprintf("%d.%d", serverVersion.Major, serverVersion.Minor)
	targetMinor := serverVersion.Minor - 1
	targets := []struct {
		form    string
		version string
	}{
		{form: "major-minor", version: fmt.Sprintf("%d.%d", serverVersion.Major, targetMinor)},
		{form: "patch-ignored", version: fmt.Sprintf("%d.%d.999", serverVersion.Major, targetMinor)},
	}

	outcomes := make([]downgradeValidateSuccessOutcome, 0, len(targets))
	for _, target := range targets {
		response, callErr := client.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
			Action:  etcdserverpb.DowngradeRequest_VALIDATE,
			Version: target.version,
		})
		header := response.GetHeader()
		outcomes = append(outcomes, downgradeValidateSuccessOutcome{
			TargetForm: target.form,
			Code:       status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			VersionMatchesCluster: response != nil && response.GetVersion() == clusterVersion,
			HeaderPresent:         header != nil,
			HeaderMatchesStatus: header != nil &&
				header.GetClusterId() == statusHeader.GetClusterId() &&
				header.GetMemberId() == statusHeader.GetMemberId() &&
				header.GetRevision() == statusHeader.GetRevision() &&
				header.GetRaftTerm() == statusHeader.GetRaftTerm(),
			HeaderIDsNonZero:       header != nil && header.GetClusterId() != 0 && header.GetMemberId() != 0,
			HeaderRaftTermPositive: header != nil && header.GetRaftTerm() > 0,
		})
	}
	return outcomes
}
