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

type downgradeTargetValidationOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestDowngradeTargetValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := downgradeTargetValidationOutcomes(t, reference)
	require.Equal(t, []downgradeTargetValidationOutcome{
		{Name: "validate-current", Code: "InvalidArgument", Message: "etcdserver: invalid downgrade target version"},
		{Name: "validate-too-old", Code: "InvalidArgument", Message: "etcdserver: invalid downgrade target version"},
		{Name: "enable-future", Code: "InvalidArgument", Message: "etcdserver: invalid downgrade target version"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, downgradeTargetValidationOutcomes(t, compatEndpoint()))
}

func downgradeTargetValidationOutcomes(t *testing.T, endpoint string) []downgradeTargetValidationOutcome {
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
	require.GreaterOrEqual(t, serverVersion.Minor, int64(2))
	current := fmt.Sprintf("%d.%d", serverVersion.Major, serverVersion.Minor)
	tooOld := fmt.Sprintf("%d.%d", serverVersion.Major, serverVersion.Minor-2)
	future := fmt.Sprintf("%d.%d", serverVersion.Major, serverVersion.Minor+1)

	tests := []struct {
		name    string
		request *etcdserverpb.DowngradeRequest
	}{
		{name: "validate-current", request: &etcdserverpb.DowngradeRequest{Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: current}},
		{name: "validate-too-old", request: &etcdserverpb.DowngradeRequest{Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: tooOld}},
		{name: "enable-future", request: &etcdserverpb.DowngradeRequest{Action: etcdserverpb.DowngradeRequest_ENABLE, Version: future}},
	}
	outcomes := make([]downgradeTargetValidationOutcome, 0, len(tests))
	for _, test := range tests {
		_, callErr := client.Downgrade(ctx, test.request)
		outcomes = append(outcomes, downgradeTargetValidationOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
