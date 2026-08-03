package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type downgradeRequestValidationOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestDowngradeRequestValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := downgradeRequestValidationOutcomes(t, reference)
	require.Equal(t, []downgradeRequestValidationOutcome{
		{Name: "validate-malformed-version", Code: "InvalidArgument", Message: "etcdserver: wrong downgrade target version format"},
		{Name: "enable-malformed-version", Code: "InvalidArgument", Message: "etcdserver: wrong downgrade target version format"},
		{Name: "unknown-action", Code: "Unknown", Message: "etcdserver: unknown method"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, downgradeRequestValidationOutcomes(t, compatEndpoint()))
}

func downgradeRequestValidationOutcomes(t *testing.T, endpoint string) []downgradeRequestValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		request *etcdserverpb.DowngradeRequest
	}{
		{name: "validate-malformed-version", request: &etcdserverpb.DowngradeRequest{Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: "not-semver"}},
		{name: "enable-malformed-version", request: &etcdserverpb.DowngradeRequest{Action: etcdserverpb.DowngradeRequest_ENABLE, Version: "not-semver"}},
		{name: "unknown-action", request: &etcdserverpb.DowngradeRequest{Action: etcdserverpb.DowngradeRequest_DowngradeAction(127), Version: "3.7"}},
	}
	outcomes := make([]downgradeRequestValidationOutcome, 0, len(tests))
	for _, test := range tests {
		_, callErr := client.Downgrade(ctx, test.request)
		outcomes = append(outcomes, downgradeRequestValidationOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
