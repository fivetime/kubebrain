package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type clientAPIVersionOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestClientAPIVersionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := runClientAPIVersionScenario(t, reference)
	require.Equal(t, []clientAPIVersionOutcome{
		{Name: "missing", Code: "OK"},
		{Name: "valid", Code: "OK"},
		{
			Name:    "invalid-utf8",
			Code:    "Internal",
			Message: `header key "client-api-version" contains value with non-printable ASCII characters`,
		},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runClientAPIVersionScenario(t, compatEndpoint(t)))
}

func runClientAPIVersionScenario(t *testing.T, endpoint string) []clientAPIVersionOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	tests := []struct {
		name    string
		version *string
	}{
		{name: "missing"},
		{name: "valid", version: stringPointer("3.7.0")},
		{name: "invalid-utf8", version: stringPointer(string([]byte{0xff}))},
	}
	outcomes := make([]clientAPIVersionOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if test.version != nil {
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
				rpctypes.MetadataClientAPIVersionKey, *test.version,
			))
		}
		_, callErr := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/dbaas-client-api-version")})
		cancel()
		outcomes = append(outcomes, clientAPIVersionOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}

func stringPointer(value string) *string {
	return &value
}
