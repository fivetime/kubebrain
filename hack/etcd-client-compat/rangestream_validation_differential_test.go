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

type rangeStreamValidationOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestRangeStreamValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run RangeStream validation differential tests")
	}
	referenceOutcomes := runRangeStreamValidationScenario(t, reference)
	require.Equal(t, []rangeStreamValidationOutcome{
		{Name: "empty-key", Code: "InvalidArgument", Message: "etcdserver: key is not provided"},
		{Name: "invalid-sort-order", Code: "InvalidArgument", Message: "etcdserver: invalid sort option"},
		{Name: "invalid-sort-target", Code: "InvalidArgument", Message: "etcdserver: invalid sort option"},
		{Name: "custom-sort", Code: "Unimplemented", Message: "RangeStream does not support custom sort orders"},
		{Name: "revision-filter", Code: "Unimplemented", Message: "RangeStream does not support revision filters"},
		{Name: "custom-sort-and-filter", Code: "Unimplemented", Message: "RangeStream does not support custom sort orders"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runRangeStreamValidationScenario(t, compatEndpoint()))
}

func runRangeStreamValidationScenario(t *testing.T, endpoint string) []rangeStreamValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	tests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "empty-key", req: &etcdserverpb.RangeRequest{}},
		{name: "invalid-sort-order", req: &etcdserverpb.RangeRequest{Key: []byte("/a"), SortOrder: 99}},
		{name: "invalid-sort-target", req: &etcdserverpb.RangeRequest{Key: []byte("/a"), SortTarget: 99}},
		{name: "custom-sort", req: &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY}},
		{name: "revision-filter", req: &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), MinModRevision: 1}},
		{name: "custom-sort-and-filter", req: &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY, MinModRevision: 1}},
	}

	outcomes := make([]rangeStreamValidationOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, callErr := client.RangeStream(ctx, test.req)
		if callErr == nil {
			_, callErr = stream.Recv()
		}
		cancel()
		outcomes = append(outcomes, rangeStreamValidationOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
