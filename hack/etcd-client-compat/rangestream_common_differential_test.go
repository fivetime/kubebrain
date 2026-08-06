package compat

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type rangeStreamCommonOutcome struct {
	Name            string
	Keys            []string
	Values          []string
	Leased          []bool
	Count           int64
	More            bool
	HeaderIsCurrent bool
	Code            string
	EndedWithEOF    bool
}

func TestRangeStreamCommonShapesDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run RangeStream common-shape differential tests")
	}
	want := runRangeStreamCommonShapes(t, reference, "reference")
	got := runRangeStreamCommonShapes(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, want, got)
}

func runRangeStreamCommonShapes(
	t *testing.T,
	endpoint, instance string,
) []rangeStreamCommonOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	// From-key ranges intentionally extend to the end of the keyspace. Keep this
	// fixture above the suite's ASCII keys while retaining a finite prefix end,
	// so concurrently running differential tests cannot leak into its result.
	prefix := "\xff\xfe" + testPrefix(t) + "/" + instance + "/"
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	first, err := client.Put(ctx, prefix+"a", "v1")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "v2")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"a", "v3")
	require.NoError(t, err)
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"c", "leased", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Revoke(cleanupCtx, lease.ID)
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	requests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "point-hit", req: &etcdserverpb.RangeRequest{Key: []byte(prefix + "a")}},
		{name: "point-miss", req: &etcdserverpb.RangeRequest{Key: []byte(prefix + "missing")}},
		{name: "equal-empty", req: &etcdserverpb.RangeRequest{
			Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "a"),
		}},
		{name: "reversed-empty", req: &etcdserverpb.RangeRequest{
			Key: []byte(prefix + "z"), RangeEnd: []byte(prefix + "a"),
		}},
		{name: "prefix-limit-one", req: &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), Limit: 1,
		}},
		{name: "historical-after-first-put", req: &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			Revision: first.Header.Revision,
		}},
		{name: "from-key", req: &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte{0},
		}},
		{name: "keys-only-prefix", req: &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), KeysOnly: true,
		}},
	}

	outcomes := make([]rangeStreamCommonOutcome, 0, len(requests))
	for _, test := range requests {
		kvClient := etcdserverpb.NewKVClient(client.ActiveConnection())
		unary, unaryErr := kvClient.Range(ctx, test.req)
		require.NoError(t, unaryErr)
		stream, callErr := kvClient.RangeStream(ctx, test.req)
		outcome := rangeStreamCommonOutcome{Name: test.name}
		if callErr != nil {
			outcome.Code = status.Code(callErr).String()
			outcomes = append(outcomes, outcome)
			continue
		}
		for {
			chunk, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				outcome.EndedWithEOF = true
				break
			}
			if recvErr != nil {
				outcome.Code = status.Code(recvErr).String()
				break
			}
			response := chunk.GetRangeResponse()
			for _, kv := range response.GetKvs() {
				outcome.Keys = append(outcome.Keys, string(kv.Key[len(prefix):]))
				outcome.Values = append(outcome.Values, string(kv.Value))
				outcome.Leased = append(outcome.Leased, kv.Lease != 0)
			}
			if response.Header != nil {
				outcome.HeaderIsCurrent = response.Header.Revision >= unary.Header.Revision
				outcome.Count = response.Count
				outcome.More = response.More
			}
		}
		outcomes = append(outcomes, outcome)
	}
	for _, outcome := range outcomes {
		require.True(t, outcome.EndedWithEOF, "%s did not terminate with io.EOF", outcome.Name)
		require.Empty(t, outcome.Code, "%s returned a terminal gRPC error", outcome.Name)
	}
	return outcomes
}
