package compat

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rangeStreamClientFutureOutcome struct {
	DirectCode     string
	DirectMessage  string
	DirectIsFuture bool
	StreamCreated  bool
	StreamCode     string
	StreamMessage  string
	StreamIsFuture bool
}

func TestRangeStreamClientFutureDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run RangeStream client future-revision differential tests")
	}

	want := runRangeStreamClientFutureScenario(t, reference)
	require.Equal(t, rangeStreamClientFutureOutcome{
		DirectCode: codes.Unknown.String(), DirectMessage: rpctypes.ErrFutureRev.Error(), DirectIsFuture: true,
		StreamCreated: true, StreamCode: codes.Unknown.String(), StreamMessage: rpctypes.ErrFutureRev.Error(), StreamIsFuture: true,
	}, want)
	require.Equal(t, want, runRangeStreamClientFutureScenario(t, compatEndpoint(t)))
}

func runRangeStreamClientFutureScenario(t *testing.T, endpoint string) rangeStreamClientFutureOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	const key = "/dbaas-rangestream-client-future/read-only"
	_, directErr := client.Get(ctx, key, clientv3.WithRev(math.MaxInt64))
	stream, streamCreateErr := client.GetStream(ctx, key, clientv3.WithRev(math.MaxInt64))
	var streamErr error
	if streamCreateErr == nil {
		_, streamErr = clientv3.GetStreamToGetResponse(stream)
	}
	return rangeStreamClientFutureOutcome{
		DirectCode: status.Code(directErr).String(), DirectMessage: status.Convert(directErr).Message(),
		DirectIsFuture: errors.Is(directErr, rpctypes.ErrFutureRev), StreamCreated: streamCreateErr == nil,
		StreamCode: status.Code(streamErr).String(), StreamMessage: status.Convert(streamErr).Message(),
		StreamIsFuture: errors.Is(streamErr, rpctypes.ErrFutureRev),
	}
}
