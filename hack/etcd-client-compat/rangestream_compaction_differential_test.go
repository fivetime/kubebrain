package compat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rangeStreamCompactionOutcome struct {
	Code           string
	ReceivedKeys   int
	Completed      bool
	CompactedError bool
}

func TestRangeStreamPartialCompactionDifferential(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_COMPACTION_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_COMPACTION_ENDPOINT to disposable instances")
	}
	if mainEndpoint, ok := configuredCompatEndpoint(); ok {
		require.NotEqual(t, mirrorEndpointIdentity(mainEndpoint), mirrorEndpointIdentity(kubebrain),
			"KUBEBRAIN_COMPACTION_ENDPOINT must not be the shared main endpoint")
	}

	want := runRangeStreamPartialCompaction(t, reference, "reference")
	got := runRangeStreamPartialCompaction(t, kubebrain, "kubebrain")
	require.Equal(t, codes.OutOfRange.String(), want.Code)
	require.Equal(t, codes.OutOfRange.String(), got.Code)
	require.True(t, want.CompactedError)
	require.True(t, got.CompactedError)
	require.False(t, want.Completed)
	require.False(t, got.Completed)
	require.Positive(t, want.ReceivedKeys)
	require.Positive(t, got.ReceivedKeys)
	require.Less(t, want.ReceivedKeys, 200)
	require.Less(t, got.ReceivedKeys, 200)
}

func runRangeStreamPartialCompaction(
	t *testing.T,
	endpoint, instance string,
) rangeStreamCompactionOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/zz-dbaas-rangestream-compact/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, deleteErr := client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, deleteErr)
		remaining, getErr := client.Get(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
	})
	value := strings.Repeat("x", 32*1024)
	for i := 0; i < 200; i++ {
		_, err = client.Put(ctx, fmt.Sprintf("%s%03d", prefix, i), value)
		require.NoError(t, err)
	}

	kvClient := etcdserverpb.NewKVClient(client.ActiveConnection())
	stream, err := kvClient.RangeStream(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
	})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	received := len(first.GetRangeResponse().GetKvs())
	require.Positive(t, received)

	advance, err := client.Put(ctx, prefix+"zzz-advance", "advance")
	require.NoError(t, err)
	_, err = client.Compact(ctx, advance.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	outcome := rangeStreamCompactionOutcome{ReceivedKeys: received}
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			outcome.Completed = true
			return outcome
		}
		if recvErr != nil {
			outcome.Code = status.Code(recvErr).String()
			outcome.CompactedError = strings.Contains(
				status.Convert(recvErr).Message(), "required revision has been compacted",
			)
			return outcome
		}
		outcome.ReceivedKeys += len(chunk.GetRangeResponse().GetKvs())
	}
}
