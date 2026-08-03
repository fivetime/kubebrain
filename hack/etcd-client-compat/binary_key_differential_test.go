package compat

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type binaryKeyOutcome struct {
	Name   string
	Keys   []string
	Values []string
	Count  int64
	More   bool
}

func TestBinaryKeyDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run binary key differential tests")
	}

	require.Equal(t,
		runBinaryKeyScenario(t, reference),
		runBinaryKeyScenario(t, compatEndpoint(t)),
	)
}

func runBinaryKeyScenario(t *testing.T, endpoint string) []binaryKeyOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	keys := [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x7f, 0x00},
		{0xfe},
		{0xfe, 0x00},
		{0xfe, 0x01},
		{0xff},
		{0xff, 0x00},
		{0xff, 0x01},
	}
	requireBinaryRangesEmpty(t, ctx, client,
		&etcdserverpb.RangeRequest{Key: []byte{0x00}, RangeEnd: []byte{0x01}},
		&etcdserverpb.RangeRequest{Key: []byte{0x7f, 0x00}},
		&etcdserverpb.RangeRequest{Key: []byte{0xfe}, RangeEnd: []byte{0xff}},
		&etcdserverpb.RangeRequest{Key: []byte{0xff}, RangeEnd: []byte{0}},
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		deleteExactKeys(cleanupCtx, client, keys)
	})
	for i, key := range keys {
		_, err = client.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte{byte('a' + i)},
		})
		require.NoError(t, err)
	}

	outcomes := []binaryKeyOutcome{
		binaryRangeOutcome(t, ctx, client, "nul-point", &etcdserverpb.RangeRequest{Key: []byte{0x00}}),
		binaryRangeOutcome(t, ctx, client, "nul-prefix", &etcdserverpb.RangeRequest{
			Key: []byte{0x00}, RangeEnd: []byte{0x01},
		}),
		binaryRangeStreamOutcome(t, ctx, client, "nul-prefix-stream", &etcdserverpb.RangeRequest{
			Key: []byte{0x00}, RangeEnd: []byte{0x01},
		}),
		binaryRangeOutcome(t, ctx, client, "embedded-nul-point", &etcdserverpb.RangeRequest{
			Key: []byte{0x7f, 0x00},
		}),
		binaryRangeOutcome(t, ctx, client, "high-prefix", &etcdserverpb.RangeRequest{
			Key: []byte{0xfe}, RangeEnd: []byte{0xff},
		}),
		binaryRangeStreamOutcome(t, ctx, client, "high-prefix-stream", &etcdserverpb.RangeRequest{
			Key: []byte{0xfe}, RangeEnd: []byte{0xff},
		}),
		binaryRangeOutcome(t, ctx, client, "ff-from-key", &etcdserverpb.RangeRequest{
			Key: []byte{0xff}, RangeEnd: []byte{0},
		}),
		binaryRangeStreamOutcome(t, ctx, client, "ff-from-key-stream", &etcdserverpb.RangeRequest{
			Key: []byte{0xff}, RangeEnd: []byte{0},
		}),
		binaryRangeOutcome(t, ctx, client, "ff-limited-descending", &etcdserverpb.RangeRequest{
			Key: []byte{0xff}, RangeEnd: []byte{0}, Limit: 2,
			SortTarget: etcdserverpb.RangeRequest_KEY, SortOrder: etcdserverpb.RangeRequest_DESCEND,
		}),
	}

	beforeDelete, err := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte{0xff}})
	require.NoError(t, err)
	require.Len(t, beforeDelete.Kvs, 1)
	_, err = client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte{0xff}})
	require.NoError(t, err)
	outcomes = append(outcomes,
		binaryRangeOutcome(t, ctx, client, "ff-deleted-current", &etcdserverpb.RangeRequest{Key: []byte{0xff}}),
		binaryRangeOutcome(t, ctx, client, "ff-deleted-historical", &etcdserverpb.RangeRequest{
			Key: []byte{0xff}, Revision: beforeDelete.Header.Revision,
		}),
	)
	return outcomes
}

func requireBinaryRangesEmpty(
	t *testing.T,
	ctx context.Context,
	client etcdserverpb.KVClient,
	ranges ...*etcdserverpb.RangeRequest,
) {
	t.Helper()
	for _, req := range ranges {
		probe := proto.Clone(req).(*etcdserverpb.RangeRequest)
		probe.Limit = 1
		probe.KeysOnly = true
		resp, err := client.Range(ctx, probe)
		require.NoError(t, err)
		require.Emptyf(t, resp.Kvs,
			"binary differential tests require a disposable endpoint; range [%x,%x) contains unrelated key %x",
			req.Key, req.RangeEnd, firstKey(resp))
	}
}

func firstKey(resp *etcdserverpb.RangeResponse) []byte {
	if len(resp.Kvs) == 0 {
		return nil
	}
	return resp.Kvs[0].Key
}

func deleteExactKeys(ctx context.Context, client etcdserverpb.KVClient, keys [][]byte) {
	for _, key := range keys {
		_, _ = client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	}
}

func binaryRangeOutcome(
	t *testing.T,
	ctx context.Context,
	client etcdserverpb.KVClient,
	name string,
	req *etcdserverpb.RangeRequest,
) binaryKeyOutcome {
	t.Helper()
	resp, err := client.Range(ctx, req)
	require.NoError(t, err, name)
	out := binaryKeyOutcome{Name: name, Count: resp.Count, More: resp.More}
	for _, kv := range resp.Kvs {
		out.Keys = append(out.Keys, hex.EncodeToString(kv.Key))
		out.Values = append(out.Values, string(kv.Value))
	}
	return out
}

func binaryRangeStreamOutcome(
	t *testing.T,
	ctx context.Context,
	client etcdserverpb.KVClient,
	name string,
	req *etcdserverpb.RangeRequest,
) binaryKeyOutcome {
	t.Helper()
	stream, err := client.RangeStream(ctx, req)
	require.NoError(t, err, name)
	out := binaryKeyOutcome{Name: name}
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			require.ErrorIs(t, recvErr, io.EOF, name)
			return out
		}
		rangeResponse := response.GetRangeResponse()
		require.NotNil(t, rangeResponse, name)
		out.Count = rangeResponse.Count
		out.More = rangeResponse.More
		for _, kv := range rangeResponse.Kvs {
			out.Keys = append(out.Keys, hex.EncodeToString(kv.Key))
			out.Values = append(out.Values, string(kv.Value))
		}
	}
}
