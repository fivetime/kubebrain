package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

const (
	defaultRangeStreamOversizePrefix = "/dbaas-rangestream-oversize/"
	rangeStreamOversizeValueSize     = 4 * 1024 * 1024
	rangeStreamOversizeKeyCount      = 12
)

type rangeStreamOversizeKVOutcome struct {
	MultipleResponses bool
	Keys              int
	Count             int64
	More              bool
	HeaderPresent     bool
	StrictlyOrdered   bool
	ValuesIntact      bool
}

func TestRangeStreamOversizeKVDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_OVERSIZE_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_OVERSIZE_ENDPOINT")
	}
	if os.Getenv("RANGESTREAM_OVERSIZE_SEED") == "true" {
		seedRangeStreamOversizeFixture(t, reference)
		seedRangeStreamOversizeFixture(t, kubebrain)
		return
	}

	want := readRangeStreamOversizeFixture(t, reference)
	require.Equal(t, rangeStreamOversizeKVOutcome{
		MultipleResponses: true, Keys: rangeStreamOversizeKeyCount, Count: rangeStreamOversizeKeyCount,
		HeaderPresent: true, StrictlyOrdered: true, ValuesIntact: true,
	}, want)
	require.Equal(t, want, readRangeStreamOversizeFixture(t, kubebrain))
}

func seedRangeStreamOversizeFixture(t *testing.T, endpoint string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prefix := rangeStreamOversizePrefix()
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	rawKV := etcdserverpb.NewKVClient(client.ActiveConnection())
	for i := 0; i < rangeStreamOversizeKeyCount; i++ {
		_, err = rawKV.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("%s%02d", prefix, i)), Value: rangeStreamOversizeValue(i),
		}, grpc.MaxCallSendMsgSize(9*1024*1024))
		require.NoError(t, err)
	}
}

func readRangeStreamOversizeFixture(t *testing.T, endpoint string) rangeStreamOversizeKVOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	prefix := rangeStreamOversizePrefix()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, deleteErr := client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, deleteErr)
		remaining, getErr := client.Get(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, getErr)
		require.Empty(t, remaining.Kvs)
	})

	stream, err := etcdserverpb.NewKVClient(client.ActiveConnection()).RangeStream(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
	}, grpc.MaxCallRecvMsgSize(64*1024*1024))
	require.NoError(t, err)

	outcome := rangeStreamOversizeKVOutcome{StrictlyOrdered: true, ValuesIntact: true}
	responses := 0
	previousKey := ""
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		responses++
		response := chunk.GetRangeResponse()
		if response.GetHeader() != nil {
			outcome.HeaderPresent = true
			outcome.Count = response.GetCount()
			outcome.More = response.GetMore()
		}
		for _, kv := range response.GetKvs() {
			key := string(kv.Key)
			if previousKey != "" && previousKey >= key {
				outcome.StrictlyOrdered = false
			}
			previousKey = key
			index := outcome.Keys
			outcome.Keys++
			if index >= rangeStreamOversizeKeyCount ||
				sha256.Sum256(kv.Value) != sha256.Sum256(rangeStreamOversizeValue(index)) {
				outcome.ValuesIntact = false
			}
		}
	}
	outcome.MultipleResponses = responses > 1
	return outcome
}

func rangeStreamOversizePrefix() string {
	if prefix := os.Getenv("RANGESTREAM_OVERSIZE_PREFIX"); prefix != "" {
		return prefix
	}
	return defaultRangeStreamOversizePrefix
}

func rangeStreamOversizeValue(index int) []byte {
	value := make([]byte, rangeStreamOversizeValueSize)
	for i := range value {
		value[i] = byte((i + index) % 251)
	}
	return value
}
