package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type resolutionClient struct {
	clientv3.KV
	put func(context.Context) (*clientv3.PutResponse, error)
	get func(context.Context) (*clientv3.GetResponse, error)
}

func (c resolutionClient) Put(ctx context.Context, _, _ string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return c.put(ctx)
}

func (c resolutionClient) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return c.get(ctx)
}

func TestResolveProbePutRejectsMalformedSuccessWithoutReconciliation(t *testing.T) {
	wrongCluster := rolloutHeader(11)
	wrongCluster.ClusterId++
	for name, response := range map[string]*clientv3.PutResponse{
		"nil response":               nil,
		"nil header":                 {},
		"wrong cluster":              {Header: wrongCluster},
		"nonadvancing revision":      {Header: rolloutHeader(10)},
		"unrequested previous value": {Header: rolloutHeader(11), PrevKv: &mvccpb.KeyValue{}},
	} {
		t.Run(name, func(t *testing.T) {
			progress := newProbeProgress(time.Now(), 1)
			client := resolutionClient{
				put: func(context.Context) (*clientv3.PutResponse, error) { return response, nil },
				get: func(context.Context) (*clientv3.GetResponse, error) {
					t.Fatal("malformed success must not be reconciled")
					return nil, nil
				},
			}
			revision, err := resolveProbePut(context.Background(), client, time.Now().Add(time.Second), "k", "v", 7, 10, progress)
			require.ErrorContains(t, err, "invalid successful put response")
			require.Zero(t, revision)
			require.Equal(t, 1, progress.putCalls)
			require.Zero(t, progress.confirmCalls)
			require.Zero(t, progress.putCallErrors) // Semantic failure, not SDK error.
		})
	}
}

func TestResolveProbePutSuccessAndUncertainWrite(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "uncertain"}[uncertain], func(t *testing.T) {
			deadline := time.Now().Add(time.Second)
			checkDeadline := func(ctx context.Context) {
				actual, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, deadline, actual)
			}
			client := resolutionClient{
				put: func(ctx context.Context) (*clientv3.PutResponse, error) {
					checkDeadline(ctx)
					if uncertain {
						return nil, errors.New("reply lost")
					}
					return &clientv3.PutResponse{Header: rolloutHeader(11)}, nil
				},
				get: func(ctx context.Context) (*clientv3.GetResponse, error) {
					checkDeadline(ctx)
					require.True(t, uncertain)
					return &clientv3.GetResponse{Header: rolloutHeader(12), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte("v"), CreateRevision: 11, ModRevision: 11, Version: 1}}}, nil
				},
			}
			progress := newProbeProgress(time.Now(), 1)
			revision, err := resolveProbePut(context.Background(), client, deadline, "k", "v", 7, 10, progress)
			require.NoError(t, err)
			require.Equal(t, int64(11), revision)
			require.Equal(t, 1, progress.putCalls)
			if uncertain {
				require.Equal(t, 1, progress.confirmCalls)
				require.Equal(t, 1, progress.putCallErrors)
			} else {
				require.Zero(t, progress.confirmCalls)
			}
		})
	}
}

func TestResolveProbePutDoesNotExtendExpiredDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	check := func(ctx context.Context) {
		actual, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, actual)
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	}
	client := resolutionClient{
		put: func(ctx context.Context) (*clientv3.PutResponse, error) { check(ctx); return nil, ctx.Err() },
		get: func(ctx context.Context) (*clientv3.GetResponse, error) { check(ctx); return nil, ctx.Err() },
	}
	progress := newProbeProgress(time.Now(), 1)
	_, err := resolveProbePut(context.Background(), client, deadline, "k", "v", 7, 10, progress)
	require.ErrorContains(t, err, "put unresolved before deadline")
	require.Equal(t, 1, progress.putCalls)
	require.Equal(t, 1, progress.confirmCalls)
}

func TestResolveProbePutRetriesUnconfirmedWriteWithinOriginalDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	puts := 0
	client := resolutionClient{
		put: func(ctx context.Context) (*clientv3.PutResponse, error) {
			actual, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, deadline, actual)
			puts++
			if puts == 1 {
				return nil, errors.New("connection lost")
			}
			return &clientv3.PutResponse{Header: rolloutHeader(11)}, nil
		},
		get: func(context.Context) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Header: rolloutHeader(10)}, nil
		},
	}
	progress := newProbeProgress(time.Now(), 1)
	revision, err := resolveProbePut(context.Background(), client, deadline, "k", "v", 7, 10, progress)
	require.NoError(t, err)
	require.Equal(t, int64(11), revision)
	require.Equal(t, 2, progress.putCalls)
	require.Equal(t, 1, progress.confirmCalls)
}
