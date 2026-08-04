package compat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type followerRangeStreamOutcome struct {
	Scenario       string
	Keys           []string
	Values         []string
	Count          int64
	More           bool
	HeaderRevDelta int64
	Code           string
	Message        string
}

// TestRangeStreamFollowerDifferentialAgainstReferenceEtcd exercises direct
// follower endpoints. A load-balanced endpoint cannot prove that Serializable
// latest/historical reads and linearizable reads preserve RangeStream's unary
// contract on the serving follower.
func TestRangeStreamFollowerDifferentialAgainstReferenceEtcd(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")

	reference := followerRangeStreamOutcomes(t, referenceEndpoints, "reference")
	want := make([]followerRangeStreamOutcome, 0, 8)
	for range 2 {
		want = append(want,
			followerRangeStreamOutcome{Scenario: "serializable-latest", Keys: []string{"a", "b"}, Values: []string{"v2", "vb"}, Count: 2, HeaderRevDelta: 3},
			followerRangeStreamOutcome{Scenario: "serializable-negative", Keys: []string{"a", "b"}, Values: []string{"v2", "vb"}, Count: 2, HeaderRevDelta: 3},
			followerRangeStreamOutcome{Scenario: "serializable-historical", Keys: []string{"a"}, Values: []string{"v1"}, Count: 1, HeaderRevDelta: 3},
			followerRangeStreamOutcome{Scenario: "linearizable-latest", Keys: []string{"a", "b"}, Values: []string{"v2", "vb"}, Count: 2, HeaderRevDelta: 3},
		)
	}
	require.Equal(t, want, reference)
	require.Equal(t, reference, followerRangeStreamOutcomes(t, kubeBrainEndpoints, "kubebrain"))
}

func followerRangeStreamOutcomes(t *testing.T, endpoints []string, instance string) []followerRangeStreamOutcome {
	t.Helper()
	type replica struct {
		conn   *grpc.ClientConn
		status *etcdserverpb.StatusResponse
	}
	replicas := make([]replica, 0, len(endpoints))
	for _, endpoint := range endpoints {
		conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, statusErr := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
		cancel()
		require.NoError(t, statusErr, endpoint)
		replicas = append(replicas, replica{conn: conn, status: response})
	}
	require.NoError(t, validateDirectReplicaTopology([]*etcdserverpb.StatusResponse{
		replicas[0].status, replicas[1].status, replicas[2].status,
	}))

	leaderID := replicas[0].status.Leader
	var leader replica
	for _, candidate := range replicas {
		if candidate.status.Header.MemberId == leaderID {
			leader = candidate
			break
		}
	}
	require.NotNil(t, leader.conn)
	kv := etcdserverpb.NewKVClient(leader.conn)
	prefix := fmt.Sprintf("/dbaas-rangestream-follower/%s/%d/", instance, time.Now().UnixNano())
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("v1")})
	require.NoError(t, err)
	first, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix + "a")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("vb")})
	require.NoError(t, err)
	last, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("v2")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix), RangeEnd: end})
	})

	for _, follower := range replicas {
		if follower.status.Header.MemberId == leaderID {
			continue
		}
		waitForFollowerRangeRevision(t, follower.conn, prefix, end, last.Header.Revision)
	}

	scenarios := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "serializable-latest", req: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, Serializable: true}},
		{name: "serializable-negative", req: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, Revision: math.MinInt64, Serializable: true}},
		{name: "serializable-historical", req: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, Revision: first.Header.Revision, Serializable: true}},
		{name: "linearizable-latest", req: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end}},
	}
	outcomes := make([]followerRangeStreamOutcome, 0, 2*len(scenarios))
	for _, follower := range replicas {
		if follower.status.Header.MemberId == leaderID {
			continue
		}
		followerKV := etcdserverpb.NewKVClient(follower.conn)
		for _, scenario := range scenarios {
			unary, unaryErr := followerKV.Range(ctx, proto.Clone(scenario.req).(*etcdserverpb.RangeRequest))
			require.NoError(t, unaryErr, scenario.name)
			streamed, streamErr := receiveRawRangeStream(ctx, followerKV, proto.Clone(scenario.req).(*etcdserverpb.RangeRequest))
			outcome := followerRangeStreamOutcome{Scenario: scenario.name}
			if streamErr != nil {
				outcome.Code = status.Code(streamErr).String()
				outcome.Message = status.Convert(streamErr).Message()
				outcomes = append(outcomes, outcome)
				continue
			}
			require.True(t, proto.Equal(unary, streamed), "%s follower RangeStream differs from unary: unary=%s stream=%s", scenario.name, unary, streamed)
			for _, item := range streamed.Kvs {
				outcome.Keys = append(outcome.Keys, strings.TrimPrefix(string(item.Key), prefix))
				outcome.Values = append(outcome.Values, string(item.Value))
			}
			outcome.Count = streamed.Count
			outcome.More = streamed.More
			outcome.HeaderRevDelta = streamed.Header.Revision - base.Header.Revision
			outcomes = append(outcomes, outcome)
		}
	}
	return outcomes
}

func waitForFollowerRangeRevision(t *testing.T, conn *grpc.ClientConn, prefix string, end []byte, revision int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, err := etcdserverpb.NewKVClient(conn).Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: end, Serializable: true,
		})
		cancel()
		if err == nil && response.Header.Revision >= revision && len(response.Kvs) == 2 {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
			require.GreaterOrEqual(t, response.Header.Revision, revision)
			require.Len(t, response.Kvs, 2)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func receiveRawRangeStream(ctx context.Context, client etcdserverpb.KVClient, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	stream, err := client.RangeStream(ctx, req)
	if err != nil {
		return nil, err
	}
	merged := &etcdserverpb.RangeResponse{}
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return merged, nil
		}
		if recvErr != nil {
			return nil, recvErr
		}
		proto.Merge(merged, chunk.RangeResponse)
	}
}
