package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type deleteRangeBoundaryOutcome struct {
	Name                    string
	Deleted                 int64
	PutRevisionGaps         []int64
	DeleteRevisionGap       int64
	HeaderAtCurrentRevision bool
	PrevKVs                 []deleteRangeBoundaryKV
	Remaining               []deleteRangeBoundaryKV
}

type deleteRangeBoundaryKV struct {
	Key             string
	Value           string
	DeletedAtHeader bool
}

func TestDeleteRangeBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []deleteRangeBoundaryOutcome{
		{
			Name:                    "from-key",
			Deleted:                 2,
			PutRevisionGaps:         []int64{1, 1},
			DeleteRevisionGap:       1,
			HeaderAtCurrentRevision: true,
			PrevKVs: []deleteRangeBoundaryKV{
				{Key: "b", Value: "value-b", DeletedAtHeader: true},
				{Key: "c", Value: "value-c", DeletedAtHeader: true},
			},
			Remaining: []deleteRangeBoundaryKV{{Key: "a", Value: "value-a"}},
		},
		{
			Name:                    "equal-empty",
			PutRevisionGaps:         []int64{1, 1},
			HeaderAtCurrentRevision: true,
			PrevKVs:                 []deleteRangeBoundaryKV{},
			Remaining: []deleteRangeBoundaryKV{
				{Key: "a", Value: "value-a"},
				{Key: "b", Value: "value-b"},
				{Key: "c", Value: "value-c"},
			},
		},
		{
			Name:                    "reverse-empty",
			PutRevisionGaps:         []int64{1, 1},
			HeaderAtCurrentRevision: true,
			PrevKVs:                 []deleteRangeBoundaryKV{},
			Remaining: []deleteRangeBoundaryKV{
				{Key: "a", Value: "value-a"},
				{Key: "b", Value: "value-b"},
				{Key: "c", Value: "value-c"},
			},
		},
	}
	referenceOutcomes := runDeleteRangeBoundaryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runDeleteRangeBoundaryScenario(t, compatEndpoint(t), "kubebrain"))
}

func runDeleteRangeBoundaryScenario(t *testing.T, endpoint, instance string) []deleteRangeBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	tests := []struct {
		name     string
		start    string
		rangeEnd func(string) []byte
	}{
		{name: "from-key", start: "b", rangeEnd: func(string) []byte { return []byte{0} }},
		{name: "equal-empty", start: "b", rangeEnd: func(prefix string) []byte { return []byte(prefix + "b") }},
		{name: "reverse-empty", start: "c", rangeEnd: func(prefix string) []byte { return []byte(prefix + "b") }},
	}
	outcomes := make([]deleteRangeBoundaryOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		prefix := string(bytes.Repeat([]byte{0xff}, 64)) +
			fmt.Sprintf("/dbaas-delete-boundary/%s/%d/%s/", instance, time.Now().UnixNano(), test.name)
		rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
		var putRevisions []int64
		for _, suffix := range []string{"a", "b", "c"} {
			put, putErr := client.Put(ctx, &etcdserverpb.PutRequest{
				Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
			})
			require.NoError(t, putErr)
			putRevisions = append(putRevisions, put.Header.Revision)
		}
		putRevisionGaps := []int64{
			putRevisions[1] - putRevisions[0],
			putRevisions[2] - putRevisions[1],
		}

		deleted, deleteErr := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + test.start), RangeEnd: test.rangeEnd(prefix), PrevKv: true,
		})
		require.NoError(t, deleteErr)
		remaining, rangeErr := client.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
		require.NoError(t, rangeErr)
		outcomes = append(outcomes, deleteRangeBoundaryOutcome{
			Name:                    test.name,
			Deleted:                 deleted.Deleted,
			PutRevisionGaps:         putRevisionGaps,
			DeleteRevisionGap:       deleted.Header.Revision - putRevisions[2],
			HeaderAtCurrentRevision: remaining.Header.Revision == deleted.Header.Revision,
			PrevKVs:                 normalizeDeleteRangeBoundaryKVs(deleted.PrevKvs, prefix, deleted.Header.Revision),
			Remaining:               normalizeDeleteRangeBoundaryKVs(remaining.Kvs, prefix, 0),
		})

		_, cleanupErr := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
		cancel()
		require.NoError(t, cleanupErr)
	}
	return outcomes
}

func normalizeDeleteRangeBoundaryKVs(
	kvs []*mvccpb.KeyValue,
	prefix string,
	deleteRevision int64,
) []deleteRangeBoundaryKV {
	out := make([]deleteRangeBoundaryKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, deleteRangeBoundaryKV{
			Key:             strings.TrimPrefix(string(kv.Key), prefix),
			Value:           string(kv.Value),
			DeletedAtHeader: deleteRevision != 0 && kv.ModRevision < deleteRevision,
		})
	}
	return out
}
