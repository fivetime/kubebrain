package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type rangeMatrixOutcome struct {
	Name    string
	Code    string
	Message string
	Range   rangeOptionOutcome
}

func TestRangeOptionMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run Range option matrix differential tests")
	}

	want := runRangeOptionMatrixScenario(t, reference, "etcd")
	got := runRangeOptionMatrixScenario(t, compatEndpoint(), "kubebrain")
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i], got[i], want[i].Name)
	}
}

func runRangeOptionMatrixScenario(t *testing.T, endpoint, instance string) []rangeMatrixOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-range-matrix/%s/%d/", instance, time.Now().UnixNano())
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: end,
		})
	})

	var createRevisions, modRevisions []int64
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("%s%c", prefix, 'a'+i)
		created, putErr := client.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(key), Value: []byte(fmt.Sprintf("initial-%d", i)),
		})
		require.NoError(t, putErr)
		createRevisions = append(createRevisions, created.Header.Revision)
		modRevisions = append(modRevisions, created.Header.Revision)
	}
	historicalRevision := createRevisions[len(createRevisions)-1]
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("%s%c", prefix, 'a'+i)
		lastRevision := modRevisions[i]
		for version := 1; version <= i; version++ {
			updated, updateErr := client.Put(ctx, &etcdserverpb.PutRequest{
				Key: []byte(key), Value: []byte(fmt.Sprintf("value-%d-%d", 6-i, version)),
			})
			require.NoError(t, updateErr)
			lastRevision = updated.Header.Revision
		}
		modRevisions[i] = lastRevision
	}

	type filterCase struct {
		name string
		minM int64
		maxM int64
		minC int64
		maxC int64
	}
	filters := []filterCase{
		{name: "none"},
		{name: "mod-window", minM: modRevisions[1], maxM: modRevisions[4]},
		{name: "create-window", minC: createRevisions[1], maxC: createRevisions[4]},
		{name: "contradictory", minM: modRevisions[4], maxM: modRevisions[1]},
	}
	modes := []struct {
		name      string
		keysOnly  bool
		countOnly bool
	}{
		{name: "full"},
		{name: "keys", keysOnly: true},
		{name: "count", countOnly: true},
	}
	targets := []etcdserverpb.RangeRequest_SortTarget{
		etcdserverpb.RangeRequest_KEY,
		etcdserverpb.RangeRequest_VERSION,
		etcdserverpb.RangeRequest_CREATE,
		etcdserverpb.RangeRequest_MOD,
		etcdserverpb.RangeRequest_VALUE,
	}
	orders := []etcdserverpb.RangeRequest_SortOrder{
		etcdserverpb.RangeRequest_NONE,
		etcdserverpb.RangeRequest_ASCEND,
		etcdserverpb.RangeRequest_DESCEND,
	}

	revisions := []struct {
		name     string
		revision int64
	}{
		{name: "current"},
		{name: "historical", revision: historicalRevision},
	}
	outcomes := make([]rangeMatrixOutcome, 0, len(revisions)*len(targets)*len(orders)*2*len(filters)*len(modes))
	for _, revision := range revisions {
		for _, target := range targets {
			for _, order := range orders {
				for _, limit := range []int64{0, 2} {
					for _, filter := range filters {
						for _, mode := range modes {
							name := fmt.Sprintf(
								"revision=%s/target=%s/order=%s/limit=%d/filter=%s/mode=%s",
								revision.name, target, order, limit, filter.name, mode.name,
							)
							resp, rangeErr := client.Range(ctx, &etcdserverpb.RangeRequest{
								Key: []byte(prefix), RangeEnd: end, Limit: limit, Revision: revision.revision,
								SortTarget: target, SortOrder: order,
								MinModRevision: filter.minM, MaxModRevision: filter.maxM,
								MinCreateRevision: filter.minC, MaxCreateRevision: filter.maxC,
								KeysOnly: mode.keysOnly, CountOnly: mode.countOnly,
							})
							outcome := rangeMatrixOutcome{
								Name: name, Code: status.Code(rangeErr).String(),
								Message: status.Convert(rangeErr).Message(),
							}
							if rangeErr == nil {
								outcome.Range = rangeOptionResult(name, resp, prefix, 0)
							}
							outcomes = append(outcomes, outcome)
						}
					}
				}
			}
		}
	}

	return outcomes
}
