package compat

import (
	"context"
	"fmt"
	"math"
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

type rangeMatrixFilterCase struct {
	name string
	minM int64
	maxM int64
	minC int64
	maxC int64
}

type rangeMatrixMode struct {
	name      string
	keysOnly  bool
	countOnly bool
}

type rangeMatrixRevisionCase struct {
	name     string
	revision int64
}

func TestRangeOptionMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run Range option matrix differential tests")
	}

	want := runRangeOptionMatrixScenario(t, reference, "etcd")
	got := runRangeOptionMatrixScenario(t, compatEndpoint(t), "kubebrain")
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i], got[i], want[i].Name)
	}
}

func TestRangeOptionMatrixCoversFastKeysOnlyTotalCountFamilies(t *testing.T) {
	covered := map[string]bool{}
	for _, revision := range rangeMatrixRevisions(6) {
		for _, target := range rangeMatrixTargets() {
			for _, limit := range rangeMatrixLimits() {
				for _, filter := range rangeMatrixFilters([]int64{1, 2, 3, 4, 5, 6}, []int64{1, 2, 3, 4, 5, 6}) {
					for _, mode := range rangeMatrixModes() {
						if !mode.keysOnly || mode.countOnly || target == etcdserverpb.RangeRequest_VALUE {
							continue
						}
						switch filter.name {
						case "none", "mod-window", "create-window", "contradictory":
							covered[fmt.Sprintf("%s/%s/limit=%d/filter=%s", revision.name, target, limit, filter.name)] = true
						}
					}
				}
			}
		}
	}

	for _, revision := range []string{"current", "historical"} {
		for _, target := range []etcdserverpb.RangeRequest_SortTarget{
			etcdserverpb.RangeRequest_KEY,
			etcdserverpb.RangeRequest_VERSION,
			etcdserverpb.RangeRequest_CREATE,
			etcdserverpb.RangeRequest_MOD,
		} {
			for _, filter := range []string{"none", "mod-window", "create-window", "contradictory"} {
				require.True(t, covered[fmt.Sprintf("%s/%s/limit=2/filter=%s", revision, target, filter)],
					"missing limited fast KeysOnly coverage for %s %s %s", revision, target, filter)
				require.True(t, covered[fmt.Sprintf("%s/%s/limit=%d/filter=%s", revision, target, int64(math.MaxInt64), filter)],
					"missing total-count fast KeysOnly coverage for %s %s %s", revision, target, filter)
			}
		}
	}
}

func TestRangeOptionMatrixCoversUpstreamMinMaxCreateModSortCases(t *testing.T) {
	covered := map[string]bool{}
	for _, target := range rangeMatrixTargets() {
		for _, order := range rangeMatrixOrders() {
			for _, filter := range rangeMatrixFilters([]int64{1, 2, 3, 4, 5, 6}, []int64{1, 2, 3, 4, 5, 6}) {
				for _, mode := range rangeMatrixModes() {
					if mode.keysOnly || mode.countOnly {
						continue
					}
					covered[fmt.Sprintf("%s/%s/%s/%s", filter.name, target, order, mode.name)] = true
				}
			}
		}
	}

	for _, tc := range []struct {
		filter string
		target etcdserverpb.RangeRequest_SortTarget
		order  etcdserverpb.RangeRequest_SortOrder
	}{
		{filter: "min-mod", target: etcdserverpb.RangeRequest_MOD, order: etcdserverpb.RangeRequest_NONE},
		{filter: "max-mod", target: etcdserverpb.RangeRequest_KEY, order: etcdserverpb.RangeRequest_DESCEND},
		{filter: "min-create", target: etcdserverpb.RangeRequest_VERSION, order: etcdserverpb.RangeRequest_DESCEND},
		{filter: "max-create", target: etcdserverpb.RangeRequest_VALUE, order: etcdserverpb.RangeRequest_DESCEND},
	} {
		require.True(t, covered[fmt.Sprintf("%s/%s/%s/full", tc.filter, tc.target, tc.order)],
			"missing upstream 43a6f8fa7 Range min/max create/mod sort coverage for %s %s %s",
			tc.filter, tc.target, tc.order)
	}
}

func rangeMatrixFilters(createRevisions, modRevisions []int64) []rangeMatrixFilterCase {
	return []rangeMatrixFilterCase{
		{name: "none"},
		{name: "min-mod", minM: modRevisions[1]},
		{name: "max-mod", maxM: modRevisions[3]},
		{name: "min-create", minC: createRevisions[1]},
		{name: "max-create", maxC: createRevisions[4]},
		{name: "mod-window", minM: modRevisions[1], maxM: modRevisions[4]},
		{name: "create-window", minC: createRevisions[1], maxC: createRevisions[4]},
		{name: "contradictory", minM: modRevisions[4], maxM: modRevisions[1]},
		{name: "negative-min", minM: -1, minC: -1},
		{name: "negative-max", maxM: -1, maxC: -1},
		{name: "maximum-min", minM: math.MaxInt64, minC: math.MaxInt64},
		{name: "maximum-max", maxM: math.MaxInt64, maxC: math.MaxInt64},
	}
}

func rangeMatrixModes() []rangeMatrixMode {
	return []rangeMatrixMode{
		{name: "full"},
		{name: "keys", keysOnly: true},
		{name: "count", countOnly: true},
		{name: "keys-and-count", keysOnly: true, countOnly: true},
	}
}

func rangeMatrixTargets() []etcdserverpb.RangeRequest_SortTarget {
	return []etcdserverpb.RangeRequest_SortTarget{
		etcdserverpb.RangeRequest_KEY,
		etcdserverpb.RangeRequest_VERSION,
		etcdserverpb.RangeRequest_CREATE,
		etcdserverpb.RangeRequest_MOD,
		etcdserverpb.RangeRequest_VALUE,
	}
}

func rangeMatrixOrders() []etcdserverpb.RangeRequest_SortOrder {
	return []etcdserverpb.RangeRequest_SortOrder{
		etcdserverpb.RangeRequest_NONE,
		etcdserverpb.RangeRequest_ASCEND,
		etcdserverpb.RangeRequest_DESCEND,
	}
}

func rangeMatrixRevisions(historicalRevision int64) []rangeMatrixRevisionCase {
	return []rangeMatrixRevisionCase{
		{name: "current"},
		{name: "historical", revision: historicalRevision},
	}
}

func rangeMatrixLimits() []int64 {
	return []int64{-1, 0, 2, math.MaxInt64}
}

func runRangeOptionMatrixScenario(t *testing.T, endpoint, instance string) []rangeMatrixOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	recreated, err := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "f"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), recreated.Deleted)
	for version := 0; version < 6; version++ {
		put, putErr := client.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "f"), Value: []byte(fmt.Sprintf("recreated-%d", version)),
		})
		require.NoError(t, putErr)
		if version == 0 {
			createRevisions[5] = put.Header.Revision
		}
		modRevisions[5] = put.Header.Revision
	}

	filters := rangeMatrixFilters(createRevisions, modRevisions)
	modes := rangeMatrixModes()
	targets := rangeMatrixTargets()
	orders := rangeMatrixOrders()
	revisions := rangeMatrixRevisions(historicalRevision)
	limits := rangeMatrixLimits()
	outcomes := make([]rangeMatrixOutcome, 0, len(revisions)*len(targets)*len(orders)*len(limits)*len(filters)*len(modes))
	for _, revision := range revisions {
		for _, target := range targets {
			for _, order := range orders {
				for _, limit := range limits {
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
