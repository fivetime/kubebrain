package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type rangeFilterBoundaryOutcome struct {
	Keys  []string
	Count int64
	More  bool
}

func TestRangeRevisionFilterBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	referenceOutcome := runRangeRevisionFilterBoundaryScenario(t, reference, "reference")
	all := rangeFilterBoundaryOutcome{Keys: []string{"a", "b", "c"}, Count: 3}
	filteredRange := rangeFilterBoundaryOutcome{Count: 3}
	require.Equal(t, map[string]rangeFilterBoundaryOutcome{
		"min-mod-negative":       all,
		"max-mod-negative":       filteredRange,
		"min-create-negative":    all,
		"max-create-negative":    filteredRange,
		"min-mod-max-int":        filteredRange,
		"max-mod-max-int":        all,
		"inverted-mod-bounds":    filteredRange,
		"inverted-create-bounds": filteredRange,
		"negative-count-only":    filteredRange,
		"negative-keys-limited":  {Keys: []string{"a", "b"}, Count: 3, More: true},
		"point-max-negative":     {Count: 1},
	}, referenceOutcome)
	require.Equal(t, referenceOutcome, runRangeRevisionFilterBoundaryScenario(t, kubebrain, "kubebrain"))
}

func runRangeRevisionFilterBoundaryScenario(t *testing.T, endpoint, instance string) map[string]rangeFilterBoundaryOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/compat/range-filter-boundary/%s/%d/", instance, time.Now().UnixNano())
	end := clientv3.GetPrefixRangeEnd(prefix)
	first, err := client.Put(ctx, prefix+"a", "a")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "b")
	require.NoError(t, err)
	latest, err := client.Put(ctx, prefix+"c", "c")
	require.NoError(t, err)
	updated, err := client.Put(ctx, prefix+"b", "b2")
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	}()

	requests := map[string]*etcdserverpb.RangeRequest{
		"min-mod-negative":       {Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: -1},
		"max-mod-negative":       {Key: []byte(prefix), RangeEnd: []byte(end), MaxModRevision: -1},
		"min-create-negative":    {Key: []byte(prefix), RangeEnd: []byte(end), MinCreateRevision: -1},
		"max-create-negative":    {Key: []byte(prefix), RangeEnd: []byte(end), MaxCreateRevision: -1},
		"min-mod-max-int":        {Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: math.MaxInt64},
		"max-mod-max-int":        {Key: []byte(prefix), RangeEnd: []byte(end), MaxModRevision: math.MaxInt64},
		"inverted-mod-bounds":    {Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: updated.Header.Revision, MaxModRevision: latest.Header.Revision},
		"inverted-create-bounds": {Key: []byte(prefix), RangeEnd: []byte(end), MinCreateRevision: latest.Header.Revision, MaxCreateRevision: first.Header.Revision},
		"negative-count-only":    {Key: []byte(prefix), RangeEnd: []byte(end), MaxModRevision: -1, CountOnly: true, Limit: 1},
		"negative-keys-limited":  {Key: []byte(prefix), RangeEnd: []byte(end), MinCreateRevision: -1, KeysOnly: true, Limit: 2},
		"point-max-negative":     {Key: []byte(prefix + "b"), MaxCreateRevision: -1},
	}
	raw := etcdserverpb.NewKVClient(client.ActiveConnection())
	outcomes := make(map[string]rangeFilterBoundaryOutcome, len(requests))
	for name, request := range requests {
		response, rangeErr := raw.Range(ctx, request)
		require.NoError(t, rangeErr, name)
		outcome := rangeFilterBoundaryOutcome{Count: response.Count, More: response.More}
		for _, kv := range response.Kvs {
			require.True(t, len(kv.Key) >= len(prefix), name)
			outcome.Keys = append(outcome.Keys, string(kv.Key[len(prefix):]))
			if request.KeysOnly {
				require.Empty(t, kv.Value, name)
			}
		}
		outcomes[name] = outcome
	}
	return outcomes
}
