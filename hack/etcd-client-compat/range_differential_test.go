package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type normalizedRange struct {
	HeaderRev int64
	KVs       []normalizedKV
	Count     int64
	More      bool
}

type rangeDifferentialResult struct {
	PutRevisions       []int64
	DeleteRevision     int64
	Historical         normalizedRange
	Filtered           normalizedRange
	FilteredCountOnly  normalizedRange
	FilteredLimited    normalizedRange
	MaxModFiltered     normalizedRange
	MaxCreateFiltered  normalizedRange
	Limited            normalizedRange
	KeysOnly           normalizedRange
	ValueSortedKeys    normalizedRange
	CreateSorted       normalizedRange
	ModSorted          normalizedRange
	VersionSorted      normalizedRange
	PointCountOnly     normalizedRange
	PointKeysOnly      normalizedRange
	MissingPoint       normalizedRange
	EmptyInterval      normalizedRange
	NegativeLimit      normalizedRange
	NegativeRevision   normalizedRange
	MinRevision        normalizedRange
	MinRevisionPoint   normalizedRange
	MinRevisionCount   normalizedRange
	NoOpDeleteRevision int64
	NoOpDeleteCount    int64
	RevisionAfterNoOp  int64
	FutureErrorCode    string
	FutureErrorMessage string
}

func TestRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	etcd := runRangeDifferentialScenario(t, reference, "etcd")
	kv := func(key, value string, create, mod, version int64) normalizedKV {
		return normalizedKV{Key: key, Value: value, CreateRev: create, ModRev: mod, Version: version}
	}
	rng := func(kvs []normalizedKV, count int64, more bool) normalizedRange {
		return normalizedRange{HeaderRev: 5, KVs: kvs, Count: count, More: more}
	}
	a := kv("a", "va", 1, 1, 1)
	b := kv("b", "vb2", 2, 4, 2)
	want := rangeDifferentialResult{
		PutRevisions:   []int64{1, 2, 3, 4},
		DeleteRevision: 5,
		Historical: rng([]normalizedKV{
			a, kv("b", "vb", 2, 2, 1), kv("c", "vc", 3, 3, 1),
		}, 3, false),
		Filtered:          rng([]normalizedKV{b}, 2, false),
		FilteredCountOnly: rng([]normalizedKV{}, 2, false),
		FilteredLimited:   rng([]normalizedKV{a}, 2, true),
		MaxModFiltered:    rng([]normalizedKV{a}, 2, false),
		MaxCreateFiltered: rng([]normalizedKV{a}, 2, false),
		Limited: rng([]normalizedKV{
			kv("c", "vc", 3, 3, 1), kv("b", "vb", 2, 2, 1),
		}, 3, true),
		KeysOnly:           rng([]normalizedKV{kv("a", "", 1, 1, 1), kv("b", "", 2, 4, 2)}, 2, false),
		ValueSortedKeys:    rng([]normalizedKV{kv("b", "", 2, 4, 2)}, 2, true),
		CreateSorted:       rng([]normalizedKV{b, a}, 2, false),
		ModSorted:          rng([]normalizedKV{b, a}, 2, false),
		VersionSorted:      rng([]normalizedKV{b, a}, 2, false),
		PointCountOnly:     rng([]normalizedKV{}, 1, false),
		PointKeysOnly:      rng([]normalizedKV{kv("b", "", 2, 4, 2)}, 1, false),
		MissingPoint:       rng([]normalizedKV{}, 0, false),
		EmptyInterval:      rng([]normalizedKV{}, 0, false),
		NegativeLimit:      rng([]normalizedKV{a, b}, 2, false),
		NegativeRevision:   rng([]normalizedKV{a, b}, 2, false),
		MinRevision:        rng([]normalizedKV{a, b}, 2, false),
		MinRevisionPoint:   rng([]normalizedKV{b}, 1, false),
		MinRevisionCount:   rng([]normalizedKV{}, 2, false),
		NoOpDeleteRevision: 5,
		RevisionAfterNoOp:  5,
		FutureErrorCode:    "Unknown",
		FutureErrorMessage: "etcdserver: mvcc: required revision is a future revision",
	}
	require.Equal(t, want, etcd)
	require.Equal(t, etcd, runRangeDifferentialScenario(t, compatEndpoint(t), "kubebrain"))
}

func runRangeDifferentialScenario(t *testing.T, endpoint, instance string) rangeDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-range-differential/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	empty, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := empty.Header.Revision

	putA, err := cli.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := cli.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)
	putC, err := cli.Put(ctx, prefix+"c", "vc")
	require.NoError(t, err)
	updateB, err := cli.Put(ctx, prefix+"b", "vb2")
	require.NoError(t, err)
	deleteC, err := cli.Delete(ctx, prefix+"c")
	require.NoError(t, err)

	historical, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(putC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	filtered, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	filteredCountOnly, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithCountOnly(),
	)
	require.NoError(t, err)
	filteredLimited, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMinCreateRev(putA.Header.Revision),
		clientv3.WithLimit(1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	maxModFiltered, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMaxModRev(updateB.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	maxCreateFiltered, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMaxCreateRev(putA.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	limited, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(putC.Header.Revision),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
		clientv3.WithLimit(2),
	)
	require.NoError(t, err)
	valueSortedKeys, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
		clientv3.WithLimit(1),
	)
	require.NoError(t, err)
	createSorted, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
	)
	require.NoError(t, err)
	modSorted, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
	)
	require.NoError(t, err)
	versionSorted, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend),
	)
	require.NoError(t, err)
	keysOnly, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	pointCountOnly, err := cli.Get(ctx, prefix+"b", clientv3.WithCountOnly())
	require.NoError(t, err)
	pointKeysOnly, err := cli.Get(ctx, prefix+"b", clientv3.WithKeysOnly())
	require.NoError(t, err)
	missingPoint, err := cli.Get(ctx, prefix+"missing", clientv3.WithCountOnly(), clientv3.WithLimit(1))
	require.NoError(t, err)
	emptyInterval, err := cli.Get(ctx, prefix+"z", clientv3.WithRange(prefix+"a"))
	require.NoError(t, err)
	negativeLimit, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)

	noOpDelete, err := cli.Delete(ctx, prefix+"z", clientv3.WithRange(prefix+"a"))
	require.NoError(t, err)
	afterNoOp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	negativeRevision, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	minRevision, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(math.MinInt64),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	minRevisionPoint, err := cli.Get(ctx, prefix+"b", clientv3.WithRev(math.MinInt64))
	require.NoError(t, err)
	minRevisionCount, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(math.MinInt64), clientv3.WithCountOnly(),
	)
	require.NoError(t, err)
	_, futureErr := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(afterNoOp.Header.Revision+100))
	require.Error(t, futureErr)
	futureStatus := status.Convert(futureErr)

	return rangeDifferentialResult{
		PutRevisions: []int64{
			putA.Header.Revision - baseRev,
			putB.Header.Revision - baseRev,
			putC.Header.Revision - baseRev,
			updateB.Header.Revision - baseRev,
		},
		DeleteRevision:     deleteC.Header.Revision - baseRev,
		Historical:         normalizeRange(historical, prefix, baseRev),
		Filtered:           normalizeRange(filtered, prefix, baseRev),
		FilteredCountOnly:  normalizeRange(filteredCountOnly, prefix, baseRev),
		FilteredLimited:    normalizeRange(filteredLimited, prefix, baseRev),
		MaxModFiltered:     normalizeRange(maxModFiltered, prefix, baseRev),
		MaxCreateFiltered:  normalizeRange(maxCreateFiltered, prefix, baseRev),
		Limited:            normalizeRange(limited, prefix, baseRev),
		KeysOnly:           normalizeRange(keysOnly, prefix, baseRev),
		ValueSortedKeys:    normalizeRange(valueSortedKeys, prefix, baseRev),
		CreateSorted:       normalizeRange(createSorted, prefix, baseRev),
		ModSorted:          normalizeRange(modSorted, prefix, baseRev),
		VersionSorted:      normalizeRange(versionSorted, prefix, baseRev),
		PointCountOnly:     normalizeRange(pointCountOnly, prefix, baseRev),
		PointKeysOnly:      normalizeRange(pointKeysOnly, prefix, baseRev),
		MissingPoint:       normalizeRange(missingPoint, prefix, baseRev),
		EmptyInterval:      normalizeRange(emptyInterval, prefix, baseRev),
		NegativeLimit:      normalizeRange(negativeLimit, prefix, baseRev),
		NegativeRevision:   normalizeRange(negativeRevision, prefix, baseRev),
		MinRevision:        normalizeRange(minRevision, prefix, baseRev),
		MinRevisionPoint:   normalizeRange(minRevisionPoint, prefix, baseRev),
		MinRevisionCount:   normalizeRange(minRevisionCount, prefix, baseRev),
		NoOpDeleteRevision: noOpDelete.Header.Revision - baseRev,
		NoOpDeleteCount:    noOpDelete.Deleted,
		RevisionAfterNoOp:  afterNoOp.Header.Revision - baseRev,
		FutureErrorCode:    futureStatus.Code().String(),
		FutureErrorMessage: futureStatus.Message(),
	}
}

func normalizeRange(resp *clientv3.GetResponse, prefix string, baseRev int64) normalizedRange {
	return normalizedRange{
		HeaderRev: resp.Header.Revision - baseRev,
		KVs:       normalizeKVs(resp.Kvs, prefix, baseRev),
		Count:     resp.Count,
		More:      resp.More,
	}
}
