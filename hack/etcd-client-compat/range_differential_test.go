package compat

import (
	"context"
	"fmt"
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
	Limited            normalizedRange
	KeysOnly           normalizedRange
	PointCountOnly     normalizedRange
	PointKeysOnly      normalizedRange
	NegativeRevision   normalizedRange
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

	kubebrain := runRangeDifferentialScenario(t, compatEndpoint(), "kubebrain")
	etcd := runRangeDifferentialScenario(t, reference, "etcd")
	require.Equal(t, etcd, kubebrain)
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
	limited, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(putC.Header.Revision),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
		clientv3.WithLimit(2),
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
		Limited:            normalizeRange(limited, prefix, baseRev),
		KeysOnly:           normalizeRange(keysOnly, prefix, baseRev),
		PointCountOnly:     normalizeRange(pointCountOnly, prefix, baseRev),
		PointKeysOnly:      normalizeRange(pointKeysOnly, prefix, baseRev),
		NegativeRevision:   normalizeRange(negativeRevision, prefix, baseRev),
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
