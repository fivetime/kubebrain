package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type deleteDifferentialResult struct {
	PutRevisions       []int64
	DeleteRevision     int64
	Deleted            int64
	PrevKVs            []normalizedKV
	HistoricalBefore   normalizedRange
	CurrentAfter       normalizedRange
	EmptyRangeRevision int64
	EmptyRangeDeleted  int64
	EmptyRangePrevKVs  int
	MissingRevision    int64
	MissingDeleted     int64
	MissingPrevKVs     int
	FinalRevision      int64
}

func TestDeleteRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	kubebrain := runDeleteDifferentialScenario(t, compatEndpoint(), "kubebrain")
	etcd := runDeleteDifferentialScenario(t, reference, "etcd")
	require.Equal(t, etcd, kubebrain)
}

func runDeleteDifferentialScenario(t *testing.T, endpoint, instance string) deleteDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-delete-differential/%s/%d/", instance, time.Now().UnixNano())
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

	deleted, err := cli.Delete(ctx, prefix+"a",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	historical, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(deleted.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	current, err := cli.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)

	emptyRange, err := cli.Delete(ctx, prefix+"c",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	missing, err := cli.Delete(ctx, prefix+"missing", clientv3.WithPrevKV())
	require.NoError(t, err)
	final, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	return deleteDifferentialResult{
		PutRevisions: []int64{
			putA.Header.Revision - baseRev,
			putB.Header.Revision - baseRev,
			putC.Header.Revision - baseRev,
			updateB.Header.Revision - baseRev,
		},
		DeleteRevision:     deleted.Header.Revision - baseRev,
		Deleted:            deleted.Deleted,
		PrevKVs:            normalizeKVs(deleted.PrevKvs, prefix, baseRev),
		HistoricalBefore:   normalizeRange(historical, prefix, baseRev),
		CurrentAfter:       normalizeRange(current, prefix, baseRev),
		EmptyRangeRevision: emptyRange.Header.Revision - baseRev,
		EmptyRangeDeleted:  emptyRange.Deleted,
		EmptyRangePrevKVs:  len(emptyRange.PrevKvs),
		MissingRevision:    missing.Header.Revision - baseRev,
		MissingDeleted:     missing.Deleted,
		MissingPrevKVs:     len(missing.PrevKvs),
		FinalRevision:      final.Header.Revision - baseRev,
	}
}
