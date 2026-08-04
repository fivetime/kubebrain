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

type compactDifferentialResult struct {
	CompactHeaderGap  int64
	BoundaryValue     string
	BoundaryHeaderGap int64
	HistoricalCode    string
	HistoricalError   string
	RepeatedCode      string
	RepeatedError     string
	OlderCode         string
	OlderError        string
	FutureCode        string
	FutureError       string
	NegativeCode      string
	NegativeError     string
	CurrentValue      string
	CurrentHeaderGap  int64
}

func TestCompactDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if kubebrain == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for differential tests")
	}
	referenceOutcome := runCompactDifferentialScenario(t, reference, "etcd")
	want := compactDifferentialResult{
		BoundaryValue:   "v2",
		HistoricalCode:  "Unknown",
		HistoricalError: "etcdserver: mvcc: required revision has been compacted",
		RepeatedCode:    "Unknown",
		RepeatedError:   "etcdserver: mvcc: required revision has been compacted",
		OlderCode:       "Unknown",
		OlderError:      "etcdserver: mvcc: required revision has been compacted",
		FutureCode:      "Unknown",
		FutureError:     "etcdserver: mvcc: required revision is a future revision",
		NegativeCode:    "Unknown",
		NegativeError:   "etcdserver: mvcc: required revision has been compacted",
		CurrentValue:    "v2",
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runCompactDifferentialScenario(t, kubebrain, "kubebrain"))
}

func runCompactDifferentialScenario(t *testing.T, endpoint, instance string) compactDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-compact-differential/%s/%d", instance, time.Now().UnixNano())
	registerPrefixCleanup(t, cli, key)

	first, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	second, err := cli.Put(ctx, key, "v2")
	require.NoError(t, err)
	third, err := cli.Put(ctx, key+"-tail", "tail")
	require.NoError(t, err)
	compact, err := cli.Compact(ctx, second.Header.Revision)
	require.NoError(t, err)

	boundary, err := cli.Get(ctx, key, clientv3.WithRev(second.Header.Revision))
	require.NoError(t, err)
	require.Len(t, boundary.Kvs, 1)
	_, historicalErr := cli.Get(ctx, key, clientv3.WithRev(first.Header.Revision))
	_, repeatedErr := cli.Compact(ctx, second.Header.Revision)
	_, olderErr := cli.Compact(ctx, first.Header.Revision)
	_, futureErr := cli.Compact(ctx, third.Header.Revision+1000)
	_, negativeErr := cli.Compact(ctx, -1)
	current, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)

	normalizeError := func(name string, err error) (string, string) {
		require.Errorf(t, err, "%s must fail after compaction", name)
		st := status.Convert(err)
		return st.Code().String(), st.Message()
	}
	historicalCode, historicalMessage := normalizeError("historical range", historicalErr)
	repeatedCode, repeatedMessage := normalizeError("repeated compact", repeatedErr)
	olderCode, olderMessage := normalizeError("older compact", olderErr)
	futureCode, futureMessage := normalizeError("future compact", futureErr)
	negativeCode, negativeMessage := normalizeError("negative compact", negativeErr)
	return compactDifferentialResult{
		CompactHeaderGap:  compact.Header.Revision - third.Header.Revision,
		BoundaryValue:     string(boundary.Kvs[0].Value),
		BoundaryHeaderGap: boundary.Header.Revision - third.Header.Revision,
		HistoricalCode:    historicalCode,
		HistoricalError:   historicalMessage,
		RepeatedCode:      repeatedCode,
		RepeatedError:     repeatedMessage,
		OlderCode:         olderCode,
		OlderError:        olderMessage,
		FutureCode:        futureCode,
		FutureError:       futureMessage,
		NegativeCode:      negativeCode,
		NegativeError:     negativeMessage,
		CurrentValue:      string(current.Kvs[0].Value),
		CurrentHeaderGap:  current.Header.Revision - third.Header.Revision,
	}
}
