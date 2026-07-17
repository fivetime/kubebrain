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
	BoundaryValue   string
	HistoricalCode  string
	HistoricalError string
	RepeatedCode    string
	RepeatedError   string
	OlderCode       string
	OlderError      string
	FutureCode      string
	FutureError     string
	NegativeCode    string
	NegativeError   string
	CurrentValue    string
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
	require.Equal(t,
		runCompactDifferentialScenario(t, reference, "etcd"),
		runCompactDifferentialScenario(t, kubebrain, "kubebrain"),
	)
}

func runCompactDifferentialScenario(t *testing.T, endpoint, instance string) compactDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-compact-differential/%s/%d", instance, time.Now().UnixNano())

	first, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	second, err := cli.Put(ctx, key, "v2")
	require.NoError(t, err)
	third, err := cli.Put(ctx, key+"-tail", "tail")
	require.NoError(t, err)
	compact, err := cli.Compact(ctx, second.Header.Revision)
	require.NoError(t, err)
	require.GreaterOrEqual(t, compact.Header.Revision, second.Header.Revision)
	require.LessOrEqual(t, compact.Header.Revision, third.Header.Revision)

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

	normalizeError := func(err error) (string, string) {
		require.Error(t, err)
		st := status.Convert(err)
		return st.Code().String(), st.Message()
	}
	historicalCode, historicalMessage := normalizeError(historicalErr)
	repeatedCode, repeatedMessage := normalizeError(repeatedErr)
	olderCode, olderMessage := normalizeError(olderErr)
	futureCode, futureMessage := normalizeError(futureErr)
	negativeCode, negativeMessage := normalizeError(negativeErr)
	return compactDifferentialResult{
		BoundaryValue:   string(boundary.Kvs[0].Value),
		HistoricalCode:  historicalCode,
		HistoricalError: historicalMessage,
		RepeatedCode:    repeatedCode,
		RepeatedError:   repeatedMessage,
		OlderCode:       olderCode,
		OlderError:      olderMessage,
		FutureCode:      futureCode,
		FutureError:     futureMessage,
		NegativeCode:    negativeCode,
		NegativeError:   negativeMessage,
		CurrentValue:    string(current.Kvs[0].Value),
	}
}
