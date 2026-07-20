package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type tombstoneLimitRangeOutcome struct {
	Stage       string
	Keys        []string
	Count       int64
	More        bool
	ValuesEmpty bool
}

func TestRangeLimitAcrossTombstonesDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := []tombstoneLimitRangeOutcome{
		{Stage: "before-deletes", Keys: []string{"a", "b"}, Count: 4, More: true, ValuesEmpty: true},
		{Stage: "after-deletes", Keys: []string{"a"}, Count: 2, More: true, ValuesEmpty: true},
		{Stage: "after-recreate", Keys: []string{"a", "b"}, Count: 4, More: true, ValuesEmpty: true},
	}
	referenceOutcome := runRangeLimitAcrossTombstonesScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runRangeLimitAcrossTombstonesScenario(t, compatEndpoint(), "kubebrain"))
}

func runRangeLimitAcrossTombstonesScenario(t *testing.T, endpoint, name string) []tombstoneLimitRangeOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-range-tombstone/%s-%d/", name, time.Now().UnixNano())
	var beforeDeletes int64
	for _, suffix := range []string{"a", "b", "c", "d"} {
		response, putErr := client.Put(ctx, prefix+suffix, "value-"+suffix)
		require.NoError(t, putErr)
		beforeDeletes = response.Header.Revision
	}
	_, err = client.Delete(ctx, prefix+"b")
	require.NoError(t, err)
	deleted, err := client.Delete(ctx, prefix+"d")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"c", "updated-c")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "recreated-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"e", "value-e")
	require.NoError(t, err)

	run := func(stage string, revision, limit int64) tombstoneLimitRangeOutcome {
		options := []clientv3.OpOption{
			clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(limit),
		}
		if revision != 0 {
			options = append(options, clientv3.WithRev(revision))
		}
		response, getErr := client.Get(ctx, prefix, options...)
		require.NoError(t, getErr)
		outcome := tombstoneLimitRangeOutcome{
			Stage: stage, Count: response.Count, More: response.More, ValuesEmpty: true,
		}
		for _, kv := range response.Kvs {
			outcome.Keys = append(outcome.Keys, strings.TrimPrefix(string(kv.Key), prefix))
			outcome.ValuesEmpty = outcome.ValuesEmpty && len(kv.Value) == 0
		}
		return outcome
	}

	return []tombstoneLimitRangeOutcome{
		run("before-deletes", beforeDeletes, 2),
		run("after-deletes", deleted.Header.Revision, 1),
		run("after-recreate", 0, 2),
	}
}
