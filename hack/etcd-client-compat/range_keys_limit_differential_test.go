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

type keysLimitRangeOutcome struct {
	Keys        []string
	Count       int64
	More        bool
	ValuesEmpty bool
}

func TestKeysOnlyLimitedRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := []keysLimitRangeOutcome{
		{Keys: []string{"00", "01", "03"}, Count: 11, More: true, ValuesEmpty: true},
		{Keys: []string{"00", "01", "02"}, Count: 8, More: true, ValuesEmpty: true},
		{
			Keys:  []string{"00", "01", "03", "04", "05", "06", "07", "08", "09", "10", "11"},
			Count: 11, ValuesEmpty: true,
		},
	}
	referenceOutcome := runKeysOnlyLimitedRangeScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runKeysOnlyLimitedRangeScenario(t, compatEndpoint(), "kubebrain"))
}

func runKeysOnlyLimitedRangeScenario(t *testing.T, endpoint, name string) []keysLimitRangeOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-keys-limit/%s-%d/", name, time.Now().UnixNano())
	for i := 0; i < 8; i++ {
		_, err = client.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), strings.Repeat("value", 100))
		require.NoError(t, err)
	}
	historical, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	require.NoError(t, err)
	for i := 8; i < 12; i++ {
		_, err = client.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), strings.Repeat("later", 100))
		require.NoError(t, err)
	}
	_, err = client.Delete(ctx, prefix+"02")
	require.NoError(t, err)

	run := func(options ...clientv3.OpOption) keysLimitRangeOutcome {
		response, getErr := client.Get(ctx, prefix, options...)
		require.NoError(t, getErr)
		outcome := keysLimitRangeOutcome{
			Count:       response.Count,
			More:        response.More,
			ValuesEmpty: true,
		}
		for _, kv := range response.Kvs {
			outcome.Keys = append(outcome.Keys, strings.TrimPrefix(string(kv.Key), prefix))
			outcome.ValuesEmpty = outcome.ValuesEmpty && len(kv.Value) == 0
		}
		return outcome
	}

	return []keysLimitRangeOutcome{
		run(clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(3)),
		run(clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(3),
			clientv3.WithRev(historical.Header.Revision)),
		run(clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(20)),
	}
}
