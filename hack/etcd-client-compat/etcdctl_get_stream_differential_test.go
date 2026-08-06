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

// TestEtcdctlGetStreamDifferentialAgainstReferenceEtcd pins upstream etcd
// 695442b44. The official etcdctl --stream flag must drive RangeStream while
// preserving get's simple key/value output.
func TestEtcdctlGetStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	etcdctl := os.Getenv("ETCDCTL_BIN")
	if etcdctl == "" {
		etcdctl = "/root/etcd/bin/etcdctl"
	}
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			runEtcdctlGetStreamScenario(t, etcdctl, endpoint, name)
		})
	}
}

func runEtcdctlGetStreamScenario(t *testing.T, etcdctl, endpoint, name string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("a3751/%s/get-stream/%d/", name, time.Now().UnixNano())
	seed := []struct {
		key   string
		value string
	}{
		{key: prefix + "b", value: "value b"},
		{key: prefix + "a", value: "value a"},
		{key: prefix + "c", value: "value c"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	for _, kv := range seed {
		_, err := client.Put(ctx, kv.key, kv.value)
		require.NoError(t, err)
	}

	output, err := runCompatCommandContext(t, ctx, etcdctl, []string{
		"--endpoints=" + endpoint,
		"get", prefix, "--prefix", "--stream",
	}, nil)
	require.NoError(t, err, string(output))
	actual := stripEtcdctlEnvironmentWarnings(string(output))
	require.Equal(t, strings.Join([]string{
		prefix + "a", "value a",
		prefix + "b", "value b",
		prefix + "c", "value c",
		"",
	}, "\n"), actual)

	response, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, response.Kvs, 3)
}

func stripEtcdctlEnvironmentWarnings(output string) string {
	lines := strings.Split(output, "\n")
	filtered := lines[:0]
	for _, line := range lines {
		if strings.Contains(line, `"msg":"unrecognized environment variable"`) {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
}
