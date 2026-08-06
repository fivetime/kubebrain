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

// TestEtcdctlSingleQuoteTxnDifferentialAgainstReferenceEtcd pins upstream etcd
// 55988933b. Argify must trim each single-quoted token using that token's own
// length; using the number of command arguments silently truncated both a key
// and value in interactive txn input.
func TestEtcdctlSingleQuoteTxnDifferentialAgainstReferenceEtcd(t *testing.T) {
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
			runEtcdctlSingleQuoteTxnScenario(t, etcdctl, endpoint, name)
		})
	}
}

func runEtcdctlSingleQuoteTxnScenario(t *testing.T, etcdctl, endpoint, name string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	key := fmt.Sprintf("a3750/%s quoted key/%d", name, time.Now().UnixNano())
	value := "a3750 quoted value with spaces"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, key)
	})

	// Empty compare block, one success operation, and an empty failure block.
	input := []byte(fmt.Sprintf("\nput '%s' '%s'\n\n\n", key, value))
	output, err := runCompatCommandInputContext(t, ctx, etcdctl, []string{
		"--endpoints=" + endpoint,
		"txn", "--interactive",
	}, nil, input)
	require.NoError(t, err, string(output))
	require.Contains(t, strings.ToUpper(string(output)), "SUCCESS")

	response, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, key, string(response.Kvs[0].Key))
	require.Equal(t, value, string(response.Kvs[0].Value))
	require.Equal(t, int64(1), response.Kvs[0].Version)
}
