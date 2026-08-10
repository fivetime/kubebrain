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

// TestEtcdctlHexLeaseTxnDifferentialAgainstReferenceEtcd pins upstream etcd
// b35f739fa. Lease IDs printed by etcdctl are hexadecimal, and interactive txn
// comparisons must accept that representation before sending the int64 LEASE
// compare over the wire.
func TestEtcdctlHexLeaseTxnDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	etcdctl := os.Getenv("ETCDCTL_BIN")
	if etcdctl == "" {
		etcdctl = "/root/etcd/bin/etcdctl"
	}
	requireReferenceEtcdProvenance(t, etcdctl)
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			runEtcdctlHexLeaseTxnScenario(t, etcdctl, endpoint, name)
		})
	}
}

func runEtcdctlHexLeaseTxnScenario(t *testing.T, etcdctl, endpoint, name string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("a4272/%s/hex-lease-txn/%d/", name, time.Now().UnixNano())
	leasedKey := prefix + "leased"
	resultKey := prefix + "result"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	grant, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = client.Put(ctx, leasedKey, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	// etcdctl lease grant/list prints IDs in hexadecimal. The fixed parser turns
	// this text into the int64 required by clientv3 before constructing Compare.
	hexLeaseID := fmt.Sprintf("%x", int64(grant.ID))
	input := []byte(fmt.Sprintf(
		"lease(\"%s\") = \"%s\"\n\nput %s success\n\nput %s failure\n\n",
		leasedKey, hexLeaseID, resultKey, resultKey,
	))
	output, err := runCompatCommandInputContext(t, ctx, etcdctl, []string{
		"--endpoints=" + endpoint,
		"txn", "--interactive",
	}, nil, input)
	require.NoError(t, err, string(output))
	require.Contains(t, strings.ToUpper(string(output)), "SUCCESS")

	response, err := client.Get(ctx, resultKey)
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, "success", string(response.Kvs[0].Value))
}
