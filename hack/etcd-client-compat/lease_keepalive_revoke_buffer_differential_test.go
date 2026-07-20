package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseKeepAliveRevokeBufferOutcome struct {
	InitialResponseValid bool
	BufferedResponsesOK  bool
	KeepAliveClosed      bool
	KeyDeleted           bool
	LeaseMissing         bool
}

func TestLeaseKeepAliveRevokeBufferDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseKeepAliveRevokeBufferOutcome{
		InitialResponseValid: true,
		BufferedResponsesOK:  true,
		KeepAliveClosed:      true,
		KeyDeleted:           true,
		LeaseMissing:         true,
	}
	require.Equal(t, want, runLeaseKeepAliveRevokeBufferScenario(t, reference, "reference"))
	require.Equal(t, want, runLeaseKeepAliveRevokeBufferScenario(t, compatEndpoint(), "kubebrain"))
}

func runLeaseKeepAliveRevokeBufferScenario(
	t *testing.T,
	endpoint, instance string,
) leaseKeepAliveRevokeBufferOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	grant, err := cli.Grant(ctx, 10)
	require.NoError(t, err)
	key := testPrefix(t) + "/keepalive-revoke-buffer/" + instance
	_, err = cli.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	revoked := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !revoked {
			_, _ = cli.Revoke(cleanupCtx, grant.ID)
		}
		_, _ = cli.Delete(cleanupCtx, key)
	})

	keepAlive, err := cli.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initial := <-keepAlive
	outcome := leaseKeepAliveRevokeBufferOutcome{
		InitialResponseValid: initial != nil && initial.ID == grant.ID && initial.TTL > 0,
		BufferedResponsesOK:  true,
	}
	_, err = cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	revoked = true

	closeDeadline := time.NewTimer(5 * time.Second)
	defer closeDeadline.Stop()
	for !outcome.KeepAliveClosed {
		select {
		case response, ok := <-keepAlive:
			if !ok {
				outcome.KeepAliveClosed = true
				break
			}
			// clientv3 may deliver responses already buffered before Revoke
			// returned. Their count is scheduling-dependent, but their lease
			// identity and live TTL must remain well formed.
			if response == nil || response.ID != grant.ID || response.TTL <= 0 {
				outcome.BufferedResponsesOK = false
			}
		case <-closeDeadline.C:
			return outcome
		}
	}

	rangeResp, err := cli.Get(ctx, key)
	require.NoError(t, err)
	outcome.KeyDeleted = len(rangeResp.Kvs) == 0
	ttlResp, err := cli.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	outcome.LeaseMissing = ttlResp.TTL == -1
	return outcome
}
