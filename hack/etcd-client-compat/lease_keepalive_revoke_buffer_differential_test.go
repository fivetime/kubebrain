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
	InitialResponseValid  bool
	InitialHeaderGap      int64
	BufferedResponsesOK   bool
	BufferedHeadersOK     bool
	KeepAliveClosed       bool
	GrantRevisionGap      int64
	PutRevisionGap        int64
	RevokeRevisionGap     int64
	FinalRangeRevisionGap int64
	MissingTTLRevisionGap int64
	KeyDeleted            bool
	SeedPreserved         bool
	LeaseMissing          bool
}

func TestLeaseKeepAliveRevokeBufferDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseKeepAliveRevokeBufferOutcome{
		InitialResponseValid:  true,
		InitialHeaderGap:      1,
		BufferedResponsesOK:   true,
		BufferedHeadersOK:     true,
		KeepAliveClosed:       true,
		PutRevisionGap:        1,
		RevokeRevisionGap:     2,
		FinalRangeRevisionGap: 2,
		MissingTTLRevisionGap: 2,
		KeyDeleted:            true,
		SeedPreserved:         true,
		LeaseMissing:          true,
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
	prefix := testPrefix(t) + "/keepalive-revoke-buffer/" + instance
	seedKey := prefix + "/seed"
	seed, err := cli.Put(ctx, seedKey, "seed")
	require.NoError(t, err)
	grant, err := cli.Grant(ctx, 10)
	require.NoError(t, err)
	require.NotNil(t, grant.ResponseHeader)
	require.Equal(t, seed.Header.Revision, grant.ResponseHeader.Revision)
	key := prefix + "/key"
	put, err := cli.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	require.Equal(t, seed.Header.Revision+1, put.Header.Revision)
	revoked := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !revoked {
			_, _ = cli.Revoke(cleanupCtx, grant.ID)
		}
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	keepAlive, err := cli.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initial := <-keepAlive
	outcome := leaseKeepAliveRevokeBufferOutcome{
		InitialResponseValid: initial != nil && initial.ID == grant.ID && initial.TTL > 0,
		BufferedResponsesOK:  true,
		BufferedHeadersOK:    true,
		GrantRevisionGap:     grant.ResponseHeader.Revision - seed.Header.Revision,
		PutRevisionGap:       put.Header.Revision - seed.Header.Revision,
	}
	require.NotNil(t, initial)
	require.NotNil(t, initial.ResponseHeader)
	outcome.InitialHeaderGap = initial.ResponseHeader.Revision - seed.Header.Revision
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	require.NotNil(t, revoke.Header)
	outcome.RevokeRevisionGap = revoke.Header.Revision - seed.Header.Revision
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
			if response == nil || response.ResponseHeader == nil || response.ResponseHeader.Revision != put.Header.Revision {
				outcome.BufferedHeadersOK = false
			}
		case <-closeDeadline.C:
			return outcome
		}
	}

	rangeResp, err := cli.Get(ctx, key)
	require.NoError(t, err)
	outcome.KeyDeleted = len(rangeResp.Kvs) == 0
	outcome.FinalRangeRevisionGap = rangeResp.Header.Revision - seed.Header.Revision
	seedResp, err := cli.Get(ctx, seedKey)
	require.NoError(t, err)
	outcome.SeedPreserved = len(seedResp.Kvs) == 1 && string(seedResp.Kvs[0].Value) == "seed"
	require.Equal(t, rangeResp.Header.Revision, seedResp.Header.Revision)
	ttlResp, err := cli.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	outcome.LeaseMissing = ttlResp.TTL == -1
	outcome.MissingTTLRevisionGap = ttlResp.ResponseHeader.Revision - seed.Header.Revision
	return outcome
}
