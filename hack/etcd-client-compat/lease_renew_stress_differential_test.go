package compat

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseRenewStressOutcome struct {
	Completed        int64
	ZeroKeepAliveTTL int64
	LeaseNotFound    int64
	OtherErrors      int64
}

func TestLeaseRenewStressDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runLeaseRenewStressScenario(t, reference),
		runLeaseRenewStressScenario(t, compatEndpoint()),
	)
}

func runLeaseRenewStressScenario(t *testing.T, endpoint string) leaseRenewStressOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	const workers = 32
	const rounds = 8
	var completed, zeroTTL, notFound, otherErrors atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range rounds {
				grant, grantErr := client.Grant(ctx, 60)
				if grantErr != nil {
					classifyLeaseStressError(grantErr, &notFound, &otherErrors)
					continue
				}
				keepAlive, keepAliveErr := client.KeepAliveOnce(ctx, grant.ID)
				if keepAliveErr != nil {
					classifyLeaseStressError(keepAliveErr, &notFound, &otherErrors)
					continue
				}
				if keepAlive.TTL == 0 {
					zeroTTL.Add(1)
				}
				if _, ttlErr := client.TimeToLive(ctx, grant.ID); ttlErr != nil {
					classifyLeaseStressError(ttlErr, &notFound, &otherErrors)
					continue
				}
				if _, revokeErr := client.Revoke(ctx, grant.ID); revokeErr != nil {
					classifyLeaseStressError(revokeErr, &notFound, &otherErrors)
					continue
				}
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, ctx.Err())

	return leaseRenewStressOutcome{
		Completed:        completed.Load(),
		ZeroKeepAliveTTL: zeroTTL.Load(),
		LeaseNotFound:    notFound.Load(),
		OtherErrors:      otherErrors.Load(),
	}
}

func classifyLeaseStressError(err error, notFound, other *atomic.Int64) {
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		notFound.Add(1)
		return
	}
	other.Add(1)
}
