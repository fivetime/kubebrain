package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type txnLeaseRevokeRaceOutcome struct {
	Rounds                int
	TxnSuccesses          int
	TxnLeaseNotFound      int
	OtherTxnErrors        int
	RevokeErrors          int
	AllStatesLinearizable bool
	OldLeasesGone         bool
	NewLeaseStateMatches  bool
	FinalKeysDeleted      bool
}

func TestTxnLeaseRevokeRaceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	const rounds = 32
	referenceOutcome := runTxnLeaseRevokeRaceScenario(t, reference, "etcd", rounds)
	t.Logf("reference outcome: %+v", referenceOutcome)
	requireTxnLeaseRevokeRaceValid(t, referenceOutcome)
	kubeBrainOutcome := runTxnLeaseRevokeRaceScenario(t, compatEndpoint(t), "kubebrain", rounds)
	t.Logf("KubeBrain outcome: %+v", kubeBrainOutcome)
	requireTxnLeaseRevokeRaceValid(t, kubeBrainOutcome)
}

func requireTxnLeaseRevokeRaceValid(t *testing.T, outcome txnLeaseRevokeRaceOutcome) {
	t.Helper()
	require.Equal(t, outcome.Rounds, outcome.TxnSuccesses+outcome.TxnLeaseNotFound+outcome.OtherTxnErrors)
	require.Zero(t, outcome.OtherTxnErrors)
	require.Zero(t, outcome.RevokeErrors)
	require.True(t, outcome.AllStatesLinearizable)
	require.True(t, outcome.OldLeasesGone)
	require.True(t, outcome.NewLeaseStateMatches)
	require.True(t, outcome.FinalKeysDeleted)
}

func runTxnLeaseRevokeRaceScenario(t *testing.T, endpoint, instance string, rounds int) txnLeaseRevokeRaceOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-lease-revoke-race/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	outcome := txnLeaseRevokeRaceOutcome{
		Rounds: rounds, AllStatesLinearizable: true, OldLeasesGone: true,
		NewLeaseStateMatches: true, FinalKeysDeleted: true,
	}

	for round := 0; round < rounds; round++ {
		leaseA, grantErr := cli.Grant(ctx, 300)
		require.NoError(t, grantErr)
		leaseB, grantErr := cli.Grant(ctx, 300)
		require.NoError(t, grantErr)
		keyPrefix := fmt.Sprintf("%s%02d/", prefix, round)
		xKey := keyPrefix + "x"
		wKey := keyPrefix + "w"
		seed, putErr := cli.Put(ctx, xKey, "old-x", clientv3.WithLease(leaseA.ID))
		require.NoError(t, putErr)
		baseRevision := seed.Header.Revision

		start := make(chan struct{})
		type txnCallResult struct {
			response *clientv3.TxnResponse
			err      error
		}
		txnResults := make(chan txnCallResult, 1)
		revokeResults := make(chan error, 1)
		var ready sync.WaitGroup
		ready.Add(2)
		go func() {
			ready.Done()
			<-start
			response, callErr := cli.Txn(ctx).Then(
				clientv3.OpPut(xKey, "new-x", clientv3.WithLease(leaseB.ID)),
				clientv3.OpPut(wKey, "new-w", clientv3.WithLease(leaseA.ID)),
			).Commit()
			txnResults <- txnCallResult{response: response, err: callErr}
		}()
		go func() {
			ready.Done()
			<-start
			_, callErr := cli.Revoke(ctx, leaseA.ID)
			revokeResults <- callErr
		}()
		ready.Wait()
		close(start)
		txnResult := <-txnResults
		revokeErr := <-revokeResults
		if revokeErr != nil {
			outcome.RevokeErrors++
		}

		current, getErr := cli.Get(ctx, keyPrefix, clientv3.WithPrefix())
		require.NoError(t, getErr)
		oldTTL, ttlErr := cli.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		newTTL, ttlErr := cli.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		outcome.OldLeasesGone = outcome.OldLeasesGone && oldTTL.TTL == -1 && len(oldTTL.Keys) == 0

		switch {
		case txnResult.err == nil:
			outcome.TxnSuccesses++
			stateMatches := txnResult.response != nil &&
				current.Header.Revision-baseRevision == 2 &&
				len(current.Kvs) == 1 && string(current.Kvs[0].Key) == xKey &&
				string(current.Kvs[0].Value) == "new-x" && current.Kvs[0].Lease == int64(leaseB.ID) &&
				len(newTTL.Keys) == 1 && string(newTTL.Keys[0]) == xKey
			outcome.AllStatesLinearizable = outcome.AllStatesLinearizable && stateMatches
			outcome.NewLeaseStateMatches = outcome.NewLeaseStateMatches && len(newTTL.Keys) == 1 && string(newTTL.Keys[0]) == xKey
		case errors.Is(txnResult.err, rpctypes.ErrLeaseNotFound):
			outcome.TxnLeaseNotFound++
			stateMatches := txnResult.response == nil &&
				current.Header.Revision-baseRevision == 1 && len(current.Kvs) == 0 && len(newTTL.Keys) == 0
			outcome.AllStatesLinearizable = outcome.AllStatesLinearizable && stateMatches
			outcome.NewLeaseStateMatches = outcome.NewLeaseStateMatches && len(newTTL.Keys) == 0
		default:
			outcome.OtherTxnErrors++
			outcome.AllStatesLinearizable = false
		}

		_, revokeBErr := cli.Revoke(ctx, leaseB.ID)
		require.NoError(t, revokeBErr)
		final, getErr := cli.Get(ctx, keyPrefix, clientv3.WithPrefix())
		require.NoError(t, getErr)
		outcome.FinalKeysDeleted = outcome.FinalKeysDeleted && len(final.Kvs) == 0
	}
	return outcome
}
