package main

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type putResolutionClient interface {
	Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

// resolveProbePut shares one deadline across writes and reconciliation reads.
// An RPC error can leave a write uncertain; a malformed successful response is
// instead a compatibility failure and must not be hidden by a later read.
func resolveProbePut(ctx context.Context, client putResolutionClient, deadline time.Time, key, value string, clusterID uint64, lastRevision int64, progress *probeProgress) (int64, error) {
	for {
		opCtx, cancel := context.WithDeadline(ctx, deadline)
		started := time.Now()
		response, putErr := client.Put(opCtx, key, value)
		progress.recordPutCall(time.Since(started), putErr)
		cancel()
		if putErr == nil {
			revision, err := validatePutResponse(response, clusterID, lastRevision)
			if err != nil {
				return 0, fmt.Errorf("invalid successful put response: %w", err)
			}
			return revision, nil
		}
		opCtx, cancel = context.WithDeadline(ctx, deadline)
		started = time.Now()
		observed, getErr := client.Get(opCtx, key)
		progress.recordConfirmCall(time.Since(started), getErr)
		cancel()
		if getErr == nil {
			revision, err := validateObservedPut(observed, clusterID, lastRevision, key, value)
			getErr = err
			if err == nil {
				return revision, nil
			}
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("put unresolved before deadline: put=%v get=%v", putErr, getErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
