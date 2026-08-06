package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// TestClientLoggerSyncRaceDifferentialAgainstReferenceEtcd pins upstream etcd
// 67a6ef4a3. Client.WithLogger may run concurrently with Sync, while Sync reads
// MemberList and installs the advertised endpoints. Under -race, a direct read
// of Client.lg races with WithLogger; GetLogger provides the required lock.
func TestClientLoggerSyncRaceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			runClientLoggerSyncRaceScenario(t, endpoint)
		})
	}
}

func runClientLoggerSyncRaceScenario(t *testing.T, endpoint string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	const syncers = 4
	const syncsPerWorker = 25
	start := make(chan struct{})
	errs := make(chan error, syncers)
	var workers sync.WaitGroup
	workers.Add(syncers + 1)
	loggerA, loggerB := zap.NewNop(), zap.NewNop()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < syncers*syncsPerWorker*20; i++ {
			if i%2 == 0 {
				client.WithLogger(loggerA)
			} else {
				client.WithLogger(loggerB)
			}
		}
	}()
	for worker := 0; worker < syncers; worker++ {
		go func(worker int) {
			defer workers.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for i := 0; i < syncsPerWorker; i++ {
				if syncErr := client.Sync(ctx); syncErr != nil {
					errs <- fmt.Errorf("worker %d sync %d: %w", worker, i, syncErr)
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	close(errs)
	for syncErr := range errs {
		require.NoError(t, syncErr)
	}
	require.NotEmpty(t, client.Endpoints())
}
