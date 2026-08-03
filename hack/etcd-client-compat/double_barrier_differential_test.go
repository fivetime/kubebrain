package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
)

type doubleBarrierOutcome struct {
	FirstTwoEnterBlocked   bool
	Entered                int
	ExtraClientRejected    bool
	FirstTwoLeaveBlocked   bool
	Left                   int
	NormalWaitersRemoved   bool
	FailoverSurvivorsLeft  int
	FailoverWaitersRemoved bool
}

func TestDoubleBarrierDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run double barrier differential tests")
	}

	referenceOutcome := runDoubleBarrierScenario(t, reference, "etcd")
	kubeBrainOutcome := runDoubleBarrierScenario(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, referenceOutcome, kubeBrainOutcome)
	require.Equal(t, doubleBarrierOutcome{
		FirstTwoEnterBlocked:   true,
		Entered:                3,
		ExtraClientRejected:    true,
		FirstTwoLeaveBlocked:   true,
		Left:                   3,
		NormalWaitersRemoved:   true,
		FailoverSurvivorsLeft:  2,
		FailoverWaitersRemoved: true,
	}, kubeBrainOutcome)
}

func runDoubleBarrierScenario(t *testing.T, endpoint, instance string) doubleBarrierOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Context: ctx,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	base := fmt.Sprintf("/dbaas-double-barrier/%s/%d/", instance, time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, base, clientv3.WithPrefix())
	}()

	newSession := func() *concurrency.Session {
		session, sessionErr := concurrency.NewSession(client, concurrency.WithTTL(10))
		require.NoError(t, sessionErr)
		return session
	}
	closeSession := func(session *concurrency.Session) { _ = session.Close() }

	normalName := base + "normal"
	sessions := []*concurrency.Session{newSession(), newSession(), newSession()}
	defer func() {
		for _, session := range sessions {
			closeSession(session)
		}
	}()
	barriers := make([]*recipe.DoubleBarrier, 0, len(sessions))
	for _, session := range sessions {
		barriers = append(barriers, recipe.NewDoubleBarrier(session, normalName, len(sessions)))
	}

	enterResults := make(chan error, len(barriers))
	go func() { enterResults <- barriers[0].Enter() }()
	go func() { enterResults <- barriers[1].Enter() }()
	require.Eventually(t, func() bool {
		return doubleBarrierWaiterCount(ctx, client, normalName) == 2
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	firstTwoEnterBlocked := len(enterResults) == 0
	go func() { enterResults <- barriers[2].Enter() }()
	entered := collectBarrierResults(t, ctx, enterResults, len(barriers), "enter")

	extraSession := newSession()
	defer closeSession(extraSession)
	extraErr := recipe.NewDoubleBarrier(extraSession, normalName, len(sessions)).Enter()
	extraClientRejected := errors.Is(extraErr, recipe.ErrTooManyClients)

	leaveResults := make(chan error, len(barriers))
	go func() { leaveResults <- barriers[0].Leave() }()
	go func() { leaveResults <- barriers[1].Leave() }()
	time.Sleep(100 * time.Millisecond)
	firstTwoLeaveBlocked := len(leaveResults) == 0
	go func() { leaveResults <- barriers[2].Leave() }()
	left := collectBarrierResults(t, ctx, leaveResults, len(barriers), "leave")
	normalWaitersRemoved := doubleBarrierWaiterCount(ctx, client, normalName) == 0

	failoverName := base + "failover"
	failoverSessions := []*concurrency.Session{newSession(), newSession(), newSession()}
	defer func() {
		for index := 1; index < len(failoverSessions); index++ {
			closeSession(failoverSessions[index])
		}
	}()
	failoverBarriers := make([]*recipe.DoubleBarrier, 0, len(failoverSessions))
	for _, session := range failoverSessions {
		failoverBarriers = append(failoverBarriers,
			recipe.NewDoubleBarrier(session, failoverName, len(failoverSessions)))
	}
	failoverEnter := make(chan error, len(failoverBarriers))
	go func() { failoverEnter <- failoverBarriers[0].Enter() }()
	require.Eventually(t, func() bool {
		return doubleBarrierWaiterCount(ctx, client, failoverName) == 1
	}, 5*time.Second, 20*time.Millisecond)
	go func() { failoverEnter <- failoverBarriers[1].Enter() }()
	require.Eventually(t, func() bool {
		return doubleBarrierWaiterCount(ctx, client, failoverName) == 2
	}, 5*time.Second, 20*time.Millisecond)
	go func() { failoverEnter <- failoverBarriers[2].Enter() }()
	require.Equal(t, len(failoverBarriers),
		collectBarrierResults(t, ctx, failoverEnter, len(failoverBarriers), "failover enter"))

	failoverLeave := make(chan error, 2)
	go func() { failoverLeave <- failoverBarriers[1].Leave() }()
	go func() { failoverLeave <- failoverBarriers[2].Leave() }()
	require.Eventually(t, func() bool {
		return doubleBarrierWaiterCount(ctx, client, failoverName) == 1
	}, 5*time.Second, 20*time.Millisecond)
	closeSession(failoverSessions[0])
	failoverSurvivorsLeft := collectBarrierResults(t, ctx, failoverLeave, 2, "failover leave")
	failoverWaitersRemoved := doubleBarrierWaiterCount(ctx, client, failoverName) == 0

	return doubleBarrierOutcome{
		FirstTwoEnterBlocked:   firstTwoEnterBlocked,
		Entered:                entered,
		ExtraClientRejected:    extraClientRejected,
		FirstTwoLeaveBlocked:   firstTwoLeaveBlocked,
		Left:                   left,
		NormalWaitersRemoved:   normalWaitersRemoved,
		FailoverSurvivorsLeft:  failoverSurvivorsLeft,
		FailoverWaitersRemoved: failoverWaitersRemoved,
	}
}

func doubleBarrierWaiterCount(ctx context.Context, client *clientv3.Client, name string) int {
	response, err := client.Get(ctx, name+"/waiters", clientv3.WithPrefix())
	if err != nil {
		return -1
	}
	return len(response.Kvs)
}

func collectBarrierResults(
	t *testing.T,
	ctx context.Context,
	results <-chan error,
	count int,
	phase string,
) int {
	t.Helper()
	completed := 0
	for range count {
		select {
		case err := <-results:
			require.NoError(t, err, phase)
			completed++
		case <-ctx.Done():
			t.Fatalf("double barrier %s did not complete: %v", phase, ctx.Err())
		}
	}
	return completed
}
