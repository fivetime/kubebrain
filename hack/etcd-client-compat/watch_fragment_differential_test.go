package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type watchFragmentClientOutcome struct {
	ResponseEventCounts []int
	HeaderRevisionGaps  []int64
	HeaderIdentitySet   bool
	CreatedFlags        []bool
	CanceledFlags       []bool
	CompactRevisionSet  []bool
	ErrorsAbsent        bool
	Types               []mvccpb.Event_EventType
	KeysOrdered         bool
	ValuesMatch         bool
	CreateRevisionGaps  []int64
	ModRevisionGaps     []int64
	Versions            []int64
	Leases              []int64
	PrevKVAbsent        bool
	KVObserved          bool
}

type watchUnfragmentedLimitOutcome struct {
	EventCount              int
	Code                    codes.Code
	ReceivedMessageTooLarge bool
	HeaderPresent           bool
	HeaderZero              bool
	Created                 bool
	Canceled                bool
	CompactRevisionSet      bool
}

type watchUnrestrictedMatrixOutcome struct {
	FragmentRequested bool
	Response          watchFragmentClientOutcome
}

func TestWatchFragmentDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := expectedWatchFragmentClientOutcome()
	referenceOutcome := runWatchFragmentClientScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome,
		runWatchFragmentClientScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchUnfragmentedReceiveLimitDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := watchUnfragmentedLimitOutcome{
		Code: codes.ResourceExhausted, ReceivedMessageTooLarge: true,
		HeaderPresent: true, HeaderZero: true, Canceled: true,
	}
	referenceOutcome := runWatchUnfragmentedLimitScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome,
		runWatchUnfragmentedLimitScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestWatchUnrestrictedReceiveMatrixDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run watch fragment differential tests")
	}

	want := []watchUnrestrictedMatrixOutcome{
		{FragmentRequested: false, Response: expectedWatchFragmentClientOutcome()},
		{FragmentRequested: true, Response: expectedWatchFragmentClientOutcome()},
	}
	referenceOutcomes := runWatchUnrestrictedReceiveMatrix(t, reference, "etcd")
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes,
		runWatchUnrestrictedReceiveMatrix(t, compatEndpoint(t), "kubebrain"))
}

func expectedWatchFragmentClientOutcome() watchFragmentClientOutcome {
	outcome := watchFragmentClientOutcome{
		ResponseEventCounts: []int{10}, HeaderRevisionGaps: []int64{10}, HeaderIdentitySet: true,
		CreatedFlags: []bool{false}, CanceledFlags: []bool{false}, CompactRevisionSet: []bool{false},
		ErrorsAbsent: true, KeysOrdered: true, ValuesMatch: true, PrevKVAbsent: true, KVObserved: true,
	}
	for revisionGap := int64(1); revisionGap <= 10; revisionGap++ {
		outcome.Types = append(outcome.Types, mvccpb.PUT)
		outcome.CreateRevisionGaps = append(outcome.CreateRevisionGaps, revisionGap)
		outcome.ModRevisionGaps = append(outcome.ModRevisionGaps, revisionGap)
		outcome.Versions = append(outcome.Versions, 1)
		outcome.Leases = append(outcome.Leases, 0)
	}
	return outcome
}

func runWatchFragmentClientScenario(t *testing.T, endpoint, instance string) watchFragmentClientOutcome {
	return runWatchSuccessfulLargeScenario(
		t, endpoint, instance, "/dbaas-watch-fragment/", true, 1536*1024,
	)
}

func runWatchSuccessfulLargeScenario(
	t *testing.T,
	endpoint, instance, prefixRoot string,
	fragment bool,
	clientMaxRecv int,
) watchFragmentClientOutcome {
	t.Helper()
	const (
		eventCount = 10
		valueBytes = 1024 * 1024
	)
	writer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	watcherConfig := clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second}
	if clientMaxRecv != 0 {
		watcherConfig.MaxCallRecvMsgSize = clientMaxRecv
	}
	watcher, err := clientv3.New(watcherConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, watcher.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("%s%s/%d/", prefixRoot, instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = writer.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	base, err := writer.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.NotNil(t, base.Header)
	expectedValue := strings.Repeat("x", valueBytes)
	for index := 0; index < eventCount; index++ {
		_, err = writer.Put(ctx, fmt.Sprintf("%s%d", prefix, index), expectedValue)
		require.NoError(t, err)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer watchCancel()
	watchOptions := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(base.Header.Revision + 1)}
	if fragment {
		watchOptions = append(watchOptions, clientv3.WithFragment())
	}
	responses := watcher.Watch(watchCtx, prefix, watchOptions...)
	outcome := watchFragmentClientOutcome{
		HeaderIdentitySet: true, ErrorsAbsent: true, KeysOrdered: true,
		ValuesMatch: true, PrevKVAbsent: true, KVObserved: true,
	}
	events := 0
	for events < eventCount {
		select {
		case response, ok := <-responses:
			require.True(t, ok)
			require.NoError(t, response.Err())
			outcome.ResponseEventCounts = append(outcome.ResponseEventCounts, len(response.Events))
			outcome.HeaderRevisionGaps = append(outcome.HeaderRevisionGaps,
				response.Header.GetRevision()-base.Header.Revision)
			outcome.HeaderIdentitySet = outcome.HeaderIdentitySet && response.Header != nil &&
				response.Header.ClusterId != 0 && response.Header.MemberId != 0 && response.Header.RaftTerm > 0
			outcome.CreatedFlags = append(outcome.CreatedFlags, response.Created)
			outcome.CanceledFlags = append(outcome.CanceledFlags, response.Canceled)
			outcome.CompactRevisionSet = append(outcome.CompactRevisionSet, response.CompactRevision != 0)
			outcome.ErrorsAbsent = outcome.ErrorsAbsent && response.Err() == nil
			for _, event := range response.Events {
				outcome.Types = append(outcome.Types, event.Type)
				outcome.PrevKVAbsent = outcome.PrevKVAbsent && event.PrevKv == nil
				if event.Kv == nil {
					outcome.KVObserved = false
					events++
					continue
				}
				expectedKey := fmt.Sprintf("%s%d", prefix, events)
				outcome.KeysOrdered = outcome.KeysOrdered && string(event.Kv.Key) == expectedKey
				outcome.ValuesMatch = outcome.ValuesMatch && string(event.Kv.Value) == expectedValue
				outcome.CreateRevisionGaps = append(outcome.CreateRevisionGaps,
					event.Kv.CreateRevision-base.Header.Revision)
				outcome.ModRevisionGaps = append(outcome.ModRevisionGaps,
					event.Kv.ModRevision-base.Header.Revision)
				outcome.Versions = append(outcome.Versions, event.Kv.Version)
				outcome.Leases = append(outcome.Leases, event.Kv.Lease)
				events++
			}
		case <-watchCtx.Done():
			require.NoError(t, watchCtx.Err())
		}
	}
	require.Equal(t, eventCount, events)
	return outcome
}

func runWatchUnfragmentedLimitScenario(t *testing.T, endpoint, instance string) watchUnfragmentedLimitOutcome {
	t.Helper()
	const (
		eventCount    = 10
		valueBytes    = 1024 * 1024
		clientMaxRecv = 1536 * 1024
	)
	writer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	watcher, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
		MaxCallRecvMsgSize: clientMaxRecv,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, watcher.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-watch-unfragmented-limit/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = writer.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	base, err := writer.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.NotNil(t, base.Header)
	expectedValue := strings.Repeat("x", valueBytes)
	for index := 0; index < eventCount; index++ {
		_, err = writer.Put(ctx, fmt.Sprintf("%s%d", prefix, index), expectedValue)
		require.NoError(t, err)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer watchCancel()
	responses := watcher.Watch(
		watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(base.Header.Revision+1),
	)
	select {
	case response, ok := <-responses:
		require.True(t, ok)
		responseErr := response.Err()
		return watchUnfragmentedLimitOutcome{
			EventCount:              len(response.Events),
			Code:                    status.Code(responseErr),
			ReceivedMessageTooLarge: responseErr != nil && strings.Contains(responseErr.Error(), "received message larger than max"),
			HeaderPresent:           response.Header != nil,
			HeaderZero: response.Header != nil && response.Header.ClusterId == 0 &&
				response.Header.MemberId == 0 && response.Header.Revision == 0 && response.Header.RaftTerm == 0,
			Created:            response.Created,
			Canceled:           response.Canceled,
			CompactRevisionSet: response.CompactRevision != 0,
		}
	case <-watchCtx.Done():
		require.NoError(t, watchCtx.Err())
		return watchUnfragmentedLimitOutcome{}
	}
}

func runWatchUnrestrictedReceiveMatrix(t *testing.T, endpoint, instance string) []watchUnrestrictedMatrixOutcome {
	t.Helper()
	outcomes := make([]watchUnrestrictedMatrixOutcome, 0, 2)
	for _, fragment := range []bool{false, true} {
		outcomes = append(outcomes, watchUnrestrictedMatrixOutcome{
			FragmentRequested: fragment,
			Response: runWatchSuccessfulLargeScenario(
				t, endpoint, instance, "/dbaas-watch-unrestricted/", fragment, 0,
			),
		})
	}
	return outcomes
}
