package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseOpKind uint8

const (
	leaseRead leaseOpKind = iota
	leasePut
	leaseKeepAlive
	leaseRevoke
	leaseGrant
	leaseTimeToLive
)

type leaseState struct {
	alive   bool
	present bool
	value   int
}

type leaseInput struct {
	kind  leaseOpKind
	value int
}

type leaseOutput struct {
	present  bool
	value    int
	failed   bool
	notFound bool
}

type leaseGenerationState struct {
	generation int
	alive      bool
	present    bool
	value      int
}

type leaseGenerationOutput struct {
	present      bool
	value        int
	alive        bool
	failed       bool
	notFound     bool
	alreadyAlive bool
}

var leaseGenerationModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{leaseGenerationState{}} },
	Step: func(state, input, output interface{}) []interface{} {
		current := state.(leaseGenerationState)
		in := input.(leaseInput)
		out := output.(leaseGenerationOutput)
		if out.alreadyAlive {
			if in.kind != leaseGrant || !current.alive {
				return nil
			}
			return []interface{}{current}
		}
		if out.notFound {
			if current.alive || (in.kind != leasePut && in.kind != leaseRevoke) {
				return nil
			}
			return []interface{}{current}
		}
		if out.failed {
			switch in.kind {
			case leaseRead, leaseTimeToLive:
				return []interface{}{current}
			case leaseGrant:
				if current.alive {
					return []interface{}{current}
				}
				granted := current
				granted.generation++
				granted.alive = true
				return []interface{}{current, granted}
			case leasePut:
				if !current.alive {
					return []interface{}{current}
				}
				committed := current
				committed.present, committed.value = true, in.value
				return []interface{}{current, committed}
			case leaseRevoke:
				if !current.alive {
					return []interface{}{current}
				}
				revoked := current
				revoked.alive, revoked.present, revoked.value = false, false, 0
				return []interface{}{current, revoked}
			}
		}
		switch in.kind {
		case leaseGrant:
			if !current.alive {
				current.generation++
				current.alive = true
				return []interface{}{current}
			}
		case leasePut:
			if current.alive {
				current.present, current.value = true, in.value
				return []interface{}{current}
			}
		case leaseRead:
			if out.present == current.present && (!out.present || out.value == current.value) {
				return []interface{}{current}
			}
		case leaseTimeToLive:
			if out.alive == current.alive {
				return []interface{}{current}
			}
		case leaseRevoke:
			if current.alive {
				current.alive, current.present, current.value = false, false, 0
				return []interface{}{current}
			}
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(leaseInput)
		out := output.(leaseGenerationOutput)
		switch {
		case out.alreadyAlive:
			return fmt.Sprintf("%s -> already-alive", describeLeaseInput(in))
		case out.notFound:
			return fmt.Sprintf("%s -> lease-not-found", describeLeaseInput(in))
		case out.failed:
			return fmt.Sprintf("%s -> unknown", describeLeaseInput(in))
		case in.kind == leaseRead:
			return fmt.Sprintf("get() -> {present:%t value:%d}", out.present, out.value)
		case in.kind == leaseTimeToLive:
			return fmt.Sprintf("time-to-live() -> {alive:%t}", out.alive)
		default:
			return fmt.Sprintf("%s -> ok", describeLeaseInput(in))
		}
	},
}).ToModel()

var leaseModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{leaseState{alive: true}} },
	Step: func(state, input, output interface{}) []interface{} {
		current := state.(leaseState)
		in := input.(leaseInput)
		out := output.(leaseOutput)
		if out.notFound {
			if current.alive || in.kind == leaseRead {
				return nil
			}
			return []interface{}{current}
		}
		if out.failed {
			switch in.kind {
			case leaseRead, leaseKeepAlive:
				return []interface{}{current}
			case leasePut:
				if !current.alive {
					return []interface{}{current}
				}
				committed := current
				committed.present, committed.value = true, in.value
				return []interface{}{current, committed}
			case leaseRevoke:
				if !current.alive {
					return []interface{}{current}
				}
				return []interface{}{current, leaseState{}}
			default:
				panic("unknown lease operation")
			}
		}
		switch in.kind {
		case leaseRead:
			if out.present == current.present && (!out.present || out.value == current.value) {
				return []interface{}{current}
			}
		case leasePut:
			if current.alive {
				current.present, current.value = true, in.value
				return []interface{}{current}
			}
		case leaseKeepAlive:
			if current.alive {
				return []interface{}{current}
			}
		case leaseRevoke:
			if current.alive {
				return []interface{}{leaseState{}}
			}
		default:
			panic("unknown lease operation")
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(leaseInput)
		out := output.(leaseOutput)
		if out.notFound {
			return fmt.Sprintf("%s -> lease-not-found", describeLeaseInput(in))
		}
		if out.failed {
			return fmt.Sprintf("%s -> unknown", describeLeaseInput(in))
		}
		if in.kind == leaseRead {
			return fmt.Sprintf("get() -> {present:%t value:%d}", out.present, out.value)
		}
		return fmt.Sprintf("%s -> ok", describeLeaseInput(in))
	},
}).ToModel()

func describeLeaseInput(in leaseInput) string {
	switch in.kind {
	case leaseRead:
		return "get()"
	case leasePut:
		return fmt.Sprintf("put-with-lease(%d)", in.value)
	case leaseKeepAlive:
		return "keepalive-once()"
	case leaseRevoke:
		return "revoke()"
	case leaseGrant:
		return "grant()"
	case leaseTimeToLive:
		return "time-to-live()"
	default:
		return "unknown"
	}
}

func TestLeaseGenerationModelFencesRegrantedLease(t *testing.T) {
	prefix := []porcupine.Operation{
		{ClientId: 0, Input: leaseInput{kind: leaseGrant}, Call: 1, Output: leaseGenerationOutput{}, Return: 2},
		{ClientId: 0, Input: leaseInput{kind: leasePut, value: 1}, Call: 3, Output: leaseGenerationOutput{}, Return: 4},
		{ClientId: 0, Input: leaseInput{kind: leaseRevoke}, Call: 5, Output: leaseGenerationOutput{}, Return: 6},
		{ClientId: 0, Input: leaseInput{kind: leaseGrant}, Call: 7, Output: leaseGenerationOutput{}, Return: 8},
		{ClientId: 0, Input: leaseInput{kind: leasePut, value: 2}, Call: 9, Output: leaseGenerationOutput{}, Return: 10},
	}
	valid := append(append([]porcupine.Operation{}, prefix...),
		porcupine.Operation{ClientId: 1, Input: leaseInput{kind: leaseRead}, Call: 11, Output: leaseGenerationOutput{present: true, value: 2}, Return: 12})
	require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(leaseGenerationModel, valid, time.Second))

	staleDelete := append(append([]porcupine.Operation{}, prefix...),
		porcupine.Operation{ClientId: 1, Input: leaseInput{kind: leaseRead}, Call: 11, Output: leaseGenerationOutput{}, Return: 12})
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseGenerationModel, staleDelete, time.Second))

	staleKey := []porcupine.Operation{
		{ClientId: 0, Input: leaseInput{kind: leaseGrant}, Call: 1, Output: leaseGenerationOutput{}, Return: 2},
		{ClientId: 0, Input: leaseInput{kind: leasePut, value: 1}, Call: 3, Output: leaseGenerationOutput{}, Return: 4},
		{ClientId: 0, Input: leaseInput{kind: leaseRevoke}, Call: 5, Output: leaseGenerationOutput{}, Return: 6},
		{ClientId: 0, Input: leaseInput{kind: leaseGrant}, Call: 7, Output: leaseGenerationOutput{}, Return: 8},
		{ClientId: 1, Input: leaseInput{kind: leaseRead}, Call: 9, Output: leaseGenerationOutput{present: true, value: 1}, Return: 10},
	}
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseGenerationModel, staleKey, time.Second))
}

func TestLeaseModelRequiresAtomicKeyDeletionOnRevoke(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: leaseInput{kind: leasePut, value: 1}, Call: 1, Output: leaseOutput{}, Return: 2},
		{ClientId: 1, Input: leaseInput{kind: leaseRevoke}, Call: 3, Output: leaseOutput{}, Return: 4},
		{ClientId: 2, Input: leaseInput{kind: leaseRead}, Call: 5, Output: leaseOutput{present: true, value: 1}, Return: 6},
	}
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseModel, history, time.Second))

	for _, present := range []bool{false, true} {
		ambiguous := []porcupine.Operation{
			{ClientId: 0, Input: leaseInput{kind: leasePut, value: 1}, Call: 1, Output: leaseOutput{}, Return: 2},
			{ClientId: 1, Input: leaseInput{kind: leaseRevoke}, Call: 3, Output: leaseOutput{failed: true}, Return: 4},
			{ClientId: 2, Input: leaseInput{kind: leaseRead}, Call: 5, Output: leaseOutput{present: present, value: 1}, Return: 6},
		}
		require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(leaseModel, ambiguous, time.Second))
	}
}

func TestClientV3LeaseGenerationHistoryIsLinearizable(t *testing.T) {
	const (
		clients                    = 5
		defaultOperationsPerClient = 10
	)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the lease generation linearizability history")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-linearizability/lease-generation/%d", time.Now().UnixNano())
	leaseID := clientv3.LeaseID(time.Now().UnixNano() & int64(^uint64(0)>>1))
	cleanupClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cleanupClient.Revoke(cleanupCtx, leaseID)
		_, _ = cleanupClient.Delete(cleanupCtx, key)
		_ = cleanupClient.Close()
	})

	var clock atomic.Int64
	var historyMu sync.Mutex
	operationsPerClient := defaultOperationsPerClient
	if failoverPod := linearizabilityDeletePod(); failoverPod != "" {
		operationsPerClient = 30
	}
	history := make([]porcupine.Operation, 0, clients*operationsPerClient)
	setupErrCh := make(chan error, clients+1)
	var ambiguousFailures atomic.Int64
	var workers sync.WaitGroup
	failoverPod := linearizabilityDeletePod()
	if failoverPod != "" {
		startLinearizabilityPodDeletion(ctx, &clock, failoverPod, setupErrCh, &workers)
	}
	for clientID := 0; clientID < clients; clientID++ {
		workers.Add(1)
		go func(clientID int) {
			defer workers.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
			if err != nil {
				setupErrCh <- err
				return
			}
			defer cli.Close()
			for i := 0; i < operationsPerClient; i++ {
				input := leaseGenerationHistoryInput(clientID, i)
				call := clock.Add(1)
				output, err := invokeLeaseGenerationOperation(ctx, cli, key, leaseID, input)
				returned := clock.Add(1)
				if err != nil {
					if !isAmbiguousRPCError(err) {
						setupErrCh <- fmt.Errorf("client %d operation %d: %w", clientID, i, err)
						return
					}
					output.failed = true
					if !output.notFound && !output.alreadyAlive {
						ambiguousFailures.Add(1)
					}
				}
				historyMu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: clientID, Input: input, Call: call, Output: output, Return: returned,
				})
				historyMu.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		}(clientID)
	}
	workers.Wait()
	close(setupErrCh)
	for err := range setupErrCh {
		require.NoError(t, err)
	}
	require.Len(t, history, clients*operationsPerClient)
	if failoverPod == "" {
		require.Zero(t, ambiguousFailures.Load(), "baseline history must not contain ambiguous RPC failures")
	} else {
		t.Logf("recorded %d ambiguous lease generation RPC failures during pod deletion", ambiguousFailures.Load())
	}
	result := porcupine.CheckOperationsTimeout(leaseGenerationModel, history, 10*time.Second)
	if result != porcupine.Ok {
		sort.Slice(history, func(i, j int) bool { return history[i].Call < history[j].Call })
		for _, operation := range history {
			t.Logf("client=%d call=%d return=%d %s", operation.ClientId, operation.Call, operation.Return,
				leaseGenerationModel.DescribeOperation(operation.Input, operation.Output))
		}
	}
	require.Equalf(t, porcupine.Ok, result, "lease generation history result: %s", result)
}

func TestClientV3LeaseLifecycleHistoryIsLinearizable(t *testing.T) {
	const (
		clients             = 5
		operationsPerClient = 12
	)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the lease lifecycle linearizability history")
	}
	failoverPod := linearizabilityDeletePod()
	operationDelay := 10 * time.Millisecond
	if failoverPod != "" {
		// kubectl startup can outlast the short 60-operation baseline. Keep fault
		// histories active long enough for storage and leader pod deletion to
		// overlap client calls instead of accepting a no-op fault run.
		operationDelay = 100 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-linearizability/lease/%d", time.Now().UnixNano())

	seed, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = seed.Close() })
	// Keep expiry outside the 90-second fault window; this model exercises
	// explicit revoke, while TTL-driven deletion is covered separately.
	grant, err := seed.Grant(ctx, 300)
	require.NoError(t, err)
	leaseID := grant.ID
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = seed.Revoke(cleanupCtx, leaseID)
		_, _ = seed.Delete(cleanupCtx, key)
	})

	var clock atomic.Int64
	var historyMu sync.Mutex
	history := make([]porcupine.Operation, 0, clients*operationsPerClient)
	setupErrCh := make(chan error, clients+1)
	var ambiguousFailures atomic.Int64
	var workers sync.WaitGroup
	if failoverPod != "" {
		startLinearizabilityPodDeletion(ctx, &clock, failoverPod, setupErrCh, &workers)
	}
	for clientID := 0; clientID < clients; clientID++ {
		workers.Add(1)
		go func(clientID int) {
			defer workers.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
			if err != nil {
				setupErrCh <- err
				return
			}
			defer cli.Close()
			for i := 0; i < operationsPerClient; i++ {
				input := leaseHistoryInput(clientID, i)
				call := clock.Add(1)
				output, err := invokeLeaseOperation(ctx, cli, key, leaseID, input)
				returned := clock.Add(1)
				if err != nil {
					if !isAmbiguousRPCError(err) {
						setupErrCh <- fmt.Errorf("client %d operation %d: %w", clientID, i, err)
						return
					}
					output.failed = true
					if !output.notFound {
						ambiguousFailures.Add(1)
					}
				}
				historyMu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: clientID, Input: input, Call: call, Output: output, Return: returned,
				})
				historyMu.Unlock()
				time.Sleep(operationDelay)
			}
		}(clientID)
	}
	workers.Wait()
	close(setupErrCh)
	for err := range setupErrCh {
		require.NoError(t, err)
	}
	require.Len(t, history, clients*operationsPerClient)
	if failoverPod == "" {
		require.Zero(t, ambiguousFailures.Load(), "baseline history must not contain ambiguous RPC failures")
	} else if namespace := os.Getenv("LINEARIZABILITY_DELETE_NAMESPACE"); namespace != "" && namespace != "kubebrain-dev" {
		t.Logf("recorded %d ambiguous lease RPC failures during %s pod deletion", ambiguousFailures.Load(), namespace)
	} else {
		require.Positive(t, ambiguousFailures.Load(), "fault history must exercise ambiguous RPC outcomes")
		t.Logf("recorded %d ambiguous lease RPC failures during pod deletion", ambiguousFailures.Load())
	}
	result := porcupine.CheckOperationsTimeout(leaseModel, history, 10*time.Second)
	if result != porcupine.Ok {
		sort.Slice(history, func(i, j int) bool { return history[i].Call < history[j].Call })
		for _, operation := range history {
			t.Logf("client=%d call=%d return=%d %s", operation.ClientId, operation.Call, operation.Return,
				leaseModel.DescribeOperation(operation.Input, operation.Output))
		}
	}
	require.Equalf(t, porcupine.Ok, result, "lease lifecycle history result: %s", result)
}

func leaseHistoryInput(clientID, iteration int) leaseInput {
	switch clientID {
	case 0:
		return leaseInput{kind: leasePut, value: 1 + iteration}
	case 1:
		return leaseInput{kind: leaseRead}
	case 2:
		return leaseInput{kind: leaseKeepAlive}
	case 3:
		if iteration == 5 {
			return leaseInput{kind: leaseRevoke}
		}
		return leaseInput{kind: leaseRead}
	default:
		if iteration%2 == 0 {
			return leaseInput{kind: leasePut, value: 100 + iteration}
		}
		return leaseInput{kind: leaseRead}
	}
}

func leaseGenerationHistoryInput(clientID, iteration int) leaseInput {
	switch clientID {
	case 0:
		return leaseInput{kind: leaseGrant}
	case 1:
		return leaseInput{kind: leaseRevoke}
	case 2:
		return leaseInput{kind: leasePut, value: 200 + iteration}
	case 3:
		return leaseInput{kind: leaseRead}
	default:
		return leaseInput{kind: leaseTimeToLive}
	}
}

func invokeLeaseGenerationOperation(
	ctx context.Context,
	cli *clientv3.Client,
	key string,
	leaseID clientv3.LeaseID,
	input leaseInput,
) (leaseGenerationOutput, error) {
	switch input.kind {
	case leaseGrant:
		_, err := etcdserverpb.NewLeaseClient(cli.ActiveConnection()).LeaseGrant(ctx,
			&etcdserverpb.LeaseGrantRequest{TTL: 300, ID: int64(leaseID)})
		if errors.Is(err, rpctypes.ErrLeaseExist) {
			return leaseGenerationOutput{alreadyAlive: true}, nil
		}
		if err != nil {
			return leaseGenerationOutput{}, markAmbiguousRPCError(err)
		}
		return leaseGenerationOutput{}, nil
	case leasePut:
		_, err := cli.Put(ctx, key, strconv.Itoa(input.value), clientv3.WithLease(leaseID))
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return leaseGenerationOutput{notFound: true}, nil
		}
		if err != nil {
			return leaseGenerationOutput{}, markAmbiguousRPCError(err)
		}
		return leaseGenerationOutput{}, nil
	case leaseRead:
		resp, err := cli.Get(ctx, key)
		if err != nil {
			return leaseGenerationOutput{}, markAmbiguousRPCError(err)
		}
		if len(resp.Kvs) == 0 {
			return leaseGenerationOutput{}, nil
		}
		if len(resp.Kvs) != 1 || resp.Kvs[0].Lease != int64(leaseID) {
			return leaseGenerationOutput{}, fmt.Errorf("lease generation read returned an invalid key or lease binding")
		}
		value, err := strconv.Atoi(string(resp.Kvs[0].Value))
		return leaseGenerationOutput{present: true, value: value}, err
	case leaseTimeToLive:
		resp, err := cli.TimeToLive(ctx, leaseID)
		if err != nil {
			return leaseGenerationOutput{}, markAmbiguousRPCError(err)
		}
		return leaseGenerationOutput{alive: resp.TTL >= 0}, nil
	case leaseRevoke:
		_, err := cli.Revoke(ctx, leaseID)
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return leaseGenerationOutput{notFound: true}, nil
		}
		if err != nil {
			return leaseGenerationOutput{}, markAmbiguousRPCError(err)
		}
		return leaseGenerationOutput{}, nil
	default:
		return leaseGenerationOutput{}, fmt.Errorf("unknown lease generation operation %d", input.kind)
	}
}

func invokeLeaseOperation(ctx context.Context, cli *clientv3.Client, key string, leaseID clientv3.LeaseID, input leaseInput) (leaseOutput, error) {
	switch input.kind {
	case leaseRead:
		resp, err := cli.Get(ctx, key)
		if err != nil {
			return leaseOutput{}, markAmbiguousRPCError(err)
		}
		if len(resp.Kvs) == 0 {
			return leaseOutput{}, nil
		}
		if len(resp.Kvs) != 1 || resp.Kvs[0].Lease != int64(leaseID) {
			return leaseOutput{}, fmt.Errorf("lease read returned an invalid key or lease binding")
		}
		value, err := strconv.Atoi(string(resp.Kvs[0].Value))
		return leaseOutput{present: true, value: value}, err
	case leasePut:
		_, err := cli.Put(ctx, key, strconv.Itoa(input.value), clientv3.WithLease(leaseID))
		return leaseRPCOutput(err)
	case leaseKeepAlive:
		resp, err := cli.KeepAliveOnce(ctx, leaseID)
		if err != nil {
			return leaseRPCOutput(err)
		}
		if resp == nil || resp.TTL <= 0 {
			return leaseOutput{}, fmt.Errorf("keepalive returned a non-positive TTL")
		}
		return leaseOutput{}, nil
	case leaseRevoke:
		_, err := cli.Revoke(ctx, leaseID)
		return leaseRPCOutput(err)
	default:
		return leaseOutput{}, fmt.Errorf("unknown lease operation %d", input.kind)
	}
}

func leaseRPCOutput(err error) (leaseOutput, error) {
	if err == nil {
		return leaseOutput{}, nil
	}
	return leaseOutput{notFound: errors.Is(err, rpctypes.ErrLeaseNotFound)}, markAmbiguousRPCError(err)
}
