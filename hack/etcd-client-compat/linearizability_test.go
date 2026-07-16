package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type registerOpKind uint8

const (
	registerRead registerOpKind = iota
	registerWrite
	registerCAS
)

type registerInput struct {
	kind    registerOpKind
	value   int
	expect  int
	desired int
}

type registerOutput struct {
	value   int
	swapped bool
	failed  bool
}

var registerModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{0} },
	Step: func(state, input, output interface{}) []interface{} {
		current := state.(int)
		in := input.(registerInput)
		out := output.(registerOutput)
		if out.failed {
			switch in.kind {
			case registerRead:
				return []interface{}{current}
			case registerWrite:
				return []interface{}{current, in.value}
			case registerCAS:
				if current == in.expect {
					return []interface{}{current, in.desired}
				}
				return []interface{}{current}
			default:
				panic("unknown register operation")
			}
		}
		switch in.kind {
		case registerRead:
			if out.value == current {
				return []interface{}{current}
			}
		case registerWrite:
			return []interface{}{in.value}
		case registerCAS:
			shouldSwap := current == in.expect
			if out.swapped != shouldSwap {
				return nil
			}
			if shouldSwap {
				return []interface{}{in.desired}
			}
			return []interface{}{current}
		default:
			panic("unknown register operation")
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(registerInput)
		out := output.(registerOutput)
		if out.failed {
			return fmt.Sprintf("%s -> unknown", describeRegisterInput(in))
		}
		switch in.kind {
		case registerRead:
			return fmt.Sprintf("get() -> %d", out.value)
		case registerWrite:
			return fmt.Sprintf("put(%d)", in.value)
		case registerCAS:
			return fmt.Sprintf("cas(%d, %d) -> %t", in.expect, in.desired, out.swapped)
		default:
			return "unknown"
		}
	},
}).ToModel()

func describeRegisterInput(in registerInput) string {
	switch in.kind {
	case registerRead:
		return "get()"
	case registerWrite:
		return fmt.Sprintf("put(%d)", in.value)
	case registerCAS:
		return fmt.Sprintf("cas(%d, %d)", in.expect, in.desired)
	default:
		return "unknown"
	}
}

func TestRegisterModelRejectsImpossibleHistory(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: registerInput{kind: registerWrite, value: 1}, Call: 1, Output: registerOutput{}, Return: 2},
		{ClientId: 1, Input: registerInput{kind: registerRead}, Call: 3, Output: registerOutput{value: 0}, Return: 4},
	}
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(registerModel, history, time.Second))
}

func TestRegisterModelAllowsEitherOutcomeForFailedWrite(t *testing.T) {
	failedPut := porcupine.Operation{
		ClientId: 0, Input: registerInput{kind: registerWrite, value: 1}, Call: 1,
		Output: registerOutput{failed: true}, Return: 2,
	}
	for _, value := range []int{0, 1} {
		history := []porcupine.Operation{
			failedPut,
			{ClientId: 1, Input: registerInput{kind: registerRead}, Call: 3, Output: registerOutput{value: value}, Return: 4},
		}
		require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(registerModel, history, time.Second))
	}
	failedCAS := porcupine.Operation{
		ClientId: 0, Input: registerInput{kind: registerCAS, expect: 0, desired: 1}, Call: 1,
		Output: registerOutput{failed: true}, Return: 2,
	}
	for _, value := range []int{0, 1} {
		history := []porcupine.Operation{
			failedCAS,
			{ClientId: 1, Input: registerInput{kind: registerRead}, Call: 3, Output: registerOutput{value: value}, Return: 4},
		}
		require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(registerModel, history, time.Second))
	}
	impossibleCAS := []porcupine.Operation{
		{ClientId: 0, Input: registerInput{kind: registerCAS, expect: 2, desired: 1}, Call: 1, Output: registerOutput{failed: true}, Return: 2},
		{ClientId: 1, Input: registerInput{kind: registerRead}, Call: 3, Output: registerOutput{value: 1}, Return: 4},
	}
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(registerModel, impossibleCAS, time.Second))
}

func TestClientV3RegisterHistoryIsLinearizable(t *testing.T) {
	const (
		clients                    = 5
		defaultOperationsPerClient = 12
	)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the client/v3 linearizability history")
	}
	failoverPod := linearizabilityDeletePod()
	operationsPerClient := defaultOperationsPerClient
	if failoverPod != "" {
		operationsPerClient = 30
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-linearizability/register/%d", time.Now().UnixNano())

	seed, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = seed.Close() })
	_, err = seed.Put(ctx, key, "0")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = seed.Delete(cleanupCtx, key)
	})

	var clock atomic.Int64
	var historyMu sync.Mutex
	history := make([]porcupine.Operation, 0, clients*operationsPerClient)
	setupErrCh := make(chan error, clients+1)
	var failedOperations atomic.Int64
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
				input := registerInput{kind: registerOpKind((clientID + i) % 3)}
				unique := 1 + clientID*operationsPerClient + i
				input.value, input.desired = unique, unique
				if i%2 == 0 {
					input.expect = 0
				} else {
					input.expect = unique - 1
				}
				call := clock.Add(1)
				output, err := invokeRegisterOperation(ctx, cli, key, input)
				returned := clock.Add(1)
				if err != nil {
					if !isAmbiguousRPCError(err) {
						setupErrCh <- fmt.Errorf("client %d operation %d: %w", clientID, i, err)
						return
					}
					output.failed = true
					failedOperations.Add(1)
				}
				historyMu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: clientID, Input: input, Call: call, Output: output, Return: returned,
				})
				historyMu.Unlock()
				if failoverPod != "" {
					time.Sleep(10 * time.Millisecond)
				}
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
		require.Zero(t, failedOperations.Load(), "baseline history must not contain failed RPCs")
	} else {
		require.Positive(t, failedOperations.Load(), "fault history must exercise ambiguous RPC outcomes")
		t.Logf("recorded %d ambiguous RPC failures during pod deletion", failedOperations.Load())
	}
	result := porcupine.CheckOperationsTimeout(registerModel, history, 10*time.Second)
	require.Equalf(t, porcupine.Ok, result, "register history result: %s", result)
}

type ambiguousRPCError struct{ err error }

func (e ambiguousRPCError) Error() string { return e.err.Error() }
func (e ambiguousRPCError) Unwrap() error { return e.err }

func markAmbiguousRPCError(err error) error {
	if err == nil {
		return nil
	}
	return ambiguousRPCError{err: err}
}

func isAmbiguousRPCError(err error) bool {
	var target ambiguousRPCError
	return errors.As(err, &target)
}

func linearizabilityDeletePod() string {
	if pod := os.Getenv("LINEARIZABILITY_DELETE_POD"); pod != "" {
		return pod
	}
	return os.Getenv("KUBEBRAIN_LINEARIZABILITY_DELETE_POD")
}

func startLinearizabilityPodDeletion(ctx context.Context, clock *atomic.Int64, pod string, errCh chan<- error, workers *sync.WaitGroup) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		for clock.Load() < 20 {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
		namespace := os.Getenv("LINEARIZABILITY_DELETE_NAMESPACE")
		if namespace == "" {
			namespace = os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
		}
		if namespace == "" {
			namespace = "kubebrain-dev"
		}
		output, err := exec.CommandContext(ctx, "kubectl", "-n", namespace, "delete", "pod", pod, "--wait=false").CombinedOutput()
		if err != nil {
			errCh <- fmt.Errorf("delete failover pod: %w: %s", err, output)
		}
	}()
}

func invokeRegisterOperation(ctx context.Context, cli *clientv3.Client, key string, input registerInput) (registerOutput, error) {
	switch input.kind {
	case registerRead:
		resp, err := cli.Get(ctx, key)
		if err != nil {
			return registerOutput{}, markAmbiguousRPCError(err)
		}
		if len(resp.Kvs) != 1 {
			return registerOutput{}, fmt.Errorf("get returned %d values", len(resp.Kvs))
		}
		value, err := strconv.Atoi(string(resp.Kvs[0].Value))
		return registerOutput{value: value}, err
	case registerWrite:
		_, err := cli.Put(ctx, key, strconv.Itoa(input.value))
		return registerOutput{}, markAmbiguousRPCError(err)
	case registerCAS:
		resp, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(key), "=", strconv.Itoa(input.expect))).
			Then(clientv3.OpPut(key, strconv.Itoa(input.desired))).
			Commit()
		if err != nil {
			return registerOutput{}, markAmbiguousRPCError(err)
		}
		return registerOutput{swapped: resp.Succeeded}, nil
	default:
		return registerOutput{}, fmt.Errorf("unknown register operation %d", input.kind)
	}
}
