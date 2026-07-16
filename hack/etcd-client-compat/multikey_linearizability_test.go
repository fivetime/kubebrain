package compat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type pairValue struct {
	left  int
	right int
}

type pairInput struct {
	kind    registerOpKind
	value   pairValue
	expect  pairValue
	desired pairValue
}

type pairOutput struct {
	value   pairValue
	swapped bool
	failed  bool
}

var pairTxnModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{pairValue{}} },
	Step: func(state, input, output interface{}) []interface{} {
		current := state.(pairValue)
		in := input.(pairInput)
		out := output.(pairOutput)
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
				panic("unknown pair operation")
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
			panic("unknown pair operation")
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(pairInput)
		out := output.(pairOutput)
		if out.failed {
			return fmt.Sprintf("%s -> unknown", describePairInput(in))
		}
		switch in.kind {
		case registerRead:
			return fmt.Sprintf("txn-get() -> %v", out.value)
		case registerWrite:
			return fmt.Sprintf("txn-put(%v)", in.value)
		case registerCAS:
			return fmt.Sprintf("txn-cas(%v, %v) -> %t", in.expect, in.desired, out.swapped)
		default:
			return "unknown"
		}
	},
}).ToModel()

func describePairInput(in pairInput) string {
	switch in.kind {
	case registerRead:
		return "txn-get()"
	case registerWrite:
		return fmt.Sprintf("txn-put(%v)", in.value)
	case registerCAS:
		return fmt.Sprintf("txn-cas(%v, %v)", in.expect, in.desired)
	default:
		return "unknown"
	}
}

func TestPairTxnModelRejectsTornWrites(t *testing.T) {
	for _, failed := range []bool{false, true} {
		history := []porcupine.Operation{
			{
				ClientId: 0, Input: pairInput{kind: registerWrite, value: pairValue{left: 1, right: -1}}, Call: 1,
				Output: pairOutput{failed: failed}, Return: 2,
			},
			{
				ClientId: 1, Input: pairInput{kind: registerRead}, Call: 3,
				Output: pairOutput{value: pairValue{left: 1, right: 0}}, Return: 4,
			},
		}
		require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(pairTxnModel, history, time.Second))
	}
}

func TestClientV3MultiKeyTxnHistoryIsLinearizable(t *testing.T) {
	const (
		clients                    = 5
		defaultOperationsPerClient = 12
	)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the multi-key Txn linearizability history")
	}
	failoverPod := linearizabilityDeletePod()
	operationsPerClient := defaultOperationsPerClient
	if failoverPod != "" {
		operationsPerClient = 30
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/dbaas-linearizability/multikey/%d/", time.Now().UnixNano())
	leftKey, rightKey := prefix+"left", prefix+"right"

	seed, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = seed.Close() })
	_, err = seed.Txn(ctx).Then(clientv3.OpPut(leftKey, "0"), clientv3.OpPut(rightKey, "0")).Commit()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = seed.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
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
				input := pairInput{kind: registerOpKind((clientID + i) % 3)}
				unique := 1 + clientID*operationsPerClient + i
				input.value = pairValue{left: unique, right: -unique}
				input.desired = input.value
				if i%2 != 0 {
					input.expect = pairValue{left: unique - 1, right: -(unique - 1)}
				}
				call := clock.Add(1)
				output, err := invokePairOperation(ctx, cli, leftKey, rightKey, input)
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
		t.Logf("recorded %d ambiguous multi-key Txn failures during pod deletion", failedOperations.Load())
	}
	result := porcupine.CheckOperationsTimeout(pairTxnModel, history, 10*time.Second)
	require.Equalf(t, porcupine.Ok, result, "multi-key Txn history result: %s", result)
}

func invokePairOperation(ctx context.Context, cli *clientv3.Client, leftKey, rightKey string, input pairInput) (pairOutput, error) {
	switch input.kind {
	case registerRead:
		resp, err := cli.Txn(ctx).Then(clientv3.OpGet(leftKey), clientv3.OpGet(rightKey)).Commit()
		if err != nil {
			return pairOutput{}, markAmbiguousRPCError(err)
		}
		if len(resp.Responses) != 2 {
			return pairOutput{}, fmt.Errorf("multi-key read returned an invalid response shape")
		}
		leftRange, rightRange := resp.Responses[0].GetResponseRange(), resp.Responses[1].GetResponseRange()
		if leftRange == nil || rightRange == nil || len(leftRange.Kvs) != 1 || len(rightRange.Kvs) != 1 {
			return pairOutput{}, fmt.Errorf("multi-key read returned an invalid range response")
		}
		left, err := strconv.Atoi(string(leftRange.Kvs[0].Value))
		if err != nil {
			return pairOutput{}, err
		}
		right, err := strconv.Atoi(string(rightRange.Kvs[0].Value))
		return pairOutput{value: pairValue{left: left, right: right}}, err
	case registerWrite:
		_, err := cli.Txn(ctx).Then(
			clientv3.OpPut(leftKey, strconv.Itoa(input.value.left)),
			clientv3.OpPut(rightKey, strconv.Itoa(input.value.right)),
		).Commit()
		return pairOutput{}, markAmbiguousRPCError(err)
	case registerCAS:
		resp, err := cli.Txn(ctx).If(
			clientv3.Compare(clientv3.Value(leftKey), "=", strconv.Itoa(input.expect.left)),
			clientv3.Compare(clientv3.Value(rightKey), "=", strconv.Itoa(input.expect.right)),
		).Then(
			clientv3.OpPut(leftKey, strconv.Itoa(input.desired.left)),
			clientv3.OpPut(rightKey, strconv.Itoa(input.desired.right)),
		).Commit()
		if err != nil {
			return pairOutput{}, markAmbiguousRPCError(err)
		}
		return pairOutput{swapped: resp.Succeeded}, nil
	default:
		return pairOutput{}, fmt.Errorf("unknown pair operation %d", input.kind)
	}
}
