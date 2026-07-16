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
}

var registerModel = porcupine.Model{
	Init: func() interface{} { return 0 },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		current := state.(int)
		in := input.(registerInput)
		out := output.(registerOutput)
		switch in.kind {
		case registerRead:
			return out.value == current, current
		case registerWrite:
			return true, in.value
		case registerCAS:
			shouldSwap := current == in.expect
			if out.swapped != shouldSwap {
				return false, current
			}
			if shouldSwap {
				return true, in.desired
			}
			return true, current
		default:
			panic("unknown register operation")
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(registerInput)
		out := output.(registerOutput)
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
}

func TestRegisterModelRejectsImpossibleHistory(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: registerInput{kind: registerWrite, value: 1}, Call: 1, Output: registerOutput{}, Return: 2},
		{ClientId: 1, Input: registerInput{kind: registerRead}, Call: 3, Output: registerOutput{value: 0}, Return: 4},
	}
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(registerModel, history, time.Second))
}

func TestClientV3RegisterHistoryIsLinearizable(t *testing.T) {
	const (
		clients             = 5
		operationsPerClient = 12
	)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the client/v3 linearizability history")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	errCh := make(chan error, clients)
	var workers sync.WaitGroup
	for clientID := 0; clientID < clients; clientID++ {
		workers.Add(1)
		go func(clientID int) {
			defer workers.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
			if err != nil {
				errCh <- err
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
					errCh <- fmt.Errorf("client %d operation %d: %w", clientID, i, err)
					return
				}
				historyMu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: clientID, Input: input, Call: call, Output: output, Return: returned,
				})
				historyMu.Unlock()
			}
		}(clientID)
	}
	workers.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.Len(t, history, clients*operationsPerClient)
	result := porcupine.CheckOperationsTimeout(registerModel, history, 10*time.Second)
	require.Equalf(t, porcupine.Ok, result, "register history result: %s", result)
}

func invokeRegisterOperation(ctx context.Context, cli *clientv3.Client, key string, input registerInput) (registerOutput, error) {
	switch input.kind {
	case registerRead:
		resp, err := cli.Get(ctx, key)
		if err != nil {
			return registerOutput{}, err
		}
		if len(resp.Kvs) != 1 {
			return registerOutput{}, fmt.Errorf("get returned %d values", len(resp.Kvs))
		}
		value, err := strconv.Atoi(string(resp.Kvs[0].Value))
		return registerOutput{value: value}, err
	case registerWrite:
		_, err := cli.Put(ctx, key, strconv.Itoa(input.value))
		return registerOutput{}, err
	case registerCAS:
		resp, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(key), "=", strconv.Itoa(input.expect))).
			Then(clientv3.OpPut(key, strconv.Itoa(input.desired))).
			Commit()
		if err != nil {
			return registerOutput{}, err
		}
		return registerOutput{swapped: resp.Succeeded}, nil
	default:
		return registerOutput{}, fmt.Errorf("unknown register operation %d", input.kind)
	}
}
