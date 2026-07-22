package compat

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseLongOpKind uint8

const (
	leaseLongGrant leaseLongOpKind = iota
	leaseLongPut
	leaseLongKeepAlive
	leaseLongAdvance
	leaseLongRead
	leaseLongTimeToLive
)

type leaseLongState struct {
	alive          bool
	present        bool
	value          int
	ttlUnits       int
	remainingUnits int
}

type leaseLongInput struct {
	kind         leaseLongOpKind
	value        int
	ttlUnits     int
	advanceUnits int
}

type leaseLongOutput struct {
	alive   bool
	present bool
	value   int
}

var leaseLongRenewalModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{leaseLongState{}} },
	Step: func(state, input, output interface{}) []interface{} {
		current := state.(leaseLongState)
		in := input.(leaseLongInput)
		out := output.(leaseLongOutput)
		switch in.kind {
		case leaseLongGrant:
			if current.alive || in.ttlUnits <= 0 {
				return nil
			}
			current.alive = true
			current.present = false
			current.value = 0
			current.ttlUnits = in.ttlUnits
			current.remainingUnits = in.ttlUnits
			return []interface{}{current}
		case leaseLongPut:
			if !current.alive {
				return nil
			}
			current.present = true
			current.value = in.value
			return []interface{}{current}
		case leaseLongKeepAlive:
			if !current.alive {
				return nil
			}
			current.remainingUnits = current.ttlUnits
			return []interface{}{current}
		case leaseLongAdvance:
			if in.advanceUnits <= 0 {
				return nil
			}
			if current.alive {
				current.remainingUnits -= in.advanceUnits
				if current.remainingUnits <= 0 {
					current.alive = false
					current.present = false
					current.value = 0
					current.remainingUnits = 0
				}
			}
			return []interface{}{current}
		case leaseLongRead:
			if out.present == current.present && (!out.present || out.value == current.value) {
				return []interface{}{current}
			}
		case leaseLongTimeToLive:
			if out.alive == current.alive {
				return []interface{}{current}
			}
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(leaseLongInput)
		out := output.(leaseLongOutput)
		switch in.kind {
		case leaseLongGrant:
			return fmt.Sprintf("grant(%d windows) -> ok", in.ttlUnits)
		case leaseLongPut:
			return fmt.Sprintf("put-with-lease(%d) -> ok", in.value)
		case leaseLongKeepAlive:
			return "keepalive-once() -> ok"
		case leaseLongAdvance:
			return fmt.Sprintf("advance(%d windows) -> ok", in.advanceUnits)
		case leaseLongRead:
			return fmt.Sprintf("get() -> {present:%t value:%d}", out.present, out.value)
		case leaseLongTimeToLive:
			return fmt.Sprintf("time-to-live() -> {alive:%t}", out.alive)
		default:
			return "unknown"
		}
	},
}).ToModel()

func TestLeaseLongRenewalModelExtendsDeadlineAcrossRenewalWindows(t *testing.T) {
	valid := leaseLongOperations(
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongGrant, ttlUnits: 2}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongPut, value: 100}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongKeepAlive}},
		leaseLongOperationSpec{clientID: 2, input: leaseLongInput{kind: leaseLongTimeToLive}, output: leaseLongOutput{alive: true}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongKeepAlive}},
		leaseLongOperationSpec{clientID: 2, input: leaseLongInput{kind: leaseLongRead}, output: leaseLongOutput{present: true, value: 100}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongKeepAlive}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}},
		leaseLongOperationSpec{clientID: 2, input: leaseLongInput{kind: leaseLongRead}, output: leaseLongOutput{present: true, value: 100}},
	)
	require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(leaseLongRenewalModel, valid, time.Second))

	drainedFromGrantDeadline := leaseLongOperations(
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongGrant, ttlUnits: 2}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongPut, value: 100}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongKeepAlive}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}},
		leaseLongOperationSpec{clientID: 2, input: leaseLongInput{kind: leaseLongRead}},
	)
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseLongRenewalModel, drainedFromGrantDeadline, time.Second))

	renewalAfterFullWindow := leaseLongOperations(
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongGrant, ttlUnits: 2}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongPut, value: 100}},
		leaseLongOperationSpec{clientID: 1, input: leaseLongInput{kind: leaseLongAdvance, advanceUnits: 2}},
		leaseLongOperationSpec{clientID: 0, input: leaseLongInput{kind: leaseLongKeepAlive}},
	)
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseLongRenewalModel, renewalAfterFullWindow, time.Second))
}

type leaseLongOperationSpec struct {
	clientID int
	input    leaseLongInput
	output   leaseLongOutput
}

func leaseLongOperations(specs ...leaseLongOperationSpec) []porcupine.Operation {
	history := make([]porcupine.Operation, 0, len(specs))
	var clock int64
	for _, spec := range specs {
		clock++
		call := clock
		clock++
		history = append(history, porcupine.Operation{
			ClientId: spec.clientID,
			Input:    spec.input,
			Call:     call,
			Output:   spec.output,
			Return:   clock,
		})
	}
	return history
}

func TestClientV3LeaseRepeatedRenewalHistoryIsLinearizable(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the lease repeated-renewal linearizability history")
	}
	const (
		cycles          = 6
		leaseTTLSeconds = 3
		virtualTTLUnits = 2
		value           = 700
	)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	key := fmt.Sprintf("/dbaas-linearizability/lease-long-renewal/%d", time.Now().UnixNano())
	var leaseID clientv3.LeaseID
	var clock int64
	history := make([]porcupine.Operation, 0, cycles*4+6)
	appendOperation := func(clientID int, input leaseLongInput, call int64, output leaseLongOutput, returned int64) {
		history = append(history, porcupine.Operation{
			ClientId: clientID,
			Input:    input,
			Call:     call,
			Output:   output,
			Return:   returned,
		})
	}
	recordRPC := func(clientID int, input leaseLongInput, output leaseLongOutput, rpc func() error) {
		clock++
		call := clock
		require.NoError(t, rpc())
		clock++
		appendOperation(clientID, input, call, output, clock)
	}
	recordAdvance := func() {
		clock++
		call := clock
		timer := time.NewTimer(1200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatalf("context ended while advancing renewal window: %v", ctx.Err())
		}
		clock++
		appendOperation(1, leaseLongInput{kind: leaseLongAdvance, advanceUnits: 1}, call, leaseLongOutput{}, clock)
	}

	recordRPC(0, leaseLongInput{kind: leaseLongGrant, ttlUnits: virtualTTLUnits}, leaseLongOutput{}, func() error {
		grant, grantErr := cli.Grant(ctx, leaseTTLSeconds)
		if grantErr == nil {
			leaseID = grant.ID
		}
		return grantErr
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if leaseID != 0 {
			_, _ = cli.Revoke(cleanupCtx, leaseID)
		}
		_, _ = cli.Delete(cleanupCtx, key)
	})
	recordRPC(0, leaseLongInput{kind: leaseLongPut, value: value}, leaseLongOutput{}, func() error {
		_, putErr := cli.Put(ctx, key, strconv.Itoa(value), clientv3.WithLease(leaseID))
		return putErr
	})

	for cycle := 0; cycle < cycles; cycle++ {
		recordAdvance()
		recordRPC(0, leaseLongInput{kind: leaseLongKeepAlive}, leaseLongOutput{}, func() error {
			keepAlive, keepAliveErr := cli.KeepAliveOnce(ctx, leaseID)
			if keepAliveErr != nil {
				return keepAliveErr
			}
			if keepAlive == nil || keepAlive.TTL <= 0 {
				return fmt.Errorf("lease %d returned invalid keepalive response: %#v", leaseID, keepAlive)
			}
			return nil
		})
		recordLeaseLongTTLAndRead(t, ctx, cli, key, leaseID, &clock, appendOperation, value)
	}
	recordAdvance()
	recordLeaseLongTTLAndRead(t, ctx, cli, key, leaseID, &clock, appendOperation, value)

	result := porcupine.CheckOperationsTimeout(leaseLongRenewalModel, history, 10*time.Second)
	if result != porcupine.Ok {
		sort.Slice(history, func(i, j int) bool { return history[i].Call < history[j].Call })
		for _, operation := range history {
			t.Logf("client=%d call=%d return=%d %s", operation.ClientId, operation.Call, operation.Return,
				leaseLongRenewalModel.DescribeOperation(operation.Input, operation.Output))
		}
	}
	require.Equalf(t, porcupine.Ok, result, "lease long-renewal history result: %s", result)
}

func recordLeaseLongTTLAndRead(
	t *testing.T,
	ctx context.Context,
	cli *clientv3.Client,
	key string,
	leaseID clientv3.LeaseID,
	clock *int64,
	appendOperation func(int, leaseLongInput, int64, leaseLongOutput, int64),
	wantValue int,
) {
	t.Helper()
	*clock += 1
	ttlCall := *clock
	ttl, ttlErr := cli.TimeToLive(ctx, leaseID, clientv3.WithAttachedKeys())
	require.NoError(t, ttlErr)
	*clock += 1
	appendOperation(2, leaseLongInput{kind: leaseLongTimeToLive}, ttlCall, leaseLongOutput{alive: ttl.TTL >= 0}, *clock)
	require.Positive(t, ttl.TTL, "renewed lease %d must remain alive", leaseID)
	require.Equal(t, [][]byte{[]byte(key)}, ttl.Keys)

	*clock += 1
	readCall := *clock
	got, getErr := cli.Get(ctx, key)
	require.NoError(t, getErr)
	output := leaseLongOutput{}
	if len(got.Kvs) > 0 {
		require.Len(t, got.Kvs, 1)
		require.Equal(t, int64(leaseID), got.Kvs[0].Lease)
		value, parseErr := strconv.Atoi(string(got.Kvs[0].Value))
		require.NoError(t, parseErr)
		output.present, output.value = true, value
	}
	*clock += 1
	appendOperation(2, leaseLongInput{kind: leaseLongRead}, readCall, output, *clock)
	require.True(t, output.present)
	require.Equal(t, wantValue, output.value)
}
