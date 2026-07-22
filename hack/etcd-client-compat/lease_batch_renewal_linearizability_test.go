package compat

import (
	"context"
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
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseBatchOpKind uint8

const (
	leaseBatchGrant leaseBatchOpKind = iota
	leaseBatchPut
	leaseBatchKeepAlive
	leaseBatchOriginalExpire
	leaseBatchRead
	leaseBatchTimeToLive
)

const leaseBatchModelSlots = 8

type leaseBatchSlotState struct {
	alive   bool
	present bool
	renewed bool
	value   int
}

type leaseBatchState struct {
	slots [leaseBatchModelSlots]leaseBatchSlotState
}

type leaseBatchInput struct {
	kind  leaseBatchOpKind
	slot  int
	value int
}

type leaseBatchOutput struct {
	alive   bool
	present bool
	value   int
}

var leaseBatchRenewalModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{leaseBatchState{}} },
	Step: func(state, input, output interface{}) []interface{} {
		current := state.(leaseBatchState)
		in := input.(leaseBatchInput)
		out := output.(leaseBatchOutput)
		if in.slot < 0 || in.slot >= len(current.slots) {
			return nil
		}
		slot := current.slots[in.slot]
		switch in.kind {
		case leaseBatchGrant:
			if slot.alive {
				return nil
			}
			slot.alive = true
			slot.present = false
			slot.renewed = false
			slot.value = 0
			current.slots[in.slot] = slot
			return []interface{}{current}
		case leaseBatchPut:
			if !slot.alive {
				return nil
			}
			slot.present = true
			slot.value = in.value
			current.slots[in.slot] = slot
			return []interface{}{current}
		case leaseBatchKeepAlive:
			if !slot.alive {
				return nil
			}
			slot.renewed = true
			current.slots[in.slot] = slot
			return []interface{}{current}
		case leaseBatchOriginalExpire:
			if !slot.alive || slot.renewed {
				return nil
			}
			slot.alive = false
			slot.present = false
			slot.value = 0
			current.slots[in.slot] = slot
			return []interface{}{current}
		case leaseBatchRead:
			if out.present == slot.present && (!out.present || out.value == slot.value) {
				return []interface{}{current}
			}
		case leaseBatchTimeToLive:
			if out.alive == slot.alive {
				return []interface{}{current}
			}
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(leaseBatchInput)
		out := output.(leaseBatchOutput)
		switch in.kind {
		case leaseBatchGrant:
			return fmt.Sprintf("lease[%d].grant() -> ok", in.slot)
		case leaseBatchPut:
			return fmt.Sprintf("lease[%d].put(%d) -> ok", in.slot, in.value)
		case leaseBatchKeepAlive:
			return fmt.Sprintf("lease[%d].keepalive-once() -> ok", in.slot)
		case leaseBatchOriginalExpire:
			return fmt.Sprintf("lease[%d].original-deadline-expire() -> ok", in.slot)
		case leaseBatchRead:
			return fmt.Sprintf("lease[%d].get() -> {present:%t value:%d}", in.slot, out.present, out.value)
		case leaseBatchTimeToLive:
			return fmt.Sprintf("lease[%d].time-to-live() -> {alive:%t}", in.slot, out.alive)
		default:
			return fmt.Sprintf("lease[%d].unknown() -> %#v", in.slot, out)
		}
	},
}).ToModel()

func TestLeaseBatchRenewalModelRequiresOriginalDeadlineIsolation(t *testing.T) {
	valid := leaseBatchOperations(
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchGrant, slot: 0}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchPut, slot: 0, value: 100}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchGrant, slot: 1}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchPut, slot: 1, value: 101}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchKeepAlive, slot: 0}},
		leaseBatchOperationSpec{clientID: 1, input: leaseBatchInput{kind: leaseBatchOriginalExpire, slot: 1}},
		leaseBatchOperationSpec{clientID: 2, input: leaseBatchInput{kind: leaseBatchRead, slot: 0}, output: leaseBatchOutput{present: true, value: 100}},
		leaseBatchOperationSpec{clientID: 2, input: leaseBatchInput{kind: leaseBatchTimeToLive, slot: 0}, output: leaseBatchOutput{alive: true}},
		leaseBatchOperationSpec{clientID: 3, input: leaseBatchInput{kind: leaseBatchRead, slot: 1}},
		leaseBatchOperationSpec{clientID: 3, input: leaseBatchInput{kind: leaseBatchTimeToLive, slot: 1}},
	)
	require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(leaseBatchRenewalModel, valid, time.Second))

	renewedExpiredAtOriginalDeadline := leaseBatchOperations(
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchGrant, slot: 0}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchPut, slot: 0, value: 100}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchKeepAlive, slot: 0}},
		leaseBatchOperationSpec{clientID: 1, input: leaseBatchInput{kind: leaseBatchOriginalExpire, slot: 0}},
	)
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseBatchRenewalModel, renewedExpiredAtOriginalDeadline, time.Second))

	staleExpiredKey := leaseBatchOperations(
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchGrant, slot: 0}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchPut, slot: 0, value: 100}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchGrant, slot: 1}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchPut, slot: 1, value: 101}},
		leaseBatchOperationSpec{clientID: 0, input: leaseBatchInput{kind: leaseBatchKeepAlive, slot: 0}},
		leaseBatchOperationSpec{clientID: 1, input: leaseBatchInput{kind: leaseBatchOriginalExpire, slot: 1}},
		leaseBatchOperationSpec{clientID: 3, input: leaseBatchInput{kind: leaseBatchRead, slot: 1}, output: leaseBatchOutput{present: true, value: 101}},
	)
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(leaseBatchRenewalModel, staleExpiredKey, time.Second))
}

type leaseBatchOperationSpec struct {
	clientID int
	input    leaseBatchInput
	output   leaseBatchOutput
}

func leaseBatchOperations(specs ...leaseBatchOperationSpec) []porcupine.Operation {
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

func TestClientV3LeaseBatchRenewalHistoryIsLinearizable(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the lease batch renewal linearizability history")
	}
	const (
		leaseCount      = 6
		leaseTTLSeconds = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	type batchLease struct {
		slot    int
		id      clientv3.LeaseID
		key     string
		value   int
		renewed bool
	}
	prefix := fmt.Sprintf("/dbaas-linearizability/lease-batch-renewal/%d/", time.Now().UnixNano())
	items := make([]batchLease, 0, leaseCount)
	var clock atomic.Int64
	var historyMu sync.Mutex
	history := make([]porcupine.Operation, 0, leaseCount*8)
	appendOperation := func(clientID int, input leaseBatchInput, call int64, output leaseBatchOutput, returned int64) {
		historyMu.Lock()
		history = append(history, porcupine.Operation{
			ClientId: clientID,
			Input:    input,
			Call:     call,
			Output:   output,
			Return:   returned,
		})
		historyMu.Unlock()
	}
	recordRPC := func(clientID int, input leaseBatchInput, output leaseBatchOutput, rpc func() error) {
		call := clock.Add(1)
		require.NoError(t, rpc())
		appendOperation(clientID, input, call, output, clock.Add(1))
	}

	var lastRevision int64
	for slot := 0; slot < leaseCount; slot++ {
		var grant *clientv3.LeaseGrantResponse
		recordRPC(slot, leaseBatchInput{kind: leaseBatchGrant, slot: slot}, leaseBatchOutput{}, func() error {
			var grantErr error
			grant, grantErr = cli.Grant(ctx, leaseTTLSeconds)
			return grantErr
		})
		item := batchLease{
			slot:    slot,
			id:      grant.ID,
			key:     fmt.Sprintf("%s%02d", prefix, slot),
			value:   100 + slot,
			renewed: slot%2 == 0,
		}
		recordRPC(slot, leaseBatchInput{kind: leaseBatchPut, slot: slot, value: item.value}, leaseBatchOutput{}, func() error {
			put, putErr := cli.Put(ctx, item.key, strconv.Itoa(item.value), clientv3.WithLease(item.id))
			if putErr == nil && put.Header.Revision > lastRevision {
				lastRevision = put.Header.Revision
			}
			return putErr
		})
		items = append(items, item)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, item := range items {
			_, _ = cli.Revoke(cleanupCtx, item.id)
		}
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(lastRevision+1), clientv3.WithPrevKV())
	keyToItem := make(map[string]batchLease, len(items))
	for _, item := range items {
		keyToItem[item.key] = item
	}

	time.Sleep(time.Duration(leaseTTLSeconds) * time.Second / 2)
	for _, item := range items {
		if !item.renewed {
			continue
		}
		item := item
		recordRPC(item.slot, leaseBatchInput{kind: leaseBatchKeepAlive, slot: item.slot}, leaseBatchOutput{}, func() error {
			keepAlive, keepAliveErr := cli.KeepAliveOnce(ctx, item.id)
			if keepAliveErr != nil {
				return keepAliveErr
			}
			if keepAlive == nil || keepAlive.TTL <= 0 {
				return fmt.Errorf("lease %d returned invalid keepalive response: %#v", item.id, keepAlive)
			}
			return nil
		})
	}

	renewCtx, stopRenewal := context.WithCancel(ctx)
	defer stopRenewal()
	renewErrs := make(chan error, 1)
	var renewer sync.WaitGroup
	renewer.Add(1)
	go func() {
		defer renewer.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
			}
			for _, item := range items {
				if !item.renewed {
					continue
				}
				call := clock.Add(1)
				keepAlive, keepAliveErr := cli.KeepAliveOnce(renewCtx, item.id)
				returned := clock.Add(1)
				if keepAliveErr != nil {
					if renewCtx.Err() != nil {
						return
					}
					select {
					case renewErrs <- fmt.Errorf("keepalive lease %d: %w", item.id, keepAliveErr):
					default:
					}
					return
				}
				if keepAlive == nil || keepAlive.TTL <= 0 {
					select {
					case renewErrs <- fmt.Errorf("lease %d returned invalid keepalive response: %#v", item.id, keepAlive):
					default:
					}
					return
				}
				appendOperation(item.slot, leaseBatchInput{kind: leaseBatchKeepAlive, slot: item.slot}, call, leaseBatchOutput{}, returned)
			}
		}
	}()

	expiryCalls := make(map[int]int64, leaseCount/2)
	for _, item := range items {
		if !item.renewed {
			expiryCalls[item.slot] = clock.Add(1)
		}
	}
	expiredSlots := make(map[int]struct{}, len(expiryCalls))
	expiryDeadline := time.NewTimer(time.Duration(leaseTTLSeconds+6) * time.Second)
	defer expiryDeadline.Stop()
	for len(expiredSlots) < len(expiryCalls) {
		select {
		case renewErr := <-renewErrs:
			require.NoError(t, renewErr)
		case response, ok := <-watch:
			require.True(t, ok, "watch closed before unrenewed leases expired")
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				item, ok := keyToItem[string(event.Kv.Key)]
				require.Truef(t, ok, "unexpected delete event for %q", event.Kv.Key)
				if item.renewed {
					expireCall := clock.Add(1)
					appendOperation(item.slot, leaseBatchInput{kind: leaseBatchOriginalExpire, slot: item.slot},
						expireCall, leaseBatchOutput{}, clock.Add(1))
					t.Fatalf("renewed lease %d expired at its original deadline", item.id)
				}
				if _, seen := expiredSlots[item.slot]; seen {
					continue
				}
				expiredSlots[item.slot] = struct{}{}
				appendOperation(item.slot, leaseBatchInput{kind: leaseBatchOriginalExpire, slot: item.slot},
					expiryCalls[item.slot], leaseBatchOutput{}, clock.Add(1))
			}
		case <-expiryDeadline.C:
			t.Fatalf("timed out waiting for %d unrenewed leases to expire; saw %d", len(expiryCalls), len(expiredSlots))
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for unrenewed leases to expire: %v", ctx.Err())
		}
	}

	for _, item := range items {
		ttlCall := clock.Add(1)
		ttl, ttlErr := cli.TimeToLive(ctx, item.id, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		ttlAlive := ttl.TTL >= 0
		appendOperation(item.slot, leaseBatchInput{kind: leaseBatchTimeToLive, slot: item.slot}, ttlCall,
			leaseBatchOutput{alive: ttlAlive}, clock.Add(1))

		readCall := clock.Add(1)
		got, getErr := cli.Get(ctx, item.key)
		require.NoError(t, getErr)
		output := leaseBatchOutput{}
		if len(got.Kvs) > 0 {
			require.Len(t, got.Kvs, 1)
			require.Equal(t, int64(item.id), got.Kvs[0].Lease)
			value, parseErr := strconv.Atoi(string(got.Kvs[0].Value))
			require.NoError(t, parseErr)
			output.present, output.value = true, value
		}
		appendOperation(item.slot, leaseBatchInput{kind: leaseBatchRead, slot: item.slot}, readCall, output, clock.Add(1))

		if item.renewed {
			require.Positive(t, ttl.TTL, "renewed lease %d must remain alive", item.id)
			require.Equal(t, [][]byte{[]byte(item.key)}, ttl.Keys)
			require.True(t, output.present)
			require.Equal(t, item.value, output.value)
			continue
		}
		require.Equal(t, int64(-1), ttl.TTL, "unrenewed lease %d must be expired", item.id)
		require.Empty(t, ttl.Keys)
		require.False(t, output.present)
	}

	require.Eventually(t, func() bool {
		leases, listErr := cli.Leases(ctx)
		if listErr != nil {
			return false
		}
		listed := make(map[clientv3.LeaseID]struct{}, len(leases.Leases))
		for _, lease := range leases.Leases {
			listed[lease.ID] = struct{}{}
		}
		for _, item := range items {
			_, ok := listed[item.id]
			if ok != item.renewed {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	stopRenewal()
	renewer.Wait()
	select {
	case renewErr := <-renewErrs:
		require.NoError(t, renewErr)
	default:
	}

	result := porcupine.CheckOperationsTimeout(leaseBatchRenewalModel, history, 10*time.Second)
	if result != porcupine.Ok {
		sort.Slice(history, func(i, j int) bool { return history[i].Call < history[j].Call })
		for _, operation := range history {
			t.Logf("client=%d call=%d return=%d %s", operation.ClientId, operation.Call, operation.Return,
				leaseBatchRenewalModel.DescribeOperation(operation.Input, operation.Output))
		}
	}
	require.Equalf(t, porcupine.Ok, result, "lease batch renewal history result: %s", result)
}
