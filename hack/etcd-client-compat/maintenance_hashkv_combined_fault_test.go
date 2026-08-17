package compat

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestFixedRevisionHashKVSurvivesPDQuorumFault extends upstream's
// member-local Maintenance.HashKV contract to KubeBrain's shared TiKV history.
// Once a positive logical revision is covered by a protected local checkpoint,
// hashing it must not require a fresh PD TSO. Latest HashKV remains free to fail
// closed while the shared coordination path is unavailable.
func TestFixedRevisionHashKVSurvivesPDQuorumFault(t *testing.T) {
	runFixedRevisionHashKVFault(t, "KUBEBRAIN_HASHKV_PD_QUORUM_FAULT_COMMAND", "PD quorum fault")
}

// TestFixedRevisionHashKVSurvivesCombinedBackendFault verifies that a warmed,
// immutable checkpoint can route both Maintenance.HashKV and the serializable
// Range it exists to serve around one blackholed TiKV replica while PD has no
// quorum to refresh Region metadata.
func TestFixedRevisionHashKVSurvivesCombinedBackendFault(t *testing.T) {
	runFixedRevisionHashKVFault(t, "KUBEBRAIN_HASHKV_COMBINED_FAULT_COMMAND", "combined backend fault")
}

func runFixedRevisionHashKVFault(t *testing.T, commandEnv, faultLabel string) {
	t.Helper()
	command := os.Getenv(commandEnv)
	if command == "" {
		t.Skipf("set %s to run destructive %s", commandEnv, faultLabel)
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatalf("set KUBEBRAIN_ETCD_ENDPOINT explicitly for %s HashKV", faultLabel)
	}
	hashCallTimeout := 10 * time.Second
	if configured := os.Getenv("KUBEBRAIN_HASHKV_CALL_TIMEOUT"); configured != "" {
		parsed, parseErr := time.ParseDuration(configured)
		require.NoError(t, parseErr, "parse KUBEBRAIN_HASHKV_CALL_TIMEOUT")
		require.Positive(t, parsed, "KUBEBRAIN_HASHKV_CALL_TIMEOUT must be positive")
		hashCallTimeout = parsed
	}
	checkpointWait := 60 * time.Second
	if configured := os.Getenv("KUBEBRAIN_HASHKV_CHECKPOINT_WAIT"); configured != "" {
		parsed, parseErr := time.ParseDuration(configured)
		require.NoError(t, parseErr, "parse KUBEBRAIN_HASHKV_CHECKPOINT_WAIT")
		require.Positive(t, parsed, "KUBEBRAIN_HASHKV_CHECKPOINT_WAIT must be positive")
		checkpointWait = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkpointWait+2*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	prefix := fmt.Sprintf("/compat/hashkv-combined/%d/", time.Now().UnixNano())
	t.Cleanup(func() {
		var cleanupErr error
		cleaned := assert.Eventually(t, func() bool {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanupCancel()
			_, cleanupErr = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
			return cleanupErr == nil
		}, 30*time.Second, 200*time.Millisecond, "delete HashKV fault fixture after backend recovery: %v", cleanupErr)
		if !cleaned {
			t.Logf("manual cleanup required for %q", prefix)
		}
	})

	put, err := client.Put(ctx, prefix+"baseline", "value")
	require.NoError(t, err)
	revision := put.Header.Revision
	rangePrefix := prefix + "range/"
	const rangeKeys = 64
	values := make([]string, rangeKeys)
	ops := make([]clientv3.Op, 0, rangeKeys/2)
	for index := 0; index < rangeKeys; index++ {
		key := fmt.Sprintf("%s%03d", rangePrefix, index)
		// The aggregate is deliberately larger than the default 1.5 MiB
		// RangeStream frame target. Seed in two transactions so request admission
		// remains below that limit while the fault-time LIST must cross a real
		// successful Send boundary.
		value := fmt.Sprintf("checkpoint-value-%03d-%s", index, strings.Repeat("x", 28*1024))
		values[index] = value
		ops = append(ops, clientv3.OpPut(key, value))
		if len(ops) != cap(ops) {
			continue
		}
		seeded, seedErr := client.Txn(ctx).Then(ops...).Commit()
		require.NoError(t, seedErr)
		require.True(t, seeded.Succeeded)
		revision = seeded.Header.Revision
		ops = ops[:0]
	}
	// TiKV's store-wide safe timestamp intentionally trails the latest TSO. Let
	// the committed revision enter every Store's common safe-time window before
	// testing the contract that applies only to a covered historical revision.
	select {
	case <-time.After(checkpointWait):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	baseline, err := client.HashKV(ctx, endpoint, revision)
	require.NoError(t, err)
	require.Equal(t, revision, baseline.HashRevision)
	baselineRange, err := client.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, baselineRange.Kvs, rangeKeys)

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	combinedReadyPath := filepath.Join(t.TempDir(), "combined-fault-ready")
	requireCombinedReady := strings.Contains(command, "KUBEBRAIN_COMBINED_FAULT_READY_FILE")
	if requireCombinedReady {
		t.Setenv("KUBEBRAIN_COMBINED_FAULT_READY_FILE", combinedReadyPath)
	}
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	if requireCombinedReady {
		require.Eventually(t, func() bool {
			_, statErr := os.Stat(combinedReadyPath)
			return statErr == nil
		}, 3*time.Minute, 100*time.Millisecond, "combined backend fault must signal both quorums ready")
	}

	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer callCancel()
		_, putErr := client.Put(callCtx, prefix+"fault-probe", fmt.Sprintf("%d", time.Now().UnixNano()))
		return putErr != nil && isMutationFailoverAmbiguous(putErr)
	}, 15*time.Second, 25*time.Millisecond, "PD quorum fault must expose a mutation-unavailable window")

	streamCtx, streamCancel := context.WithTimeout(ctx, hashCallTimeout)
	rawStream, streamErr := etcdserverpb.NewKVClient(client.ActiveConnection()).RangeStream(
		streamCtx,
		&etcdserverpb.RangeRequest{
			Key: []byte(rangePrefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(rangePrefix)), Serializable: true,
		},
	)
	var streamFrames []*etcdserverpb.RangeStreamResponse
	if streamErr == nil {
		for {
			frame, recvErr := rawStream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				streamErr = recvErr
				break
			}
			streamFrames = append(streamFrames, frame)
		}
	}
	streamCancel()
	callCtx, callCancel := context.WithTimeout(ctx, hashCallTimeout)
	duringFault, hashErr := client.HashKV(callCtx, endpoint, revision)
	callCancel()
	rangeCtx, rangeCancel := context.WithTimeout(ctx, hashCallTimeout)
	duringRange, rangeErr := client.Get(
		rangeCtx, rangePrefix, clientv3.WithPrefix(), clientv3.WithSerializable(),
	)
	rangeCancel()
	txnCtx, txnCancel := context.WithTimeout(ctx, hashCallTimeout)
	duringTxn, txnErr := client.Txn(txnCtx).
		If(clientv3.Compare(clientv3.Value(rangePrefix+"000"), "=", values[0])).
		Then(clientv3.OpGet(rangePrefix+"001", clientv3.WithSerializable())).
		Else(clientv3.OpGet(rangePrefix+"missing", clientv3.WithSerializable())).
		Commit()
	txnCancel()
	var result commandResult
	endedBeforeHashValidation := false
	select {
	case result = <-commandDone:
		endedBeforeHashValidation = true
	default:
	}
	if !endedBeforeHashValidation {
		// Always let the destructive helper reach its cleanup trap before asserting
		// the RPC result. A RED must not strand network-partition rules in the Kind
		// node and poison subsequent diagnostics.
		result = <-commandDone
	}
	require.NoErrorf(t, result.err, "%s command: %s", faultLabel, strings.TrimSpace(string(result.output)))
	t.Logf("%s command: %s", faultLabel, strings.TrimSpace(string(result.output)))
	require.Falsef(t, endedBeforeHashValidation,
		"combined fault ended before fixed-revision HashKV was validated: %s",
		strings.TrimSpace(string(result.output)))
	require.NoError(t, hashErr, "fixed-revision HashKV must use the protected member checkpoint")
	require.NotNil(t, duringFault)
	require.Equal(t, baseline.Hash, duringFault.Hash)
	require.Equal(t, baseline.HashRevision, duringFault.HashRevision)
	require.Equal(t, baseline.CompactRevision, duringFault.CompactRevision)
	require.NoError(t, streamErr, "serializable RangeStream must fall back before its first live frame")
	require.GreaterOrEqual(t, len(streamFrames), 2, "fault-time LIST must exercise a successful non-terminal frame")
	streamedKVs := make([]*etcdserverpb.RangeResponse, 0, len(streamFrames))
	var concatenatedKVs = baselineRange.Kvs[:0:0]
	for index, frame := range streamFrames {
		response := frame.GetRangeResponse()
		require.NotNil(t, response)
		streamedKVs = append(streamedKVs, response)
		concatenatedKVs = append(concatenatedKVs, response.Kvs...)
		if index < len(streamFrames)-1 {
			require.Nil(t, response.Header, "only the terminal RangeStream frame may carry metadata")
			require.Zero(t, response.Count)
		}
	}
	terminal := streamedKVs[len(streamedKVs)-1]
	require.NotNil(t, terminal.Header)
	require.GreaterOrEqual(t, terminal.Header.Revision, revision)
	require.EqualValues(t, rangeKeys, terminal.Count)
	require.False(t, terminal.More)
	require.Equal(t, baselineRange.Kvs, concatenatedKVs)
	require.NoError(t, rangeErr, "serializable Range must use the protected member checkpoint")
	require.NotNil(t, duringRange)
	require.Equal(t, baselineRange.Kvs, duringRange.Kvs)
	require.Equal(t, baselineRange.Count, duringRange.Count)
	require.GreaterOrEqual(t, duringRange.Header.Revision, revision)
	require.NoError(t, txnErr, "serializable read-only Txn must use the protected member checkpoint")
	require.NotNil(t, duringTxn)
	require.True(t, duringTxn.Succeeded)
	require.Len(t, duringTxn.Responses, 1)
	txnRange := duringTxn.Responses[0].GetResponseRange()
	require.NotNil(t, txnRange)
	require.Len(t, txnRange.Kvs, 1)
	require.Equal(t, rangePrefix+"001", string(txnRange.Kvs[0].Key))
	require.Equal(t, values[1], string(txnRange.Kvs[0].Value))
	require.GreaterOrEqual(t, duringTxn.Header.Revision, revision)
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		defer callCancel()
		latest, latestErr := client.HashKV(callCtx, endpoint, 0)
		return latestErr == nil && latest != nil && latest.HashRevision >= revision
	}, 45*time.Second, 100*time.Millisecond, "latest HashKV must recover after the backend fault")
}
