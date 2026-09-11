package tikv

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
)

func protocolSmokePrefixEmpty(ctx context.Context, kv storage.KvStorage, prefix string) error {
	end := []byte(strings.TrimSuffix(prefix, "/") + "0")
	iter, err := kv.Iter(ctx, []byte(prefix), end, 0, 1)
	if err != nil {
		return err
	}
	err = iter.Next(ctx)
	closeErr := iter.Close()
	if errors.Is(err, io.EOF) {
		return closeErr
	}
	if err == nil {
		err = fmt.Errorf("test prefix is not empty")
	}
	return errors.Join(err, closeErr)
}

func cleanupProtocolSmoke(ctx context.Context, kv storage.KvStorage, prefix string, owner []byte) error {
	if _, err := validateProtocolSmokeScope("1", prefix, "2pc"); err != nil {
		return err
	}
	if len(owner) != 32 {
		return fmt.Errorf("invalid owner token")
	}
	claim := []byte(prefix + "owner")
	got, err := kv.Get(ctx, claim)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return protocolSmokePrefixEmpty(ctx, kv, prefix)
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(owner, got) {
		return fmt.Errorf("ownership changed: refuse cleanup")
	}
	batch := kv.BeginBatchWrite()
	batch.CAS(claim, owner, owner, 0)
	batch.Del([]byte(prefix + "data"))
	batch.Del([]byte(prefix + "witness"))
	batch.Del(claim)
	if err := batch.Commit(ctx); err != nil {
		return err
	}
	return protocolSmokePrefixEmpty(ctx, kv, prefix)
}

type protocolCleanupRaceStore struct {
	storage.KvStorage
	afterGet func() error
}

func (s *protocolCleanupRaceStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := s.KvStorage.Get(ctx, key)
	if err == nil && s.afterGet != nil {
		hook := s.afterGet
		s.afterGet = nil
		if err := hook(); err != nil {
			return nil, err
		}
	}
	return value, err
}

func TestProtocolSmokeCleanupOwnership(t *testing.T) {
	for _, mode := range []string{"owned", "absent", "foreign", "raced", "orphan", "invalid-scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
			owner, foreign := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
			if mode != "absent" {
				batch := kv.BeginBatchWrite()
				if mode != "orphan" {
					token := owner
					if mode == "foreign" {
						token = foreign
					}
					batch.Put([]byte(prefix+"owner"), token, 0)
				}
				batch.Put([]byte(prefix+"data"), []byte("keep"), 0)
				batch.Put([]byte(prefix+"witness"), []byte("keep"), 0)
				require.NoError(t, batch.Commit(ctx))
			}
			store := &protocolCleanupRaceStore{KvStorage: kv}
			if mode == "raced" {
				store.afterGet = func() error {
					batch := kv.BeginBatchWrite()
					batch.Put([]byte(prefix+"owner"), foreign, 0)
					return batch.Commit(ctx)
				}
			}
			cleanupPrefix := prefix
			if mode == "invalid-scope" {
				cleanupPrefix = ""
			}
			err := cleanupProtocolSmoke(ctx, store, cleanupPrefix, owner)
			if mode == "owned" || mode == "absent" {
				require.NoError(t, err)
				require.NoError(t, cleanupProtocolSmoke(ctx, store, prefix, owner), "cleanup is retry-safe")
				return
			}
			require.Error(t, err)
			if mode == "raced" {
				require.ErrorIs(t, err, storage.ErrCASFailed)
			}
			for _, suffix := range []string{"data", "witness"} {
				value, err := kv.Get(ctx, []byte(prefix+suffix))
				require.NoError(t, err)
				require.Equal(t, []byte("keep"), value, "refused cleanup must preserve data")
			}
		})
	}
}

func validateProtocolSmokeScope(cluster, prefix, mode string) (uint64, error) {
	id, err := strconv.ParseUint(cluster, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != cluster {
		return 0, fmt.Errorf("explicit canonical nonzero cluster ID required")
	}
	if !regexp.MustCompile(`^kubebrain/protocol-smoke/[a-f0-9]{32}/$`).MatchString(prefix) {
		return 0, fmt.Errorf("dedicated 32-hex protocol-smoke prefix required")
	}
	if mode != "1pc" && mode != "2pc" {
		return 0, fmt.Errorf("explicit 1pc or 2pc mode required")
	}
	return id, nil
}

func TestProtocolSmokeScopeValidation(t *testing.T) {
	prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
	for _, mode := range []string{"1pc", "2pc"} {
		id, err := validateProtocolSmokeScope("123", prefix, mode)
		require.NoError(t, err)
		require.EqualValues(t, 123, id)
	}
	for _, tc := range [][3]string{
		{"", prefix, "1pc"}, {"0", prefix, "1pc"}, {"0123", prefix, "1pc"},
		{"123", "", "1pc"}, {"123", "/registry/", "1pc"},
		{"123", prefix, ""}, {"123", prefix, "async"},
	} {
		_, err := validateProtocolSmokeScope(tc[0], tc[1], tc[2])
		require.Error(t, err)
	}
}

// Opt-in real storage smoke. Run alone in a dedicated test process: protocol
// counters are process-global. This is not a Raft failure/durability benchmark
// or proof of KubeBrain's backend uncertain-result resolver.
func TestRealTiKVProtocolSmoke(t *testing.T) {
	testRealTiKVProtocolSmoke(t, false)
}

func testRealTiKVProtocolSmoke(t *testing.T, dropResponse bool) {
	t.Helper()
	pd := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PD")
	if pd == "" {
		t.Skip("explicit protocol PD endpoint required")
	}
	prefix, mode := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PREFIX"), os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE")
	expected, err := validateProtocolSmokeScope(os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID"), prefix, mode)
	require.NoError(t, err) // Validate mutation scope before opening a client.
	if dropResponse {
		require.Equal(t, "1pc", mode, "response loss requires explicit 1pc mode")
	}
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) {
		cfg.Enable1PC = mode == "1pc"
		cfg.EnableAsyncCommit = false
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	kv, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	require.Equal(t, expected, kv.(storage.ClusterIdentifier).ClusterID(), "wrong cluster: no writes allowed")
	var loss *protocolResponseLoss
	if dropResponse {
		client := kv.(*store).getClient()
		loss = &protocolResponseLoss{Client: client.GetTiKVClient()}
		client.SetTiKVClient(loss)
	}
	require.NoError(t, protocolSmokePrefixEmpty(ctx, kv, prefix))
	claim, data, witness := []byte(prefix+"owner"), []byte(prefix+"data"), []byte(prefix+"witness")
	// A fresh owner token also fences accidentally concurrent invocations using
	// the same prefix: a losing claim must never delete the winner's data.
	owner := make([]byte, 32)
	_, err = rand.Read(owner)
	require.NoError(t, err)
	t.Logf("PROTOCOL_FIXTURE_STARTED cluster=%d prefix=%s owner_sha256=%x", expected, prefix, sha256.Sum256(owner))
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		require.NoError(t, cleanupProtocolSmoke(cleanupCtx, kv, prefix, owner))
		t.Logf("PROTOCOL_FIXTURE_CLEANUP_OK prefix=%s", prefix)
	})
	initial := kv.BeginBatchWrite()
	initial.PutIfNotExist(claim, owner, 0)
	initial.Put(data, []byte("before"), 0)
	initial.Put(witness, []byte("before"), 0)
	before := tikvmetrics.GetTxnCommitCounter()
	started := time.Now()
	commitCtx := ctx
	if dropResponse {
		commitCtx = context.WithValue(ctx, protocolCommitMarker{}, true)
	}
	commitErr := initial.Commit(commitCtx)
	elapsed := time.Since(started)
	delta := tikvmetrics.GetTxnCommitCounter().Sub(before)
	if dropResponse {
		require.True(t, commitErr == nil || errors.Is(commitErr, storage.ErrUncertainResult), "committed response loss must not become a definite failure: %v", commitErr)
		stats := loss.snapshot()
		require.Equal(t, 1, stats.Drops, "must drop an actual successful 1PC response")
		require.Positive(t, stats.Attempts)
		require.Greater(t, stats.CommitTS, stats.StartTS)
		require.False(t, stats.Changed, "retry must preserve transaction identity and commit timestamp")
		t.Logf("PROTOCOL_RESPONSE_LOSS_CONFIRMED uncertain=%t attempts=%d drops=%d start_ts=%d commit_ts=%d", errors.Is(commitErr, storage.ErrUncertainResult), stats.Attempts, stats.Drops, stats.StartTS, stats.CommitTS)
	} else if mode == "1pc" {
		require.NoError(t, commitErr)
		require.Equal(t, tikvmetrics.TxnCommitCounter{OnePC: 1}, delta, "must actually commit with 1PC")
	} else {
		require.NoError(t, commitErr)
		require.Equal(t, tikvmetrics.TxnCommitCounter{TwoPC: 1}, delta)
	}
	ts, err := kv.GetTimestampOracle(ctx)
	require.NoError(t, err)
	for _, key := range [][]byte{data, witness} {
		value, err := kv.(storage.SnapshotGetter).GetAt(ctx, key, ts)
		require.NoError(t, err)
		require.Equal(t, []byte("before"), value, "both keys must be visible after the committed response loss")
	}
	update := kv.BeginBatchWrite()
	update.CAS(claim, owner, owner, 0)
	update.Put(data, []byte("after"), 0)
	update.Put(witness, []byte("after"), 0)
	require.NoError(t, update.Commit(ctx))
	for _, key := range [][]byte{data, witness} {
		old, err := kv.(storage.SnapshotGetter).GetAt(ctx, key, ts)
		require.NoError(t, err)
		require.Equal(t, []byte("before"), old)
		current, err := kv.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, []byte("after"), current)
	}
	t.Logf("PROTOCOL_SMOKE_OK cluster=%d prefix=%s mode=%s commit=%s counts=%+v scope=storage_only", expected, prefix, mode, elapsed, delta)
}
