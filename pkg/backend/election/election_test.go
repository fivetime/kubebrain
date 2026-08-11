// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type postCommitTSOFailureStorage struct {
	storage.KvStorage
	failTSO bool
}

func (s *postCommitTSOFailureStorage) GetTimestampOracle(ctx context.Context) (uint64, error) {
	if s.failTSO {
		return 0, errors.New("PD unavailable after commit")
	}
	return s.KvStorage.GetTimestampOracle(ctx)
}

func TestResourceLockRejectsMalformedElectionMetadata(t *testing.T) {
	base, err := json.Marshal(resourcelock.LeaderElectionRecord{
		HolderIdentity:       "peer-a",
		LeaseDurationSeconds: 10,
		LeaderTransitions:    1,
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		raw  []byte
		want string
	}{
		{
			name: "unknown field",
			raw:  append(base[:len(base)-1:len(base)-1], []byte(`,"unexpected":true}`)...),
			want: `json: unknown field "unexpected"`,
		},
		{
			name: "trailing json",
			raw:  append(append([]byte(nil), base...), []byte(` {}`)...),
			want: "leader election record contains trailing JSON",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			prefix := "/registry/kubebrain"
			batch := kv.BeginBatchWrite()
			batch.Put(getElectionKey(prefix), tc.raw, 0)
			require.NoError(t, batch.Commit(context.Background()))

			lock := NewResourceLockManager(Config{
				Prefix:   prefix,
				Identity: "peer-b",
				Timeout:  time.Second,
			}, kv).GetResourceLock()
			_, _, err := lock.Get(context.Background())
			require.EqualError(t, err, tc.want)
		})
	}
}

func TestResourceLockRotatesShardedStorageFenceOnlyOnProcessOwnership(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	const prefix = "/registry/fence-token"
	ctx := context.Background()

	managerA := NewResourceLockManager(Config{Prefix: prefix, Identity: "peer-a", Timeout: time.Second}, kv)
	lockA := managerA.GetResourceLock()
	record := resourcelock.LeaderElectionRecord{
		HolderIdentity:       "peer-a",
		LeaseDurationSeconds: 8,
		LeaderTransitions:    1,
	}
	require.NoError(t, lockA.Create(ctx, record))
	providerA := lockA.(StorageFenceTokenProvider)
	keyA0, tokenA, ok := providerA.StorageFenceToken(0)
	require.True(t, ok)
	keyA255, tokenA255, ok := providerA.StorageFenceToken(storageFenceShardCount - 1)
	require.True(t, ok)
	require.Equal(t, tokenA, tokenA255)
	require.NotEqual(t, keyA0, keyA255)
	stored, err := kv.Get(ctx, keyA0)
	require.NoError(t, err)
	require.Equal(t, tokenA, stored)

	// Ordinary renewals retain the process token, avoiding false conflicts for
	// user transactions that overlap the one-second election renew cadence.
	record.RenewTime = metav1.NewTime(time.Now())
	require.NoError(t, lockA.Update(ctx, record))
	_, tokenAfterRenew, ok := providerA.StorageFenceToken(0)
	require.True(t, ok)
	require.Equal(t, tokenA, tokenAfterRenew)

	// The same running process may lose and reacquire leadership without a Get
	// that observes the intermediate holder. A new shared term must rotate its
	// stale cached token; otherwise every subsequent write self-fences forever.
	record.LeaderTransitions++
	require.NoError(t, lockA.Update(ctx, record))
	_, tokenAReacquired, ok := providerA.StorageFenceToken(0)
	require.True(t, ok)
	require.NotEqual(t, tokenA, tokenAReacquired)
	stored, err = kv.Get(ctx, keyA0)
	require.NoError(t, err)
	require.Equal(t, tokenAReacquired, stored)

	managerB := NewResourceLockManager(Config{Prefix: prefix, Identity: "peer-b", Timeout: time.Second}, kv)
	lockB := managerB.GetResourceLock()
	_, _, err = lockB.Get(ctx)
	require.NoError(t, err)
	record.HolderIdentity = "peer-b"
	record.LeaderTransitions++
	require.NoError(t, lockB.Update(ctx, record))
	_, tokenB, ok := lockB.(StorageFenceTokenProvider).StorageFenceToken(0)
	require.True(t, ok)
	require.NotEqual(t, tokenA, tokenB)
	stored, err = kv.Get(ctx, keyA0)
	require.NoError(t, err)
	require.Equal(t, tokenB, stored)

	// A restarted process with the same stable peer identity must still rotate
	// ownership; client-go otherwise sees the stored holder as itself.
	managerBRestart := NewResourceLockManager(Config{Prefix: prefix, Identity: "peer-b", Timeout: time.Second}, kv)
	lockBRestart := managerBRestart.GetResourceLock()
	restartedRecord, _, err := lockBRestart.Get(ctx)
	require.NoError(t, err)
	require.NoError(t, lockBRestart.Update(ctx, *restartedRecord))
	_, tokenBRestart, ok := lockBRestart.(StorageFenceTokenProvider).StorageFenceToken(0)
	require.True(t, ok)
	require.NotEqual(t, tokenB, tokenBRestart)
}

func TestResourceLockUpdateReconcilesCommittedRecordAfterPostCommitTSOFailure(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	store := &postCommitTSOFailureStorage{KvStorage: kv}
	const prefix = "/registry/post-commit-tso-failure"
	ctx := context.Background()
	lock := NewResourceLockManager(Config{Prefix: prefix, Identity: "peer-a", Timeout: time.Second}, store).GetResourceLock()
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "peer-a", LeaseDurationSeconds: 8}
	require.NoError(t, lock.Create(ctx, record))

	firstRenew := metav1.NewTime(time.Unix(100, 0))
	record.RenewTime = firstRenew
	store.failTSO = true
	require.ErrorContains(t, lock.Update(ctx, record), "PD unavailable after commit")

	// The transaction above committed. A later renewal must use its value as
	// the CAS predecessor instead of becoming permanently wedged on stale state.
	store.failTSO = false
	secondRenew := metav1.NewTime(time.Unix(200, 0))
	record.RenewTime = secondRenew
	require.NoError(t, lock.Update(ctx, record))
	stored, _, err := lock.Get(ctx)
	require.NoError(t, err)
	require.True(t, stored.RenewTime.Equal(&secondRenew))
}

func TestRestorationFenceStopsElectionRenewalAndRestart(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	const prefix = "/registry/restoration-fence"
	ctx := context.Background()
	manager := NewResourceLockManager(Config{Prefix: prefix, Identity: "peer-a", Timeout: time.Second}, kv)
	lock := manager.GetResourceLock()
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "peer-a", LeaseDurationSeconds: 8}
	require.NoError(t, lock.Create(ctx, record))
	provider := lock.(RestorationFenceTokenProvider)
	key, open, ok := provider.RestorationFenceControlToken()
	require.True(t, ok)
	require.Equal(t, []byte(restorationfence.Open), open)

	closed := []byte(`{"operation_id":"restore-1"}`)
	batch := kv.BeginBatchWrite()
	batch.CAS(key, closed, open, 0)
	require.NoError(t, batch.Commit(ctx))

	require.ErrorIs(t, lock.Update(ctx, record), storage.ErrCASFailed)
	restarted := NewResourceLockManager(Config{Prefix: prefix, Identity: "peer-b", Timeout: time.Second}, kv).GetResourceLock()
	_, _, err := restarted.Get(ctx)
	require.ErrorContains(t, err, "fenced for target restoration")
}
