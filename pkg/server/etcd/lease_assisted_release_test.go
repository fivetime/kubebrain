package etcd

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type leaseHandoffStorage struct{ storage.KvStorage }

func (leaseHandoffStorage) ClusterID() uint64                    { return 42 }
func (s leaseHandoffStorage) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }

// Two independent backend/lease-manager instances share storage, but no memory
// lease deadlines. This tests the storage handoff/recovery semantics, not TLS,
// actual network partitions, or the original thirty-second availability gate.
func TestAcknowledgedLeaseSurvivesAssistedReleaseAndNewBackend(t *testing.T) {
	for _, checkpointed := range []bool{false, true} {
		name := "memory-only-renewal"
		if checkpointed {
			name = "checkpoint-clearing-renewal"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			kv := leaseHandoffStorage{memkv.NewKvStorage()}
			metrics := metricmock.NewMinimalMetrics(gomock.NewController(t))
			makeBackend := func(identity string) backend.Backend {
				b := backend.NewBackend(kv, backend.Config{Prefix: "/lease-handoff", Keyspace: "handoff", Identity: identity, EnableEtcdCompatibility: true}, metrics)
				t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
				return b
			}
			oldBackend, nextBackend := makeBackend("old"), makeBackend("next")
			oldLock, nextLock := oldBackend.GetResourceLock(), nextBackend.GetResourceLock()
			record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 1}
			require.NoError(t, oldLock.Create(ctx, record))
			condition, ok := oldLock.(election.OwnershipConditionProvider).OwnershipConditionFor(record)
			require.True(t, ok)
			var oldFresh atomic.Bool
			oldFresh.Store(true)
			oldBackend.SetLeadershipFence(func() (uint64, bool) { return 1, oldFresh.Load() })
			old := New(oldBackend, metrics, testPeerService{isLeaderFn: oldFresh.Load, epochFn: func() (uint64, bool) { return 1, oldFresh.Load() }})
			t.Cleanup(func() { require.NoError(t, old.Close()); old.backend.(*backendShim).Close() })
			const id int64 = 889351
			const grantedTTL int64 = 600
			key := []byte("/registry/assisted-lease")
			_, err := old.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: grantedTTL})
			require.NoError(t, err)
			_, err = old.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("preserve"), Lease: id})
			require.NoError(t, err)
			if checkpointed {
				func() {
					old.leaseMu.Lock()
					defer old.leaseMu.Unlock()
					state := old.leases[id]
					require.NotNil(t, state)
					if state.timer != nil {
						state.timer.Stop()
					}
					if state.checkpointTimer != nil {
						state.checkpointTimer.Stop()
					}
					state.deadline = time.Now().Add(60 * time.Second)
				}()
				old.checkpointLease(id)
				data, err := old.backend.InternalGet(ctx, leaseStorageKey(id))
				require.NoError(t, err)
				var saved leaseRecord
				require.NoError(t, json.Unmarshal(data, &saved))
				require.Positive(t, saved.RemainingTTL)
				require.Less(t, saved.RemainingTTL, grantedTTL)
			}
			type ackObservation struct {
				record leaseRecord
				err    error
			}
			atAck := make(chan ackObservation, 1)
			stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: id}}, onSend: func() {
				data, err := old.backend.InternalGet(ctx, leaseStorageKey(id))
				var record leaseRecord
				if err == nil {
					err = json.Unmarshal(data, &record)
				}
				atAck <- ackObservation{record, err}
			}}
			require.NoError(t, old.LeaseKeepAlive(stream))
			require.Len(t, stream.sent, 1)
			require.Equal(t, grantedTTL, stream.sent[0].TTL, "only proceed after a successful public acknowledgement")
			ack := <-atAck
			require.NoError(t, ack.err)
			require.Zero(t, ack.record.RemainingTTL, "checkpoint must be durably cleared by the response Send boundary")
			old.leaseMu.Lock()
			acknowledgedDeadline := old.leases[id].deadline
			old.leaseMu.Unlock()
			oldFresh.Store(false)
			old.StopLeases() // Production post-join callback must follow this barrier.
			releaser := nextLock.(election.RetiredOwnershipReleaser)
			require.NoError(t, releaser.ReleaseRetiredOwnership(ctx, releaser.RetirementScope(), condition))
			released, _, err := nextLock.Get(ctx)
			require.NoError(t, err)
			require.Empty(t, released.HolderIdentity)
			released.HolderIdentity = "next"
			released.LeaseDurationSeconds = 30
			released.LeaderTransitions++
			require.NoError(t, nextLock.Update(ctx, *released))
			nextBackend.SetLeadershipFence(func() (uint64, bool) { return 2, true })
			require.NoError(t, nextBackend.InitializeLeadershipRevision(ctx, 2))
			next := New(nextBackend, metrics, testPeerService{isLeader: true, epochFn: func() (uint64, bool) { return 2, true }})
			t.Cleanup(func() { require.NoError(t, next.Close()); next.backend.(*backendShim).Close() })
			next.SetLeasePromotionExtension(30 * time.Second)
			require.NoError(t, next.ReloadLeases(ctx))
			next.leaseMu.Lock()
			recovered := next.leases[id]
			var deadline time.Time
			if recovered != nil {
				deadline = recovered.deadline
			}
			next.leaseMu.Unlock()
			require.NotNil(t, recovered)
			require.False(t, deadline.Before(acknowledgedDeadline), "handoff cannot shorten the acknowledged deadline")
			ttl, err := next.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
			require.NoError(t, err)
			require.Positive(t, ttl.TTL)
			require.Equal(t, grantedTTL, ttl.GrantedTTL)
			require.Contains(t, ttl.Keys, key)
			response, err := next.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Len(t, response.Kvs, 1)
			require.Equal(t, id, response.Kvs[0].Lease)
			_, err = old.refreshLease(ctx, id)
			require.ErrorIs(t, err, errLeaseDemotedDuringRenew)
		})
	}
}
