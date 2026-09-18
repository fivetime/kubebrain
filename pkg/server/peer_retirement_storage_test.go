package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type retirementStorageFixture struct {
	storage.KvStorage
	id uint64
}

func (s retirementStorageFixture) ClusterID() uint64 { return s.id }

type retirementLostAckWriter struct{ http.ResponseWriter }

func (w retirementLostAckWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w retirementLostAckWriter) WriteHeader(status int) {
	if status == http.StatusNoContent {
		conn, _, err := w.ResponseWriter.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func TestPeerRetirementStorageRoundTripAndReacquisitionReplay(t *testing.T) {
	for _, dropAck := range []bool{false, true} {
		t.Run(fmt.Sprintf("dropAck=%v", dropAck), func(t *testing.T) {
			ctx := context.Background()
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			store := retirementStorageFixture{kv, 42}
			config := election.Config{Prefix: "/storage-retirement", Keyspace: "tenant", Identity: "old", Timeout: time.Second}
			old := election.NewResourceLockManager(config, store).GetResourceLock()
			config.Identity = "helper"
			helper := election.NewResourceLockManager(config, store).GetResourceLock()
			scope := helper.(election.RetiredOwnershipReleaser).RetirementScope()
			pool, certs := retirementTestCertificates(t)
			auth, err := newPeerRetirementAuthorizer(scope, map[string][]string{"old": {retirementTestPin(certs[0])}})
			require.NoError(t, err)
			h, err := newStoragePeerRetirementHandler(auth, helper, time.Second, time.Second, 8, 1000)
			require.NoError(t, err)
			// A copied policy cannot be attached to another tenant, prefix or PD
			// cluster even before its request body or transaction is considered.
			for _, mismatch := range []string{"cluster", "keyspace", "prefix", "no identifier"} {
				otherConfig, otherStore := config, store
				switch mismatch {
				case "cluster":
					otherStore.id++
				case "keyspace":
					otherConfig.Keyspace = "other"
				case "prefix":
					otherConfig.Prefix = "/other"
				case "no identifier":
					otherStore.id = 0
				}
				other := election.NewResourceLockManager(otherConfig, otherStore).GetResourceLock()
				_, err := newStoragePeerRetirementHandler(auth, other, time.Second, time.Second, 1, 1)
				require.Error(t, err, mismatch)
			}
			record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 7}
			require.NoError(t, old.Create(ctx, record))
			condition, ok := old.(election.OwnershipConditionProvider).OwnershipConditionFor(record)
			require.True(t, ok)
			tokens := make(map[string]string)
			for shard := uint64(0); shard < restorationfence.ShardCount; shard++ {
				key, token, ok := old.(election.StorageFenceTokenProvider).StorageFenceToken(shard)
				require.True(t, ok)
				tokens[string(key)] = string(token)
			}
			srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if dropAck {
					h.ServeHTTP(retirementLostAckWriter{w}, r)
				} else {
					h.ServeHTTP(w, r)
				}
			}), pool, certs, false)
			credentials := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
			sender, err := newPeerRetirementSender(scope, "old", []string{srv.URL}, credentials, time.Second)
			require.NoError(t, err)
			unauthorized := credentials.Clone()
			unauthorized.Certificates = []tls.Certificate{certs[1]}
			denied, err := newPeerRetirementSender(scope, "old", []string{srv.URL}, unauthorized, time.Second)
			require.NoError(t, err)
			require.ErrorIs(t, denied.send(ctx, condition), errPeerRetirementUnconfirmed)
			unchanged, _, err := helper.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, record, *unchanged)
			err = sender.send(ctx, condition)
			if dropAck {
				require.ErrorIs(t, err, errPeerRetirementUnconfirmed)
			} else {
				require.NoError(t, err)
			}
			released, _, err := helper.Get(ctx)
			require.NoError(t, err)
			require.Empty(t, released.HolderIdentity)
			for key, token := range tokens {
				value, err := kv.Get(ctx, []byte(key))
				require.NoError(t, err)
				require.NotEqual(t, token, string(value))
			}
			// Explicitly simulate a later acquisition, not reviving the old term.
			_, _, err = old.Get(ctx)
			require.NoError(t, err)
			record.LeaderTransitions++
			require.NoError(t, old.Update(ctx, record))
			_, before, err := helper.Get(ctx)
			require.NoError(t, err)
			for key := range tokens {
				value, err := kv.Get(ctx, []byte(key))
				require.NoError(t, err)
				tokens[key] = string(value)
			}
			require.ErrorIs(t, sender.send(ctx, condition), errPeerRetirementUnconfirmed)
			_, after, err := helper.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)
			for key, token := range tokens {
				value, err := kv.Get(ctx, []byte(key))
				require.NoError(t, err)
				require.Equal(t, token, string(value))
			}
		})
	}
}
