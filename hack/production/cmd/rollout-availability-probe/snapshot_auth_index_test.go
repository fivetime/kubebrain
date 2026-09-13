package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3alarm"
	"go.etcd.io/etcd/server/v3/etcdserver/apply"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.uber.org/zap"
)

type restoredAuthIndexCapture struct {
	auth.AuthStore // Any unexpected store operation must fail the test.
	index          uint64
	calls          int
	stop           error
}

func (s *restoredAuthIndexCapture) Authenticate(ctx context.Context, _, _ string) (*etcdserverpb.AuthenticateResponse, error) {
	s.calls++
	s.index = ctx.Value(auth.AuthenticateParamIndex{}).(uint64)
	return nil, s.stop // Stop before response header construction; no credentials.
}

// Characterize the pinned official applier, not a desired KubeBrain contract.
// This proves which index reaches token assignment when committed and applying
// indexes differ. It does not reproduce follower lag or the deployed failure.
// Revisit this diagnostic when upgrading the official etcd dependency.
func TestRestoredOfficialAuthApplierIndexDiagnostic(t *testing.T) {
	index := cindex.NewConsistentIndex(nil)
	index.SetConsistentIndex(41, 1)
	index.SetConsistentApplyingIndex(42, 1)
	applying, _ := index.ConsistentApplyingIndex()
	require.Equal(t, uint64(42), applying)
	store := &restoredAuthIndexCapture{stop: errors.New("index captured")}
	applier := apply.NewUberApplier(apply.ApplierOptions{
		Logger: zap.NewNop(), AuthStore: store, ConsistentIndex: index,
		AlarmStore: &v3alarm.AlarmStore{}, QuotaBackendBytesCfg: -1,
		WarningApplyDuration: time.Hour,
	})
	result := applier.Apply(&apply.InternalRaftRequestWrapper{
		InternalRaftRequest: &etcdserverpb.InternalRaftRequest{
			Authenticate: &etcdserverpb.InternalAuthenticateRequest{Name: "index-fixture"},
		},
	}, membership.ApplyBoth)
	require.ErrorIs(t, result.Err, store.stop)
	require.Equal(t, 1, store.calls)
	require.Equal(t, uint64(41), store.index,
		"pinned upstream passes the consistent index, not current applying index")
	require.Less(t, store.index, applying)
}
