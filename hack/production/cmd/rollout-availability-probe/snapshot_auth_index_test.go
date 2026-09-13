package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3alarm"
	"go.etcd.io/etcd/server/v3/etcdserver/apply"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/metadata"
)

type restoredAuthIndexCapture struct {
	auth.AuthStore // Any unexpected store operation must fail the test.
	index          uint64
	calls          int
	stop           error
}

// Two actual auth stores model the window before one member applies the same
// Authenticate entry. Only Raft scheduling/index advancement is controlled.
// The applying-index case is a counterfactual control, not an upstream patch.
func TestRestoredOfficialSimpleTokenEarlyIndexWindow(t *testing.T) {
	for _, tokenIndex := range []uint64{41, 42} {
		name := "previous_index"
		if tokenIndex == 42 {
			name = "applying_index_control"
		}
		t.Run(name, func(t *testing.T) {
			applied := wait.NewTimeList()
			applied.Trigger(41)
			requested := make(chan uint64, 2)
			newStore := func(member string, observe bool) auth.AuthStore {
				lg := zap.NewNop()
				be := backend.NewDefaultBackend(lg, filepath.Join(t.TempDir(), member+".db"))
				t.Cleanup(func() { require.NoError(t, be.Close()) })
				tp, err := auth.NewTokenProvider(lg, "simple", func(index uint64) <-chan struct{} {
					if observe {
						requested <- index
					}
					return applied.Wait(index)
				}, time.Minute)
				require.NoError(t, err)
				store := auth.NewAuthStore(lg, schema.NewAuthBackend(lg, be), tp, bcrypt.MinCost)
				t.Cleanup(func() { store.Close() })
				_, err = store.UserAdd(&etcdserverpb.AuthUserAddRequest{Name: "root", Password: "fixture-only"})
				require.NoError(t, err)
				_, err = store.RoleAdd(&etcdserverpb.AuthRoleAddRequest{Name: "root"})
				require.NoError(t, err)
				_, err = store.UserGrantRole(&etcdserverpb.AuthUserGrantRoleRequest{User: "root", Role: "root"})
				require.NoError(t, err)
				require.NoError(t, store.AuthEnable())
				return store
			}
			source, target := newStore("source", false), newStore("target", true)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			assignCtx := context.WithValue(ctx, auth.AuthenticateParamIndex{}, tokenIndex)
			assignCtx = context.WithValue(assignCtx, auth.AuthenticateParamSimpleTokenPrefix{}, "index-fixture")
			issued, err := source.Authenticate(assignCtx, "root", "fixture-only")
			require.NoError(t, err)
			queryCtx := metadata.NewIncomingContext(ctx, metadata.Pairs("token", issued.Token))
			type outcome struct {
				info *auth.AuthInfo
				err  error
			}
			result := make(chan outcome, 1)
			done := make(chan struct{})
			defer func() { cancel(); <-done }() // Join before stores/backends close, including assertion failures.
			go func() {
				defer close(done)
				info, err := target.AuthInfoFromCtx(queryCtx)
				result <- outcome{info, err}
			}()
			select {
			case index := <-requested:
				require.Equal(t, tokenIndex, index)
			case <-ctx.Done():
				t.Fatal("token lookup did not reach index waiter")
			}
			if tokenIndex == 41 {
				select {
				case got := <-result:
					require.ErrorIs(t, got.err, auth.ErrInvalidAuthToken)
					require.NoError(t, ctx.Err(), "failure must precede deadline")
				case <-ctx.Done():
					t.Fatal("previous index did not release lookup")
				}
			} else {
				select {
				case <-result:
					t.Fatal("current index released before Authenticate was applied")
				default:
				}
			}
			// Apply the very same replicated Authenticate, then publish index 42.
			replicated, err := target.Authenticate(assignCtx, "root", "fixture-only")
			require.NoError(t, err)
			require.True(t, issued.Token == replicated.Token, "members must assign identical tokens")
			applied.Trigger(42)
			if tokenIndex == 41 {
				info, err := target.AuthInfoFromCtx(queryCtx)
				require.NoError(t, err)
				require.Equal(t, "root", info.Username)
			} else {
				select {
				case got := <-result:
					require.NoError(t, got.err)
					require.Equal(t, "root", got.info.Username)
				case <-ctx.Done():
					t.Fatal("current index lookup did not complete after application")
				}
			}
		})
	}
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
