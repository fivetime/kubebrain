package etcd

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

// Change durable auth state after capturing a read, without invalidating this
// member's cache, as an auth mutation on another member would do.
type authConfigAfterReadShim struct {
	BackendShim
	fired       atomic.Bool
	reads       atomic.Int32
	afterReadAt int32
	afterRead   func()
}

func (s *authConfigAfterReadShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	value, err := s.BackendShim.InternalGet(ctx, key)
	if (err == nil || errors.Is(err, storage.ErrKeyNotFound)) && bytes.Equal(key, authConfigKey) {
		at := s.afterReadAt
		if at == 0 {
			at = 1
		}
		if s.reads.Add(1) == at && s.fired.CompareAndSwap(false, true) {
			s.afterRead()
		}
	}
	return value, err
}

func TestAuthDisabledWriteCommitFence(t *testing.T) {
	for _, state := range []string{"absent", "persisted"} {
		for _, transition := range []string{"stable", "enable"} {
			for _, op := range []string{"put", "delete", "txn", "lease-revoke"} {
				t.Run(state+"/"+transition+"/"+op, func(t *testing.T) {
					server, closeFn := newTestRPCServer(t)
					defer closeFn()
					ctx := context.Background()
					if state == "persisted" {
						require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
						require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
					}
					lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
					require.NoError(t, err)
					key := []byte("/disabled-auth-commit-fence")
					_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("original"), Lease: lease.ID})
					require.NoError(t, err)
					original := server.backend
					before, err := original.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
					require.NoError(t, err)
					snapshot, err := server.tokens.snapshots.current(ctx)
					require.NoError(t, err)
					require.Equal(t, state == "persisted", snapshot.ConfigExists)
					shim := &authConfigAfterReadShim{BackendShim: original, afterReadAt: 2}
					shim.afterRead = func() {
						if transition == "enable" {
							config := snapshot.Config
							config.Enabled = true
							config.Revision++
							// Model an external member's committed config change after
							// the local apply read, without invalidating its cache.
							// The absent case exercises the first metadata creation.
							require.NoError(t, original.InternalPut(ctx, authConfigKey, encodeAuthConfig(config)))
						}
					}
					server.tokens.snapshots.repo.backend = shim
					switch op {
					case "put":
						_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")})
					case "delete":
						_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
					case "txn":
						_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")}}}}})
					case "lease-revoke":
						_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: lease.ID})
					}
					require.True(t, shim.fired.Load())
					if transition == "enable" {
						require.ErrorIs(t, err, rpctypes.ErrAuthOldRevision)
						after, getErr := original.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
						require.NoError(t, getErr)
						require.Equal(t, before, after, "rejected writes must preserve user data and revision")
					} else {
						require.NoError(t, err)
						value, getErr := original.InternalGet(ctx, authConfigKey)
						if state == "absent" {
							require.ErrorIs(t, getErr, storage.ErrKeyNotFound)
						} else {
							require.NoError(t, getErr)
							require.Equal(t, encodeAuthConfig(snapshot.Config), value)
						}
					}
				})
			}
		}
	}
}

// Upstream stamps the first AuthInfo.Revision into the raft request header.
// Apply checks that version against the current auth store, rather than
// authenticating again and silently adopting a newer version.
func TestKVWritePreservesInitialAuthRevisionAtApply(t *testing.T) {
	for _, identity := range []string{"simple-token", "client-certificate", "forwarded-certificate"} {
		t.Run(identity, func(t *testing.T) {
			for _, op := range []string{"put", "delete", "txn"} {
				t.Run(op, func(t *testing.T) {
					server, closeFn := newTestRPCServer(t)
					defer closeFn()
					ctx := setupAuthKVUser(t, server)
					admissionReads := int32(3)
					if identity != "simple-token" {
						server.SetClientCertAuth(true)
						admissionReads = 1
						ctx = verifiedTLSContext(context.Background(), "alice")
						if identity == "forwarded-certificate" {
							ctx = context.WithValue(verifiedTLSContext(context.Background(), "peer-member"), peerRequestContextKey{}, true)
							ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(forwardedClientCertificateUsernameMetadataKey, "alice"))
						}
					}
					key := []byte("/allowed/auth-revision-at-apply")
					_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("original")})
					require.NoError(t, err)
					original := server.backend
					before, err := original.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
					require.NoError(t, err)
					snapshot, err := server.tokens.snapshots.current(ctx)
					require.NoError(t, err)
					// A warm simple-token authentication reads config before verify,
					// during verify, and when assigning AuthInfo.Revision.
					// Certificate identities capture their revision in one config read.
					shim := &authConfigAfterReadShim{BackendShim: original, afterReadAt: admissionReads}
					shim.afterRead = func() {
						config := snapshot.Config
						config.Revision++
						require.NoError(t, original.InternalPut(context.Background(), authConfigKey, encodeAuthConfig(config)))
					}
					server.tokens.snapshots.repo.backend = shim
					switch op {
					case "put":
						_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")})
					case "delete":
						_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
					case "txn":
						_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")}}}}})
					}
					require.True(t, shim.fired.Load())
					require.ErrorIs(t, err, rpctypes.ErrAuthOldRevision)
					after, err := original.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
					require.NoError(t, err)
					require.Equal(t, before, after, "stale auth must not change the value or user revision")
				})
			}
		})
	}
}

func TestPutRechecksAuthEnabledAfterInitialAuthInfoRead(t *testing.T) {
	for _, test := range []struct {
		name           string
		ctx            context.Context
		want           error
		clientCert     bool
		admissionReads int32
	}{
		{"missing credentials", context.Background(), rpctypes.ErrUserEmpty, false, 1},
		{"empty token ignored before auth enable", metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, "")), rpctypes.ErrUserEmpty, false, 1},
		{"certificate captured before auth enable", verifiedTLSContext(context.Background(), "root"), rpctypes.ErrAuthOldRevision, true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			server.SetClientCertAuth(test.clientCert)
			ctx := context.Background()
			require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
			require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
			snapshot, err := server.tokens.snapshots.current(ctx)
			require.NoError(t, err)
			require.False(t, snapshot.Config.Enabled)
			original := server.backend
			shim := &authConfigAfterReadShim{BackendShim: original, afterReadAt: test.admissionReads}
			shim.afterRead = func() {
				config := snapshot.Config
				config.Enabled = true
				config.Revision++
				require.NoError(t, original.InternalPut(ctx, authConfigKey, encodeAuthConfig(config)))
			}
			server.tokens.snapshots.repo.backend = shim
			key := []byte("/auth-enable-admission")
			response, err := server.Put(test.ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("must-not-commit")})
			require.True(t, shim.fired.Load(), "must exercise the auth state transition")
			require.ErrorIs(t, err, test.want)
			require.Nil(t, response)
			stored, err := original.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Empty(t, stored.Kvs, "a cached disabled-auth result must not admit this write")
		})
	}
}

func TestDisabledAuthCertificateAdmissionUsesOneSnapshot(t *testing.T) {
	for _, forwarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "forwarded"}[forwarded], func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			server.SetClientCertAuth(true)
			ctx := verifiedTLSContext(context.Background(), "root")
			if forwarded {
				ctx = context.WithValue(verifiedTLSContext(context.Background(), "peer-member"), peerRequestContextKey{}, true)
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(forwardedClientCertificateUsernameMetadataKey, "root"))
			}
			// Disabled auth still ignores invalid tokens, but apply admission must
			// retain the verified certificate identity for a later enable.
			md, _ := metadata.FromIncomingContext(ctx)
			md = md.Copy()
			md.Set(rpctypes.TokenFieldNameGRPC, "invalid-ignored-token")
			ctx = metadata.NewIncomingContext(ctx, md)
			snapshot, err := server.tokens.snapshots.current(ctx)
			require.NoError(t, err)
			require.False(t, snapshot.Config.Enabled)
			shim := &authConfigAfterReadShim{BackendShim: server.backend, afterRead: func() {}}
			server.tokens.snapshots.repo.backend = shim
			caller, err := server.prepareEtcdApplyAuthInfo(ctx)
			require.NoError(t, err)
			require.NotNil(t, caller)
			require.Equal(t, "root", caller.username)
			require.True(t, caller.certificate)
			require.Equal(t, snapshot.Config.Revision, caller.revision)
			require.EqualValues(t, 1, shim.reads.Load(), "one fresh admission read, not a TLS fallback reread")
			_, applied, err := server.authCallerForEtcdApply(ctx, caller)
			require.NoError(t, err)
			require.Nil(t, applied)
			require.EqualValues(t, 2, shim.reads.Load(), "apply must still fetch fresh state")
		})
	}
}

func TestDisabledAuthCertificatePutRetainsCommitFence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetClientCertAuth(true)
	ctx := verifiedTLSContext(context.Background(), "root")
	snapshot, err := server.tokens.snapshots.current(ctx)
	require.NoError(t, err)
	original := server.backend
	shim := &authConfigAfterReadShim{BackendShim: original, afterReadAt: 2}
	shim.afterRead = func() {
		config := snapshot.Config
		config.Enabled = true
		config.Revision++
		require.NoError(t, original.InternalPut(context.Background(), authConfigKey, encodeAuthConfig(config)))
	}
	server.tokens.snapshots.repo.backend = shim
	key := []byte("/certificate-disabled-auth-commit-fence")
	response, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("must-not-commit")})
	require.True(t, shim.fired.Load())
	require.ErrorIs(t, err, rpctypes.ErrAuthOldRevision)
	require.Nil(t, response)
	stored, err := original.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, stored.Kvs)
}
