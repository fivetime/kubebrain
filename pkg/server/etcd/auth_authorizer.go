package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"sort"
	"strings"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

type authCaller struct {
	username     string
	revision     uint64
	snapshot     *authSnapshot
	forwardToken string
	certificate  bool
}

// forwardedClientCertificateUsernameMetadataKey carries an identity already
// verified by a public mTLS listener across KubeBrain's mutually-authenticated
// peer hop. It is honored only when PeerServerOptions marked the receiving
// context as an internal peer request, so a public client cannot assert it.
const forwardedClientCertificateUsernameMetadataKey = "kubebrain-forwarded-client-certificate-username"

func (s *RPCServer) authCallerFromContext(ctx context.Context) (*authCaller, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if !snapshot.Config.Enabled {
		return nil, nil
	}
	credential, ok := authCredentialFromContext(ctx)
	if !ok {
		return s.authCallerFromTLS(ctx, snapshot)
	}
	token, err := authTokenFromCredential(credential)
	if err != nil {
		return nil, err
	}
	claims, err := s.tokens.verify(ctx, token)
	if err != nil {
		return nil, err
	}
	// verify refreshed the shared cache if needed.
	snapshot, err = s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	// etcd's simple-token provider resolves AuthInfo.Revision from the auth store
	// when a request starts. A token may survive unrelated auth mutations, but a
	// serialized read must fail with ErrAuthOldRevision if the store changes while
	// that request is executing.
	revision := snapshot.Config.Revision
	if s.tokens.jwt != nil {
		revision = claims.Revision
	}
	return &authCaller{
		username: claims.Username, revision: revision,
		snapshot: snapshot, forwardToken: credential,
	}, nil
}

// authCallerFromCachedContext authenticates against this member's already
// observed applied auth state. ok=false means that state is incomplete and the
// caller must use authCallerFromContext; it never means authentication passed.
func (s *RPCServer) authCallerFromCachedContext(ctx context.Context) (*authCaller, error, bool) {
	snapshot, ok := s.tokens.snapshots.cachedSnapshot()
	if !ok {
		return nil, nil, false
	}
	if !snapshot.Config.Enabled {
		return nil, nil, true
	}
	credential, ok := authCredentialFromContext(ctx)
	if !ok {
		caller, err := s.authCallerFromTLS(ctx, snapshot)
		return caller, err, true
	}
	token, err := authTokenFromCredential(credential)
	if err != nil {
		return nil, err, true
	}
	claims, err, complete := s.tokens.verifyCached(token, snapshot)
	if !complete {
		return nil, nil, false
	}
	if err != nil {
		return nil, err, true
	}
	revision := snapshot.Config.Revision
	if s.tokens.jwt != nil {
		revision = claims.Revision
	}
	return &authCaller{
		username: claims.Username, revision: revision,
		snapshot: snapshot, forwardToken: credential,
	}, nil, true
}

// prepareEtcdApplyAuthInfo mirrors EtcdServer.processInternalRaftRequestOnce's
// AuthInfoFromCtx step. Missing credentials are represented by a nil AuthInfo
// upstream and reach the apply-time authorization wrapper; malformed or invalid
// credentials fail before an InternalRaftRequest is proposed and are not apply
// observations.
func (s *RPCServer) prepareEtcdApplyAuthInfo(ctx context.Context) (*authCaller, error) {
	caller, err := s.authCallerFromContext(ctx)
	if err == nil && caller == nil && s.clientCertAuth {
		// EtcdServer.AuthInfoFromCtx falls back to TLS even when the token
		// provider returned no identity because auth was disabled. Preserve
		// that certificate identity/revision if auth is enabled before apply.
		var snapshot *authSnapshot
		snapshot, err = s.tokens.snapshots.current(ctx)
		if err == nil {
			caller, err = s.authCallerFromTLS(ctx, snapshot)
		}
	}
	if errors.Is(err, rpctypes.ErrUserEmpty) {
		return nil, nil
	}
	return caller, err
}

// Auth management handlers retain their separate apply authorization path.
func (s *RPCServer) validateEtcdApplyAuthInfo(ctx context.Context) error {
	_, err := s.prepareEtcdApplyAuthInfo(ctx)
	return err
}

// authCallerForEtcdApply retains the identity/revision captured before apply,
// just as etcd carries AuthInfo in its raft request header. Permissions must
// still come from the current auth store: re-authenticating here would silently
// upgrade a simple token to a newer revision and admit a stale request.
func (s *RPCServer) authCallerForEtcdApply(ctx context.Context, admitted *authCaller) (*authCaller, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if !snapshot.Config.Enabled {
		return nil, nil
	}
	if admitted == nil {
		// A nil AuthInfo becomes an empty raft request identity upstream.
		// Do not parse credentials that were ignored while auth was disabled:
		// current enabled-state authorization must report ErrUserEmpty.
		return &authCaller{snapshot: snapshot}, nil
	}
	caller := *admitted
	caller.snapshot = snapshot
	return &caller, nil
}

func authCredentialFromContext(ctx context.Context) (string, bool) {
	values := metadata.ValueFromIncomingContext(ctx, rpctypes.TokenFieldNameGRPC)
	if len(values) == 0 {
		values = metadata.ValueFromIncomingContext(ctx, rpctypes.TokenFieldNameSwagger)
	}
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

func authTokenFromCredential(credential string) (string, error) {
	if credential == "" {
		return "", rpctypes.ErrInvalidAuthToken
	}
	return strings.TrimPrefix(credential, "Bearer "), nil
}

func (s *RPCServer) authCallerFromTLS(ctx context.Context, snapshot *authSnapshot) (*authCaller, error) {
	if !s.clientCertAuth {
		return nil, rpctypes.ErrUserEmpty
	}
	// The peer transport certificate authenticates another KubeBrain member,
	// not the original etcd client. Accept only the identity explicitly carried
	// by a trusted ingress over the peer listener; never reinterpret the peer
	// certificate CN or public metadata as an etcd username.
	if isPeerRequest(ctx) {
		usernames := metadata.ValueFromIncomingContext(ctx, forwardedClientCertificateUsernameMetadataKey)
		if len(usernames) != 1 || usernames[0] == "" {
			return nil, rpctypes.ErrUserEmpty
		}
		return &authCaller{
			username: usernames[0], revision: snapshot.Config.Revision,
			snapshot: snapshot, certificate: true,
		}, nil
	}
	username, err := verifiedClientCertificateUsername(ctx)
	if err != nil {
		return nil, err
	}
	return &authCaller{
		username: username, revision: snapshot.Config.Revision,
		snapshot: snapshot, certificate: true,
	}, nil
}

func verifiedClientCertificateUsername(ctx context.Context) (string, error) {
	var state tls.ConnectionState
	var verified bool
	if p, ok := peer.FromContext(ctx); ok && p != nil && p.AuthInfo != nil {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			state, verified = tlsInfo.State, true
		}
	}
	if !verified {
		state, verified = transportidentity.TLSStateFromContext(ctx)
	}
	if !verified {
		return "", rpctypes.ErrUserEmpty
	}
	if values := metadata.ValueFromIncomingContext(ctx, "grpcgateway-accept"); len(values) > 0 {
		return "", rpctypes.ErrUserEmpty
	}
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		username := chain[0].Subject.CommonName
		if username != "" {
			return username, nil
		}
	}
	return "", rpctypes.ErrUserEmpty
}

func (s *RPCServer) ensureAuthRevision(ctx context.Context, caller *authCaller) error {
	if caller == nil {
		return nil
	}
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return err
	}
	if caller.revision != snapshot.Config.Revision {
		return rpctypes.ErrAuthOldRevision
	}
	return nil
}

// ensureAuthRevisionAfterSerializedRead matches etcd's in-memory auth revision
// comparison after doSerialize's read callback. The callback may have consumed
// the request deadline or canceled its context; that cancellation must not
// prevent a concurrent auth-store mutation from being reported as AuthOldRevision.
// KubeBrain persists auth metadata in TiKV, so give the final lookup its own
// bounded context while retaining request values.
func (s *RPCServer) ensureAuthRevisionAfterSerializedRead(ctx context.Context, caller *authCaller) error {
	fenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unaryRpcTimeout)
	defer cancel()
	return s.ensureAuthRevision(fenceCtx, caller)
}

func isAuthContractError(err error) bool {
	for _, target := range []error{
		rpctypes.ErrRootUserNotExist,
		rpctypes.ErrRootRoleNotExist,
		rpctypes.ErrUserAlreadyExist,
		rpctypes.ErrUserEmpty,
		rpctypes.ErrUserNotFound,
		rpctypes.ErrRoleAlreadyExist,
		rpctypes.ErrRoleNotFound,
		rpctypes.ErrRoleEmpty,
		rpctypes.ErrAuthFailed,
		rpctypes.ErrPermissionDenied,
		rpctypes.ErrRoleNotGranted,
		rpctypes.ErrPermissionNotGranted,
		rpctypes.ErrAuthNotEnabled,
		rpctypes.ErrInvalidAuthToken,
		rpctypes.ErrAuthOldRevision,
		rpctypes.ErrInvalidAuthMgmt,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (s *RPCServer) ensureAuthStoreRevisionUnchanged(ctx context.Context, caller *authCaller) error {
	if caller == nil {
		return nil
	}
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return err
	}
	if caller.snapshot.Config.Revision != snapshot.Config.Revision {
		return rpctypes.ErrAuthOldRevision
	}
	return nil
}

// ensureAuthStoreRevisionUnchangedAfterLeaseAuthorization mirrors etcd's
// in-memory AuthStore.Revision comparison at the end of checkLeaseRenew,
// checkLeaseTimeToLive, and checkLeaseLeases. The request may be canceled after
// its key permissions have been checked; cancellation must not prevent the
// revision fence from either accepting that snapshot or reporting AuthOld.
func (s *RPCServer) ensureAuthStoreRevisionUnchangedAfterLeaseAuthorization(
	ctx context.Context,
	caller *authCaller,
) error {
	fenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unaryRpcTimeout)
	defer cancel()
	return s.ensureAuthStoreRevisionUnchanged(fenceCtx, caller)
}

func (s *RPCServer) leaseTimeToLiveAuthRevision(ctx context.Context) (uint64, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return 0, err
	}
	return snapshot.Config.Revision, nil
}

// ensureLeaseTimeToLiveAuthRevision mirrors LeaseTimeToLive's separate final
// fence in etcd. Unlike LeaseLeases and LeaseRenew, etcd applies this fence to
// every Keys=true success, including root callers and requests that began while
// auth was disabled but raced with AuthEnable.
func (s *RPCServer) ensureLeaseTimeToLiveAuthRevision(ctx context.Context, revision uint64) error {
	fenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unaryRpcTimeout)
	defer cancel()
	snapshot, err := s.tokens.snapshots.current(fenceCtx)
	if err != nil {
		return err
	}
	if snapshot.Config.Enabled && snapshot.Config.Revision != revision {
		return rpctypes.ErrAuthOldRevision
	}
	return nil
}

func withAuthWriteGuard(ctx context.Context, caller *authCaller) context.Context {
	if caller == nil {
		return ctx
	}
	return backend.WithInternalWriteGuard(ctx, authConfigKey, encodeAuthConfig(caller.snapshot.Config))
}

func withCanonicalForwardedAuthIdentity(ctx context.Context, token, certificateUsername string) context.Context {
	outgoing, _ := metadata.FromOutgoingContext(ctx)
	canonical := outgoing.Copy()
	canonical.Delete(rpctypes.TokenFieldNameGRPC)
	canonical.Delete(rpctypes.TokenFieldNameSwagger)
	canonical.Delete(forwardedClientCertificateUsernameMetadataKey)
	// Capabilities are granted by their dedicated forwarding paths after auth
	// identity has been normalized. Never let an inherited outgoing context turn
	// an initial Watch into an authorized continuation, bypass HashKV admin auth,
	// or attribute quota admission to a different member on the trusted peer hop.
	canonical.Delete(etcdproxy.AuthorizedWatchProxyMetadataKey)
	canonical.Delete(authorizedPeerHashKVProxyMetadataKey)
	canonical.Delete(quotaAdmissionMemberMetadataKey)
	canonical.Delete(countProxyMarkerKey)
	if token != "" {
		canonical.Set(rpctypes.TokenFieldNameGRPC, token)
	} else if certificateUsername != "" {
		canonical.Set(forwardedClientCertificateUsernameMetadataKey, certificateUsername)
	}
	return metadata.NewOutgoingContext(ctx, canonical)
}

func (s *RPCServer) forwardAuthToken(ctx context.Context, caller *authCaller) (context.Context, error) {
	if caller == nil {
		return withCanonicalForwardedAuthIdentity(ctx, "", ""), nil
	}
	token := caller.forwardToken
	if token == "" && caller.certificate {
		var err error
		token, err = s.tokens.issueCertificate(ctx, caller.snapshot, caller.username)
		if err != nil {
			return nil, err
		}
	}
	if token != "" {
		return withCanonicalForwardedAuthIdentity(ctx, token, ""), nil
	}
	return withCanonicalForwardedAuthIdentity(ctx, "", ""), nil
}

// forwardWriteAuthContext preserves etcd's raft-write admission order: a
// follower forwards the request before the auth applier runs on the leader.
// Bearer/simple/JWT credentials are therefore passed through verbatim and are
// verified authoritatively by the leader. A verified client certificate cannot
// cross the internal gRPC hop, so mint the same short-lived forwarding token
// used by the existing authenticated proxy path when one is available.
func (s *RPCServer) forwardWriteAuthContext(ctx context.Context) (context.Context, error) {
	if credential, ok := authCredentialFromContext(ctx); ok {
		return withCanonicalForwardedAuthIdentity(ctx, credential, ""), nil
	}
	// Without client-certificate auth there is nothing to translate for the
	// internal hop. In particular, do not turn a follower proxy into a local
	// auth-storage read: the leader will authoritatively apply auth to the raw
	// request, and a follower may legitimately have lost its own PD path.
	if !s.clientCertAuth {
		return withCanonicalForwardedAuthIdentity(ctx, "", ""), nil
	}
	username, err := verifiedClientCertificateUsername(ctx)
	if err != nil {
		// Forward an unauthenticated request and let the leader's auth applier
		// return the canonical error after leadership has been established.
		return withCanonicalForwardedAuthIdentity(ctx, "", ""), nil
	}
	// Do not consult the follower's auth cache here. A cluster-wide AuthEnable
	// may have committed after this ingress lost its PD/TiKV path; the leader
	// owns the current enabled flag, auth revision, user existence and roles.
	return withCanonicalForwardedAuthIdentity(ctx, "", username), nil
}

func (c *authCaller) isRoot() bool {
	if c == nil {
		return true
	}
	return c.hasRole("root")
}

func (c *authCaller) hasRole(roleName string) bool {
	if c == nil {
		return false
	}
	user := c.snapshot.Users[c.username]
	if user == nil {
		return false
	}
	for _, role := range user.Roles {
		if role == roleName {
			return true
		}
	}
	return false
}

func (c *authCaller) adminError() error {
	if c == nil || c.username == "" {
		return rpctypes.ErrUserEmpty
	}
	if c.snapshot.Users[c.username] == nil {
		return rpctypes.ErrUserNotFound
	}
	if c.hasRole("root") {
		return nil
	}
	return rpctypes.ErrPermissionDenied
}

type authInterval struct {
	start []byte
	end   []byte
	open  bool
}

func permissionAllows(permission, required authpb.Permission_Type) bool {
	return permission == authpb.READWRITE || permission == required
}

func (c *authCaller) permits(key, rangeEnd []byte, required authpb.Permission_Type) bool {
	if c == nil || c.isRoot() {
		return true
	}
	user := c.snapshot.Users[c.username]
	if user == nil {
		return false
	}
	intervals := make([]authInterval, 0)
	for _, roleName := range user.Roles {
		role := c.snapshot.Roles[roleName]
		if role == nil {
			continue
		}
		for _, permission := range role.KeyPermission {
			if !permissionAllows(permission.PermType, required) {
				continue
			}
			if len(permission.RangeEnd) == 0 {
				if len(rangeEnd) == 0 && bytes.Equal(permission.Key, key) {
					return true
				}
				continue
			}
			intervals = append(intervals, authInterval{
				start: permission.Key, end: permission.RangeEnd,
				open: len(permission.RangeEnd) == 1 && permission.RangeEnd[0] == 0,
			})
		}
	}
	if len(rangeEnd) == 0 {
		for _, interval := range intervals {
			if bytes.Compare(interval.start, key) <= 0 && (interval.open || bytes.Compare(key, interval.end) < 0) {
				return true
			}
		}
		return false
	}
	requestOpen := len(rangeEnd) == 1 && rangeEnd[0] == 0
	sort.Slice(intervals, func(i, j int) bool { return bytes.Compare(intervals[i].start, intervals[j].start) < 0 })
	covered := append([]byte(nil), key...)
	for _, interval := range intervals {
		if bytes.Compare(interval.start, covered) > 0 {
			return false
		}
		if !interval.open && bytes.Compare(interval.end, covered) <= 0 {
			continue
		}
		if interval.open {
			return true
		}
		covered = interval.end
		if !requestOpen && bytes.Compare(covered, rangeEnd) >= 0 {
			return true
		}
	}
	return false
}

func (c *authCaller) require(key, rangeEnd []byte, required authpb.Permission_Type) error {
	if c == nil {
		return nil
	}
	if c.revision == 0 {
		return rpctypes.ErrUserEmpty
	}
	if c.revision < c.snapshot.Config.Revision {
		return rpctypes.ErrAuthOldRevision
	}
	if c.permits(key, rangeEnd, required) {
		return nil
	}
	return rpctypes.ErrPermissionDenied
}

func (s *RPCServer) authorizePut(caller *authCaller, request *etcdserverpb.PutRequest) error {
	if err := caller.require(request.Key, nil, authpb.WRITE); err != nil {
		return err
	}
	if request.PrevKv {
		if err := caller.require(request.Key, nil, authpb.READ); err != nil {
			return err
		}
	}
	if request.Lease != 0 {
		for _, key := range s.keysForLease(request.Lease) {
			if err := caller.require([]byte(key), nil, authpb.WRITE); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RPCServer) authorizeTxn(caller *authCaller, request *etcdserverpb.TxnRequest) error {
	for _, compare := range request.Compare {
		if err := caller.require(compare.Key, compare.RangeEnd, authpb.READ); err != nil {
			return err
		}
	}
	for _, branch := range [][]*etcdserverpb.RequestOp{request.Success, request.Failure} {
		for _, op := range branch {
			switch {
			case op.GetRequestRange() != nil:
				rangeRequest := op.GetRequestRange()
				if err := caller.require(rangeRequest.Key, rangeRequest.RangeEnd, authpb.READ); err != nil {
					return err
				}
			case op.GetRequestPut() != nil:
				if err := s.authorizePut(caller, op.GetRequestPut()); err != nil {
					return err
				}
			case op.GetRequestDeleteRange() != nil:
				deleteRequest := op.GetRequestDeleteRange()
				if err := caller.require(deleteRequest.Key, deleteRequest.RangeEnd, authpb.WRITE); err != nil {
					return err
				}
				if deleteRequest.PrevKv {
					if err := caller.require(deleteRequest.Key, deleteRequest.RangeEnd, authpb.READ); err != nil {
						return err
					}
				}
			case op.GetRequestTxn() != nil:
				if err := s.authorizeTxn(caller, op.GetRequestTxn()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
