package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"sort"
	"strings"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

type authCaller struct {
	username     string
	revision     uint64
	snapshot     *authSnapshot
	forwardToken string
	certificate  bool
}

func (s *RPCServer) authCallerFromContext(ctx context.Context) (*authCaller, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if !snapshot.Config.Enabled {
		return nil, nil
	}
	values := metadata.ValueFromIncomingContext(ctx, rpctypes.TokenFieldNameGRPC)
	if len(values) == 0 {
		values = metadata.ValueFromIncomingContext(ctx, rpctypes.TokenFieldNameSwagger)
	}
	if len(values) == 0 {
		return s.authCallerFromTLS(ctx, snapshot)
	}
	credential := values[0]
	if credential == "" {
		return nil, rpctypes.ErrInvalidAuthToken
	}
	token := strings.TrimPrefix(credential, "Bearer ")
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

func (s *RPCServer) authCallerFromTLS(ctx context.Context, snapshot *authSnapshot) (*authCaller, error) {
	if !s.clientCertAuth {
		return nil, rpctypes.ErrUserEmpty
	}
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
		return nil, rpctypes.ErrUserEmpty
	}
	if values := metadata.ValueFromIncomingContext(ctx, "grpcgateway-accept"); len(values) > 0 {
		return nil, rpctypes.ErrUserEmpty
	}
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		username := chain[0].Subject.CommonName
		return &authCaller{
			username: username,
			revision: snapshot.Config.Revision,
			snapshot: snapshot, certificate: true,
		}, nil
	}
	return nil, rpctypes.ErrUserEmpty
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

func withAuthWriteGuard(ctx context.Context, caller *authCaller) context.Context {
	if caller == nil {
		return ctx
	}
	return backend.WithInternalWriteGuard(ctx, authConfigKey, encodeAuthConfig(caller.snapshot.Config))
}

func (s *RPCServer) forwardAuthToken(ctx context.Context, caller *authCaller) (context.Context, error) {
	if caller == nil {
		return ctx, nil
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
		return metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, token), nil
	}
	return ctx, nil
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
