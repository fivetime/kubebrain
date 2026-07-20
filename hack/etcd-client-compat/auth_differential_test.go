package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authErrorOutcome struct {
	Code             codes.Code
	Message          string
	PermissionDenied bool
	UserEmpty        bool
}

type authDifferentialOutcome struct {
	EnabledRevision           uint64
	DuplicateGrantRevision    uint64
	DuplicateRoleRevision     uint64
	ImplicitRootRole          authErrorOutcome
	WrongCredentials          authErrorOutcome
	NoPassword                authErrorOutcome
	DuplicateRole             authErrorOutcome
	MissingRevoke             authErrorOutcome
	ConcurrentDistinctOK      int
	ConcurrentDistinctDelta   uint64
	ConcurrentDuplicateOK     int
	ConcurrentDuplicateExists int
	ConcurrentDuplicateDelta  uint64
	PermissionCount           int
	PermissionType            int32
	MissingPermissionRevoke   authErrorOutcome
	InvalidPermissionRange    authErrorOutcome
	AliceRolesAfterRoleDelete int
	RootUserDelete            authErrorOutcome
	RootRoleRevoke            authErrorOutcome
	RootRoleDelete            authErrorOutcome
	OldTokenAfterMutation     authErrorOutcome
	OldPassword               authErrorOutcome
	NewPasswordOK             bool
	AnonymousCompact          authErrorOutcome
	UserCompact               authErrorOutcome
	RootCompactOK             bool
	AnonymousRangeStream      authErrorOutcome
	UserRangeStream           authErrorOutcome
	RootRangeStreamCount      int
	AnonymousStatus           authErrorOutcome
	UserStatusOK              bool
	AnonymousMemberList       authErrorOutcome
	UserMemberListOK          bool
	AnonymousAlarmList        authErrorOutcome
	UserAlarmListOK           bool
	UserAlarmDisarm           authErrorOutcome
	UserHash                  authErrorOutcome
	RootHashOK                bool
	UserKeepAlive             authErrorOutcome
	RootKeepAliveOK           bool
	AnonymousTTL              authErrorOutcome
	AnonymousTTLWithKeys      authErrorOutcome
	UserTTLOK                 bool
	UserTTLWithKeys           authErrorOutcome
	RootTTLContainsProtected  bool
	KeepAliveStreamFirstOK    bool
	KeepAliveAfterRoleRevoke  authErrorOutcome
	KeepAliveAfterRestoreOK   bool
	AnonymousLeaseList        authErrorOutcome
	UserLeaseList             authErrorOutcome
	RootLeaseListContains     bool
	UserLeaseListAfterRevoke  bool
	UserLeasedPut             authErrorOutcome
	UserLeasedTxnPut          authErrorOutcome
	WriterTxnPutPrevKV        authErrorOutcome
	WriterTxnValuePreserved   bool
	NestedDeniedRange         authErrorOutcome
	NestedDeniedPut           authErrorOutcome
	NestedDeniedDelete        authErrorOutcome
	NestedLeasedPut           authErrorOutcome
	NestedPutPrevKV           authErrorOutcome
	NestedDeniedPutPreserved  bool
	NestedDeletePreserved     bool
	NestedPrevKVPreserved     bool
}

func runConcurrentClientOperations(count int, operation func(int) error) []error {
	errs := make([]error, count)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) {
			defer done.Done()
			ready.Done()
			<-start
			errs[index] = operation(index)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	return errs
}

func authError(err error) authErrorOutcome {
	code := status.Code(err)
	return authErrorOutcome{
		Code:             code,
		Message:          status.Convert(err).Message(),
		PermissionDenied: code == codes.PermissionDenied || errors.Is(err, rpctypes.ErrPermissionDenied),
		UserEmpty:        errors.Is(err, rpctypes.ErrUserEmpty),
	}
}

func authRangeStream(t *testing.T, ctx context.Context, cli *clientv3.Client) (*clientv3.GetResponse, error) {
	t.Helper()
	stream, err := cli.GetStream(ctx, "/auth-compact/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	response, err := clientv3.GetStreamToGetResponse(stream)
	return (*clientv3.GetResponse)(response), err
}

func collectAuthDifferentialOutcome(t *testing.T, endpoint string) authDifferentialOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")
	compactRevisions := make([]int64, 0, 3)
	for i := 0; i < 3; i++ {
		put, putErr := bootstrap.Put(ctx, fmt.Sprintf("/auth-compact/%d", i), "value")
		require.NoError(t, putErr)
		compactRevisions = append(compactRevisions, put.Header.Revision)
	}
	protectedLease, err := bootstrap.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = bootstrap.Put(ctx, "/auth-protected/leased", "secret", clientv3.WithLease(protectedLease.ID))
	require.NoError(t, err)

	_, err = bootstrap.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = bootstrap.UserAddWithOptions(ctx, "nopass", "", &clientv3.UserAddOptions{NoPassword: true})
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "writer", "writer-secret")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "allowed")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "write-only")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(
		ctx,
		"allowed",
		"/auth-allowed/",
		clientv3.GetPrefixRangeEnd("/auth-allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "alice", "allowed")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(
		ctx,
		"write-only",
		"/auth-write-only/key",
		"",
		clientv3.PermissionType(clientv3.PermWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "writer", "write-only")
	require.NoError(t, err)
	_, implicitRootRoleErr := bootstrap.RoleGet(ctx, "root")
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)
	statusAfterEnable, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)

	_, wrongCredentialsErr := bootstrap.Authenticate(ctx, "missing", "wrong")
	_, noPasswordErr := bootstrap.Authenticate(ctx, "nopass", "password")
	root := authClient(t, endpoint, "root", "root-secret")
	alice := authClient(t, endpoint, "alice", "alice-secret")
	writer := authClient(t, endpoint, "writer", "writer-secret")
	var dynamicLeaseID clientv3.LeaseID
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = root.Revoke(cleanupCtx, protectedLease.ID)
		if dynamicLeaseID != 0 {
			_, _ = root.Revoke(cleanupCtx, dynamicLeaseID)
		}
		_, _ = root.AuthDisable(cleanupCtx)
		if users, listErr := bootstrap.UserList(cleanupCtx); listErr == nil {
			for _, user := range users.Users {
				if detail, getErr := bootstrap.UserGet(cleanupCtx, user); getErr == nil {
					for _, role := range detail.Roles {
						_, _ = bootstrap.UserRevokeRole(cleanupCtx, user, role)
					}
				}
				_, _ = bootstrap.UserDelete(cleanupCtx, user)
			}
		}
		if roles, listErr := bootstrap.RoleList(cleanupCtx); listErr == nil {
			for _, role := range roles.Roles {
				_, _ = bootstrap.RoleDelete(cleanupCtx, role)
			}
		}
		_, _ = bootstrap.Delete(cleanupCtx, "/auth-", clientv3.WithPrefix())
	})
	_, anonymousCompactErr := bootstrap.Compact(ctx, compactRevisions[0])
	_, userCompactErr := alice.Compact(ctx, compactRevisions[1])
	_, rootCompactErr := root.Compact(ctx, compactRevisions[2])
	_, anonymousRangeStreamErr := authRangeStream(t, ctx, bootstrap)
	_, userRangeStreamErr := authRangeStream(t, ctx, alice)
	rootRangeStream, rootRangeStreamErr := authRangeStream(t, ctx, root)
	require.NoError(t, rootRangeStreamErr)
	_, anonymousStatusErr := bootstrap.Status(ctx, bootstrap.Endpoints()[0])
	_, userStatusErr := alice.Status(ctx, alice.Endpoints()[0])
	_, anonymousMemberListErr := bootstrap.MemberList(ctx)
	_, userMemberListErr := alice.MemberList(ctx)
	_, anonymousAlarmListErr := bootstrap.AlarmList(ctx)
	_, userAlarmListErr := alice.AlarmList(ctx)
	_, userAlarmDisarmErr := alice.AlarmDisarm(ctx, &clientv3.AlarmMember{})
	_, userHashErr := alice.HashKV(ctx, alice.Endpoints()[0], 0)
	_, rootHashErr := root.HashKV(ctx, root.Endpoints()[0], 0)
	_, userKeepAliveErr := alice.KeepAliveOnce(ctx, protectedLease.ID)
	_, rootKeepAliveErr := root.KeepAliveOnce(ctx, protectedLease.ID)
	_, anonymousTTLErr := bootstrap.TimeToLive(ctx, protectedLease.ID)
	_, anonymousTTLWithKeysErr := bootstrap.TimeToLive(
		ctx, protectedLease.ID, clientv3.WithAttachedKeys(),
	)
	_, userTTLErr := alice.TimeToLive(ctx, protectedLease.ID)
	_, userTTLWithKeysErr := alice.TimeToLive(ctx, protectedLease.ID, clientv3.WithAttachedKeys())
	rootTTL, rootTTLErr := root.TimeToLive(ctx, protectedLease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, rootTTLErr)
	rootTTLContainsProtected := false
	for _, key := range rootTTL.Keys {
		if string(key) == "/auth-protected/leased" {
			rootTTLContainsProtected = true
			break
		}
	}
	_, anonymousLeaseListErr := bootstrap.Leases(ctx)
	_, userLeaseListErr := alice.Leases(ctx)
	rootLeaseList, rootLeaseListErr := root.Leases(ctx)
	require.NoError(t, rootLeaseListErr)
	rootLeaseListContains := false
	for _, lease := range rootLeaseList.Leases {
		if lease.ID == protectedLease.ID {
			rootLeaseListContains = true
			break
		}
	}
	_, userLeasedPutErr := alice.Put(
		ctx, "/auth-allowed/leased-put", "value", clientv3.WithLease(protectedLease.ID),
	)
	_, userLeasedTxnPutErr := alice.Txn(ctx).Then(
		clientv3.OpPut("/auth-allowed/leased-txn-put", "value", clientv3.WithLease(protectedLease.ID)),
	).Commit()
	_, err = root.Put(ctx, "/auth-write-only/key", "before")
	require.NoError(t, err)
	_, writerTxnPutPrevKVErr := writer.Txn(ctx).Then(
		clientv3.OpPut("/auth-write-only/key", "after", clientv3.WithPrevKV()),
	).Commit()
	writerTxnValue, err := root.Get(ctx, "/auth-write-only/key")
	require.NoError(t, err)
	require.Len(t, writerTxnValue.Kvs, 1)
	twoLevelNested := func(op clientv3.Op) clientv3.Op {
		return clientv3.OpTxn(nil, []clientv3.Op{
			clientv3.OpTxn(nil, []clientv3.Op{op}, nil),
		}, nil)
	}
	_, nestedDeniedRangeErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpGet("/auth-denied/range")),
	).Commit()
	_, err = root.Put(ctx, "/auth-denied/put", "before")
	require.NoError(t, err)
	_, nestedDeniedPutErr := alice.Txn(ctx).Then(clientv3.OpTxn(
		nil,
		[]clientv3.Op{clientv3.OpGet("/auth-allowed/")},
		[]clientv3.Op{clientv3.OpPut("/auth-denied/put", "after")},
	)).Commit()
	nestedDeniedPutValue, err := root.Get(ctx, "/auth-denied/put")
	require.NoError(t, err)
	require.Len(t, nestedDeniedPutValue.Kvs, 1)
	_, err = root.Put(ctx, "/auth-denied/delete", "before")
	require.NoError(t, err)
	_, nestedDeniedDeleteErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpDelete("/auth-denied/delete", clientv3.WithPrevKV())),
	).Commit()
	nestedDeleteValue, err := root.Get(ctx, "/auth-denied/delete")
	require.NoError(t, err)
	require.Len(t, nestedDeleteValue.Kvs, 1)
	_, nestedLeasedPutErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpPut(
			"/auth-allowed/nested-leased", "value", clientv3.WithLease(protectedLease.ID),
		)),
	).Commit()
	_, nestedPutPrevKVErr := writer.Txn(ctx).Then(
		twoLevelNested(clientv3.OpPut("/auth-write-only/key", "nested-after", clientv3.WithPrevKV())),
	).Commit()
	nestedPrevKVValue, err := root.Get(ctx, "/auth-write-only/key")
	require.NoError(t, err)
	require.Len(t, nestedPrevKVValue.Kvs, 1)
	_, err = root.Revoke(ctx, protectedLease.ID)
	require.NoError(t, err)
	_, userLeaseListAfterRevokeErr := alice.Leases(ctx)
	dynamicLease, err := alice.Grant(ctx, 60)
	require.NoError(t, err)
	dynamicLeaseID = dynamicLease.ID
	_, err = alice.Put(
		ctx, "/auth-allowed/dynamic-keepalive", "value", clientv3.WithLease(dynamicLease.ID),
	)
	require.NoError(t, err)
	keepAliveStream, err := etcdserverpb.NewLeaseClient(alice.ActiveConnection()).LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, keepAliveStream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(dynamicLease.ID)}))
	firstKeepAlive, firstKeepAliveErr := keepAliveStream.Recv()
	require.NoError(t, firstKeepAliveErr)
	keepAliveStreamFirstOK := firstKeepAlive.ID == int64(dynamicLease.ID) && firstKeepAlive.TTL > 0
	_, err = root.RoleRevokePermission(
		ctx, "allowed", "/auth-allowed/", clientv3.GetPrefixRangeEnd("/auth-allowed/"),
	)
	require.NoError(t, err)
	require.NoError(t, keepAliveStream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(dynamicLease.ID)}))
	_, keepAliveAfterRoleRevokeErr := keepAliveStream.Recv()
	_, err = root.RoleGrantPermission(
		ctx,
		"allowed",
		"/auth-allowed/",
		clientv3.GetPrefixRangeEnd("/auth-allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	restoredKeepAlive, keepAliveAfterRestoreErr := alice.KeepAliveOnce(ctx, dynamicLease.ID)
	keepAliveAfterRestoreOK := keepAliveAfterRestoreErr == nil &&
		restoredKeepAlive.ID == dynamicLease.ID && restoredKeepAlive.TTL > 0
	_, err = root.Revoke(ctx, dynamicLease.ID)
	require.NoError(t, err)
	dynamicLeaseID = 0

	_, err = root.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	statusAfterDuplicateGrant, err := root.AuthStatus(ctx)
	require.NoError(t, err)
	_, err = root.RoleAdd(ctx, "reader")
	require.NoError(t, err)
	_, duplicateRoleErr := root.RoleAdd(ctx, "reader")
	statusAfterDuplicateRole, err := root.AuthStatus(ctx)
	require.NoError(t, err)
	_, missingRevokeErr := root.UserRevokeRole(ctx, "alice", "reader")
	beforeConcurrent, err := root.AuthStatus(ctx)
	require.NoError(t, err)
	const concurrentCount = 16
	distinctErrors := runConcurrentClientOperations(concurrentCount, func(index int) error {
		_, operationErr := root.RoleAdd(ctx, fmt.Sprintf("concurrent-%02d", index))
		return operationErr
	})
	distinctOK := 0
	for _, operationErr := range distinctErrors {
		if operationErr == nil {
			distinctOK++
		}
	}
	afterDistinct, err := root.AuthStatus(ctx)
	require.NoError(t, err)
	duplicateErrors := runConcurrentClientOperations(concurrentCount, func(int) error {
		_, operationErr := root.RoleAdd(ctx, "concurrent-shared")
		return operationErr
	})
	duplicateOK := 0
	duplicateExists := 0
	for _, operationErr := range duplicateErrors {
		switch status.Code(operationErr) {
		case codes.OK:
			duplicateOK++
		case codes.FailedPrecondition:
			if status.Convert(operationErr).Message() == "etcdserver: role name already exists" {
				duplicateExists++
			}
		}
	}
	afterDuplicate, err := root.AuthStatus(ctx)
	require.NoError(t, err)

	_, err = root.RoleAdd(ctx, "lifecycle")
	require.NoError(t, err)
	_, err = root.RoleGrantPermission(ctx, "lifecycle", "/lifecycle/", clientv3.GetPrefixRangeEnd("/lifecycle/"), clientv3.PermissionType(clientv3.PermRead))
	require.NoError(t, err)
	_, err = root.RoleGrantPermission(ctx, "lifecycle", "/lifecycle/", clientv3.GetPrefixRangeEnd("/lifecycle/"), clientv3.PermissionType(clientv3.PermWrite))
	require.NoError(t, err)
	permissionRole, err := root.RoleGet(ctx, "lifecycle")
	require.NoError(t, err)
	permissionCount := len(permissionRole.Perm)
	permissionType := int32(-1)
	if permissionCount == 1 {
		permissionType = int32(permissionRole.Perm[0].PermType)
	}
	_, missingPermissionRevokeErr := root.RoleRevokePermission(ctx, "lifecycle", "/missing/", clientv3.GetPrefixRangeEnd("/missing/"))
	_, invalidPermissionRangeErr := root.RoleGrantPermission(ctx, "lifecycle", "z", "a", clientv3.PermissionType(clientv3.PermRead))
	_, err = root.UserGrantRole(ctx, "alice", "lifecycle")
	require.NoError(t, err)
	_, err = root.RoleDelete(ctx, "lifecycle")
	require.NoError(t, err)
	aliceAfterRoleDelete, err := root.UserGet(ctx, "alice")
	require.NoError(t, err)

	_, rootUserDeleteErr := root.UserDelete(ctx, "root")
	_, rootRoleRevokeErr := root.UserRevokeRole(ctx, "root", "root")
	_, rootRoleDeleteErr := root.RoleDelete(ctx, "root")

	oldAuth, err := root.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)
	oldTokenClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Token: oldAuth.Token,
	})
	require.NoError(t, err)
	defer oldTokenClient.Close()
	_, err = root.RoleAdd(ctx, "token-invalidator")
	require.NoError(t, err)
	_, oldTokenErr := oldTokenClient.RoleList(ctx)

	_, err = root.UserChangePassword(ctx, "alice", "alice-changed")
	require.NoError(t, err)
	_, oldPasswordErr := bootstrap.Authenticate(ctx, "alice", "alice-secret")
	_, newPasswordErr := bootstrap.Authenticate(ctx, "alice", "alice-changed")

	return authDifferentialOutcome{
		EnabledRevision:           statusAfterEnable.AuthRevision,
		DuplicateGrantRevision:    statusAfterDuplicateGrant.AuthRevision,
		DuplicateRoleRevision:     statusAfterDuplicateRole.AuthRevision,
		ImplicitRootRole:          authError(implicitRootRoleErr),
		WrongCredentials:          authError(wrongCredentialsErr),
		NoPassword:                authError(noPasswordErr),
		DuplicateRole:             authError(duplicateRoleErr),
		MissingRevoke:             authError(missingRevokeErr),
		ConcurrentDistinctOK:      distinctOK,
		ConcurrentDistinctDelta:   afterDistinct.AuthRevision - beforeConcurrent.AuthRevision,
		ConcurrentDuplicateOK:     duplicateOK,
		ConcurrentDuplicateExists: duplicateExists,
		ConcurrentDuplicateDelta:  afterDuplicate.AuthRevision - afterDistinct.AuthRevision,
		PermissionCount:           permissionCount,
		PermissionType:            permissionType,
		MissingPermissionRevoke:   authError(missingPermissionRevokeErr),
		InvalidPermissionRange:    authError(invalidPermissionRangeErr),
		AliceRolesAfterRoleDelete: len(aliceAfterRoleDelete.Roles),
		RootUserDelete:            authError(rootUserDeleteErr),
		RootRoleRevoke:            authError(rootRoleRevokeErr),
		RootRoleDelete:            authError(rootRoleDeleteErr),
		OldTokenAfterMutation:     authError(oldTokenErr),
		OldPassword:               authError(oldPasswordErr),
		NewPasswordOK:             newPasswordErr == nil,
		AnonymousCompact:          authError(anonymousCompactErr),
		UserCompact:               authError(userCompactErr),
		RootCompactOK:             rootCompactErr == nil,
		AnonymousRangeStream:      authError(anonymousRangeStreamErr),
		UserRangeStream:           authError(userRangeStreamErr),
		RootRangeStreamCount:      len(rootRangeStream.Kvs),
		AnonymousStatus:           authError(anonymousStatusErr),
		UserStatusOK:              userStatusErr == nil,
		AnonymousMemberList:       authError(anonymousMemberListErr),
		UserMemberListOK:          userMemberListErr == nil,
		AnonymousAlarmList:        authError(anonymousAlarmListErr),
		UserAlarmListOK:           userAlarmListErr == nil,
		UserAlarmDisarm:           authError(userAlarmDisarmErr),
		UserHash:                  authError(userHashErr),
		RootHashOK:                rootHashErr == nil,
		UserKeepAlive:             authError(userKeepAliveErr),
		RootKeepAliveOK:           rootKeepAliveErr == nil,
		AnonymousTTL:              authError(anonymousTTLErr),
		AnonymousTTLWithKeys:      authError(anonymousTTLWithKeysErr),
		UserTTLOK:                 userTTLErr == nil,
		UserTTLWithKeys:           authError(userTTLWithKeysErr),
		RootTTLContainsProtected:  rootTTLContainsProtected,
		KeepAliveStreamFirstOK:    keepAliveStreamFirstOK,
		KeepAliveAfterRoleRevoke:  authError(keepAliveAfterRoleRevokeErr),
		KeepAliveAfterRestoreOK:   keepAliveAfterRestoreOK,
		AnonymousLeaseList:        authError(anonymousLeaseListErr),
		UserLeaseList:             authError(userLeaseListErr),
		RootLeaseListContains:     rootLeaseListContains,
		UserLeaseListAfterRevoke:  userLeaseListAfterRevokeErr == nil,
		UserLeasedPut:             authError(userLeasedPutErr),
		UserLeasedTxnPut:          authError(userLeasedTxnPutErr),
		WriterTxnPutPrevKV:        authError(writerTxnPutPrevKVErr),
		WriterTxnValuePreserved:   string(writerTxnValue.Kvs[0].Value) == "before",
		NestedDeniedRange:         authError(nestedDeniedRangeErr),
		NestedDeniedPut:           authError(nestedDeniedPutErr),
		NestedDeniedDelete:        authError(nestedDeniedDeleteErr),
		NestedLeasedPut:           authError(nestedLeasedPutErr),
		NestedPutPrevKV:           authError(nestedPutPrevKVErr),
		NestedDeniedPutPreserved:  string(nestedDeniedPutValue.Kvs[0].Value) == "before",
		NestedDeletePreserved:     string(nestedDeleteValue.Kvs[0].Value) == "before",
		NestedPrevKVPreserved:     string(nestedPrevKVValue.Kvs[0].Value) == "before",
	}
}

// TestAuthDifferentialAgainstEtcd requires two empty, disposable instances.
// It deliberately enables authentication and must never target a production
// endpoint or an endpoint shared with Kubernetes.
func TestAuthDifferentialAgainstEtcd(t *testing.T) {
	referenceEndpoint := os.Getenv("ETCD_AUTH_DIFF_ENDPOINT")
	kubebrainEndpoint := os.Getenv("KUBEBRAIN_AUTH_DIFF_ENDPOINT")
	if referenceEndpoint == "" || kubebrainEndpoint == "" {
		t.Skip("set ETCD_AUTH_DIFF_ENDPOINT and KUBEBRAIN_AUTH_DIFF_ENDPOINT to empty disposable instances")
	}
	reference := collectAuthDifferentialOutcome(t, referenceEndpoint)
	require.True(t, reference.UserLeasedPut.PermissionDenied)
	require.True(t, reference.UserLeasedTxnPut.PermissionDenied)
	require.True(t, reference.AnonymousTTL.UserEmpty)
	require.True(t, reference.AnonymousTTLWithKeys.UserEmpty)
	require.True(t, reference.UserTTLOK)
	require.True(t, reference.UserTTLWithKeys.PermissionDenied)
	require.True(t, reference.RootTTLContainsProtected)
	require.True(t, reference.KeepAliveStreamFirstOK)
	require.True(
		t,
		reference.KeepAliveAfterRoleRevoke.PermissionDenied,
		"unexpected reference keepalive error after role revoke: %+v",
		reference.KeepAliveAfterRoleRevoke,
	)
	require.True(t, reference.KeepAliveAfterRestoreOK)
	require.True(t, reference.AnonymousLeaseList.UserEmpty)
	require.True(t, reference.UserLeaseList.PermissionDenied)
	require.True(t, reference.RootLeaseListContains)
	require.True(t, reference.UserLeaseListAfterRevoke)
	require.True(t, reference.WriterTxnPutPrevKV.PermissionDenied)
	require.True(t, reference.WriterTxnValuePreserved)
	actual := collectAuthDifferentialOutcome(t, kubebrainEndpoint)
	require.Equal(t, reference, actual)
}
