package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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

type delegatedRangeStreamAuthOutcome struct {
	Error  authErrorOutcome
	KVs    int
	Count  int64
	Header bool
	More   bool
}

const authTransitionRangeCount = 50

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
	AnonymousPointStream      delegatedRangeStreamAuthOutcome
	DeniedPointStream         delegatedRangeStreamAuthOutcome
	AllowedPointStream        delegatedRangeStreamAuthOutcome
	AllowedCountOnlyStream    delegatedRangeStreamAuthOutcome
	AllowedEmptyStream        delegatedRangeStreamAuthOutcome
	AllowedReversedStream     delegatedRangeStreamAuthOutcome
	AnonymousStatus           authErrorOutcome
	UserStatusOK              bool
	AnonymousMemberList       authErrorOutcome
	UserMemberListOK          bool
	AnonymousAlarmList        authErrorOutcome
	UserAlarmListOK           bool
	UserAlarmDisarm           authErrorOutcome
	UserHash                  authErrorOutcome
	RootHashOK                bool
	AnonymousDefragment       authErrorOutcome
	UserDefragment            authErrorOutcome
	AnonymousSnapshot         authErrorOutcome
	UserSnapshot              authErrorOutcome
	AnonymousRawHash          authErrorOutcome
	UserRawHash               authErrorOutcome
	AnonymousMoveLeader       authErrorOutcome
	UserMoveLeader            authErrorOutcome
	AnonymousDowngrade        authErrorOutcome
	UserDowngrade             authErrorOutcome
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
	AnonymousKeepAliveBefore  bool
	KeepAliveSendAfterEnable  bool
	KeepAliveAfterEnable      authErrorOutcome
	AnonymousWatchBeforeAuth  bool
	ExistingWatchAfterEnable  bool
	NewWatchAfterEnableCancel bool
	NewWatchAfterEnable       string
	WatchStreamFirstCreated   bool
	WatchCreateAfterRevoke    string
	ExistingWatchAfterRevoke  bool
	WatchCreateAfterRestore   bool
	RestoredWatchFanoutOK     bool
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
	DisableKeepAliveBefore    bool
	DisableKeepAliveAfter     bool
	DisableExistingWatch      bool
	DisableNewWatchCreated    bool
	DisableNewWatchEvent      bool
	DisableRangeFirst         bool
	DisableRangeAfter         bool
	DisableRangeCount         int
	DisableRangeTerminal      bool
	DisableRangeError         authErrorOutcome
	AuthDisabledStatus        bool
	PreAuthRangeStreamFirst   bool
	RangeStreamAfterChunks    bool
	RangeStreamAfterCount     int
	RangeStreamAfterTerminal  bool
	RangeStreamAfterEOF       bool
	AuthRangeStreamFirst      bool
	AuthRangeStreamAfter      bool
	AuthRangeStreamCount      int
	AuthRangeStreamTerminal   bool
	AuthRangeStreamError      authErrorOutcome
	SnapshotStreamFirst       bool
	SnapshotStreamAfter       bool
	SnapshotStreamTerminal    bool
	SnapshotStreamDigest      bool
	SnapshotStreamEOF         bool
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
	message := status.Convert(err).Message()
	return authErrorOutcome{
		Code:             code,
		Message:          message,
		PermissionDenied: code == codes.PermissionDenied || errors.Is(err, rpctypes.ErrPermissionDenied),
		UserEmpty: errors.Is(err, rpctypes.ErrUserEmpty) ||
			(code == status.Code(rpctypes.ErrGRPCUserEmpty) &&
				message == status.Convert(rpctypes.ErrGRPCUserEmpty).Message()),
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

func delegatedAuthRangeStream(
	t *testing.T,
	ctx context.Context,
	cli *clientv3.Client,
	request *etcdserverpb.RangeRequest,
) delegatedRangeStreamAuthOutcome {
	t.Helper()
	stream, err := etcdserverpb.NewKVClient(cli.ActiveConnection()).RangeStream(ctx, request)
	if err != nil {
		return delegatedRangeStreamAuthOutcome{Error: authError(err)}
	}
	outcome := delegatedRangeStreamAuthOutcome{}
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return outcome
		}
		if recvErr != nil {
			outcome.Error = authError(recvErr)
			return outcome
		}
		rangeResponse := response.GetRangeResponse()
		if rangeResponse == nil {
			continue
		}
		outcome.KVs += len(rangeResponse.Kvs)
		if rangeResponse.Header != nil {
			outcome.Header = true
			outcome.Count = rangeResponse.Count
			outcome.More = rangeResponse.More
		}
	}
}

func collectAuthDifferentialOutcome(t *testing.T, endpoint string) authDifferentialOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")
	transitionRangeValue := strings.Repeat("x", 256*1024)
	for i := 0; i < authTransitionRangeCount; i++ {
		_, putErr := bootstrap.Put(
			ctx,
			fmt.Sprintf("/auth-transition/rangestream/%02d", i),
			transitionRangeValue,
		)
		require.NoError(t, putErr)
	}
	preAuthRangeStream, err := etcdserverpb.NewKVClient(bootstrap.ActiveConnection()).RangeStream(
		ctx,
		&etcdserverpb.RangeRequest{
			Key:      []byte("/auth-transition/rangestream/"),
			RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/auth-transition/rangestream/")),
		},
	)
	require.NoError(t, err)
	preAuthRangeFirst, err := preAuthRangeStream.Recv()
	require.NoError(t, err)
	preAuthRangeStreamFirst := preAuthRangeFirst.RangeResponse != nil &&
		len(preAuthRangeFirst.RangeResponse.Kvs) > 0 &&
		preAuthRangeFirst.RangeResponse.Header == nil
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
	preAuthKeepAlive, err := etcdserverpb.NewLeaseClient(bootstrap.ActiveConnection()).LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, preAuthKeepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(protectedLease.ID)}))
	preAuthKeepAliveResponse, err := preAuthKeepAlive.Recv()
	require.NoError(t, err)
	anonymousKeepAliveBefore := preAuthKeepAliveResponse.ID == int64(protectedLease.ID) &&
		preAuthKeepAliveResponse.TTL > 0
	preAuthWatch, err := etcdserverpb.NewWatchClient(bootstrap.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	const (
		preAuthWatchID  = int64(91)
		postAuthWatchID = int64(92)
	)
	watchCreateRequest := func(id int64) *etcdserverpb.WatchRequest {
		return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/auth-transition/watch"), WatchId: id,
			},
		}}
	}
	require.NoError(t, preAuthWatch.Send(watchCreateRequest(preAuthWatchID)))
	preAuthCreated, err := preAuthWatch.Recv()
	require.NoError(t, err)
	anonymousWatchBeforeAuth := preAuthCreated.Created && !preAuthCreated.Canceled &&
		preAuthCreated.WatchId == preAuthWatchID

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
	_, err = bootstrap.RoleGrantPermission(
		ctx,
		"allowed",
		"/auth-transition/rangestream/",
		clientv3.GetPrefixRangeEnd("/auth-transition/rangestream/"),
		clientv3.PermissionType(clientv3.PermRead),
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
	rangeStreamAfterChunks := 0
	rangeStreamAfterCount := len(preAuthRangeFirst.GetRangeResponse().Kvs)
	rangeStreamAfterTerminal := false
	rangeStreamAfterEOF := false
	for {
		response, receiveErr := preAuthRangeStream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			rangeStreamAfterEOF = true
			break
		}
		require.NoError(t, receiveErr)
		rangeStreamAfterChunks++
		rangeResponse := response.GetRangeResponse()
		require.NotNil(t, rangeResponse)
		rangeStreamAfterCount += len(rangeResponse.Kvs)
		if rangeResponse.Header != nil && rangeResponse.Count == authTransitionRangeCount && !rangeResponse.More {
			rangeStreamAfterTerminal = true
		}
	}

	_, wrongCredentialsErr := bootstrap.Authenticate(ctx, "missing", "wrong")
	_, noPasswordErr := bootstrap.Authenticate(ctx, "nopass", "password")
	root := authClient(t, endpoint, "root", "root-secret")
	alice := authClient(t, endpoint, "alice", "alice-secret")
	writer := authClient(t, endpoint, "writer", "writer-secret")
	transitionAuthRangeStream, err := etcdserverpb.NewKVClient(alice.ActiveConnection()).RangeStream(
		ctx,
		&etcdserverpb.RangeRequest{
			Key:      []byte("/auth-transition/rangestream/"),
			RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/auth-transition/rangestream/")),
		},
	)
	require.NoError(t, err)
	authRangeFirst, err := transitionAuthRangeStream.Recv()
	require.NoError(t, err)
	authRangeStreamFirst := authRangeFirst.RangeResponse != nil &&
		len(authRangeFirst.RangeResponse.Kvs) > 0 && authRangeFirst.RangeResponse.Header == nil
	_, err = root.RoleAdd(ctx, "range-stream-revision-bump")
	require.NoError(t, err)
	authRangeStreamAfterChunks := 0
	authRangeStreamCount := len(authRangeFirst.GetRangeResponse().Kvs)
	authRangeStreamTerminal := false
	var authRangeStreamErr error
	for {
		response, receiveErr := transitionAuthRangeStream.Recv()
		if receiveErr != nil {
			authRangeStreamErr = receiveErr
			break
		}
		authRangeStreamAfterChunks++
		rangeResponse := response.GetRangeResponse()
		require.NotNil(t, rangeResponse)
		authRangeStreamCount += len(rangeResponse.Kvs)
		if rangeResponse.Header != nil && rangeResponse.Count == authTransitionRangeCount && !rangeResponse.More {
			authRangeStreamTerminal = true
		}
	}
	snapshotStream, err := etcdserverpb.NewMaintenanceClient(root.ActiveConnection()).Snapshot(
		ctx, &etcdserverpb.SnapshotRequest{},
	)
	require.NoError(t, err)
	snapshotFirst, err := snapshotStream.Recv()
	require.NoError(t, err)
	snapshotStreamFirst := len(snapshotFirst.Blob) > 0 && snapshotFirst.RemainingBytes > 0 &&
		snapshotFirst.Version != ""
	_, err = root.RoleAdd(ctx, "snapshot-stream-revision-bump")
	require.NoError(t, err)
	snapshotHash := sha256.New()
	snapshotPending := snapshotFirst
	snapshotStreamAfterResponses := 0
	snapshotStreamEOF := false
	for {
		response, receiveErr := snapshotStream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			snapshotStreamEOF = true
			break
		}
		require.NoError(t, receiveErr)
		snapshotStreamAfterResponses++
		_, _ = snapshotHash.Write(snapshotPending.Blob)
		snapshotPending = response
	}
	snapshotStreamTerminal := snapshotPending.RemainingBytes == 0 &&
		len(snapshotPending.Blob) == sha256.Size && snapshotPending.Version != ""
	snapshotStreamDigest := snapshotStreamTerminal &&
		string(snapshotHash.Sum(nil)) == string(snapshotPending.Blob)
	keepAliveSendAfterEnableErr := preAuthKeepAlive.Send(
		&etcdserverpb.LeaseKeepAliveRequest{ID: int64(protectedLease.ID)},
	)
	var keepAliveAfterEnableErr error
	if keepAliveSendAfterEnableErr == nil {
		_, keepAliveAfterEnableErr = preAuthKeepAlive.Recv()
	}
	_ = preAuthKeepAlive.CloseSend()
	_, err = root.Put(ctx, "/auth-transition/watch", "after-enable")
	require.NoError(t, err)
	preAuthEvent, err := preAuthWatch.Recv()
	require.NoError(t, err)
	existingWatchAfterEnable := preAuthEvent.WatchId == preAuthWatchID &&
		len(preAuthEvent.Events) == 1 &&
		string(preAuthEvent.Events[0].Kv.Value) == "after-enable"
	require.NoError(t, preAuthWatch.Send(watchCreateRequest(postAuthWatchID)))
	postAuthCreated, err := preAuthWatch.Recv()
	require.NoError(t, err)
	newWatchAfterEnableCancel := postAuthCreated.Created && postAuthCreated.Canceled &&
		postAuthCreated.WatchId == clientv3.InvalidWatchID
	newWatchAfterEnable := postAuthCreated.CancelReason
	require.NoError(t, preAuthWatch.CloseSend())
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
	_, err = root.Put(ctx, "/auth-allowed/delegated", "value")
	require.NoError(t, err)
	allowedPrefixEnd := clientv3.GetPrefixRangeEnd("/auth-allowed/")
	anonymousPointStream := delegatedAuthRangeStream(t, ctx, bootstrap, &etcdserverpb.RangeRequest{
		Key: []byte("/auth-allowed/delegated"),
	})
	deniedPointStream := delegatedAuthRangeStream(t, ctx, alice, &etcdserverpb.RangeRequest{
		Key: []byte("/auth-protected/leased"),
	})
	allowedPointStream := delegatedAuthRangeStream(t, ctx, alice, &etcdserverpb.RangeRequest{
		Key: []byte("/auth-allowed/delegated"),
	})
	allowedCountOnlyStream := delegatedAuthRangeStream(t, ctx, alice, &etcdserverpb.RangeRequest{
		Key: []byte("/auth-allowed/"), RangeEnd: []byte(allowedPrefixEnd), CountOnly: true,
	})
	allowedEmptyStream := delegatedAuthRangeStream(t, ctx, alice, &etcdserverpb.RangeRequest{
		Key: []byte("/auth-allowed/delegated"), RangeEnd: []byte("/auth-allowed/delegated"),
	})
	allowedReversedStream := delegatedAuthRangeStream(t, ctx, alice, &etcdserverpb.RangeRequest{
		Key: []byte("/auth-allowed/z"), RangeEnd: []byte("/auth-allowed/a"),
	})
	_, anonymousStatusErr := bootstrap.Status(ctx, bootstrap.Endpoints()[0])
	_, userStatusErr := alice.Status(ctx, alice.Endpoints()[0])
	_, anonymousMemberListErr := bootstrap.MemberList(ctx)
	_, userMemberListErr := alice.MemberList(ctx)
	_, anonymousAlarmListErr := bootstrap.AlarmList(ctx)
	_, userAlarmListErr := alice.AlarmList(ctx)
	_, userAlarmDisarmErr := alice.AlarmDisarm(ctx, &clientv3.AlarmMember{})
	_, userHashErr := alice.HashKV(ctx, alice.Endpoints()[0], 0)
	_, rootHashErr := root.HashKV(ctx, root.Endpoints()[0], 0)
	_, anonymousDefragmentErr := bootstrap.Defragment(ctx, bootstrap.Endpoints()[0])
	_, userDefragmentErr := alice.Defragment(ctx, alice.Endpoints()[0])
	_, anonymousSnapshotErr := bootstrap.SnapshotWithVersion(ctx)
	_, userSnapshotErr := alice.SnapshotWithVersion(ctx)
	_, anonymousRawHashErr := etcdserverpb.NewMaintenanceClient(
		bootstrap.ActiveConnection(),
	).Hash(ctx, &etcdserverpb.HashRequest{})
	_, userRawHashErr := etcdserverpb.NewMaintenanceClient(
		alice.ActiveConnection(),
	).Hash(ctx, &etcdserverpb.HashRequest{})
	_, anonymousMoveLeaderErr := bootstrap.MoveLeader(ctx, 0)
	_, userMoveLeaderErr := alice.MoveLeader(ctx, 0)
	_, anonymousDowngradeErr := bootstrap.Downgrade(ctx, clientv3.DowngradeValidate, "3.6")
	_, userDowngradeErr := alice.Downgrade(ctx, clientv3.DowngradeValidate, "3.6")
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
	watchStream, err := etcdserverpb.NewWatchClient(alice.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	const (
		// Watch ID 0 asks the server to allocate the first stream-local ID. Keep
		// this watch alive across the denied create below to pin etcd f1d4935e9:
		// an auth failure must not mistake the existing automatic ID 0 for the
		// failed create and cancel it.
		firstWatchID    = int64(0)
		restoredWatchID = int64(103)
	)
	watchCreate := func(id int64) *etcdserverpb.WatchRequest {
		return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/auth-allowed/dynamic-watch"), WatchId: id,
			},
		}}
	}
	require.NoError(t, watchStream.Send(watchCreate(firstWatchID)))
	firstWatchCreated, err := watchStream.Recv()
	require.NoError(t, err)
	watchStreamFirstCreated := firstWatchCreated.Created &&
		!firstWatchCreated.Canceled && firstWatchCreated.WatchId == firstWatchID
	_, err = root.RoleRevokePermission(
		ctx, "allowed", "/auth-allowed/", clientv3.GetPrefixRangeEnd("/auth-allowed/"),
	)
	require.NoError(t, err)
	require.NoError(t, watchStream.Send(watchCreate(102)))
	watchCreateAfterRevoke, err := watchStream.Recv()
	require.NoError(t, err)
	_, err = root.Put(ctx, "/auth-allowed/dynamic-watch", "during-revoke")
	require.NoError(t, err)
	existingWatchAfterRevoke, err := watchStream.Recv()
	require.NoError(t, err)
	existingWatchAfterRevokeOK := existingWatchAfterRevoke.WatchId == firstWatchID &&
		len(existingWatchAfterRevoke.Events) == 1 &&
		string(existingWatchAfterRevoke.Events[0].Kv.Value) == "during-revoke"
	_, err = root.RoleGrantPermission(
		ctx,
		"allowed",
		"/auth-allowed/",
		clientv3.GetPrefixRangeEnd("/auth-allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	require.NoError(t, watchStream.Send(watchCreate(restoredWatchID)))
	watchCreateAfterRestore, err := watchStream.Recv()
	require.NoError(t, err)
	watchCreateAfterRestoreOK := watchCreateAfterRestore.Created &&
		!watchCreateAfterRestore.Canceled && watchCreateAfterRestore.WatchId == restoredWatchID
	_, err = root.Put(ctx, "/auth-allowed/dynamic-watch", "after-restore")
	require.NoError(t, err)
	restoredWatchEvents := make(map[int64]bool, 2)
	for range 2 {
		response, receiveErr := watchStream.Recv()
		require.NoError(t, receiveErr)
		if len(response.Events) == 1 &&
			string(response.Events[0].Kv.Value) == "after-restore" {
			restoredWatchEvents[response.WatchId] = true
		}
	}
	restoredWatchFanoutOK := restoredWatchEvents[firstWatchID] &&
		restoredWatchEvents[restoredWatchID]
	require.NoError(t, watchStream.CloseSend())

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
	disableLease, err := alice.Grant(ctx, 30)
	require.NoError(t, err)
	_, err = alice.Put(ctx, "/auth-allowed/disable-lease", "value", clientv3.WithLease(disableLease.ID))
	require.NoError(t, err)
	disableKeepAlive, err := etcdserverpb.NewLeaseClient(alice.ActiveConnection()).LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, disableKeepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(disableLease.ID)}))
	disableKeepAliveFirst, err := disableKeepAlive.Recv()
	require.NoError(t, err)
	disableKeepAliveBefore := disableKeepAliveFirst.ID == int64(disableLease.ID) && disableKeepAliveFirst.TTL > 0

	disableWatch, err := etcdserverpb.NewWatchClient(alice.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	const (
		disableExistingWatchID = int64(201)
		disableNewWatchID      = int64(202)
	)
	disableWatchCreate := func(id int64, key string) *etcdserverpb.WatchRequest {
		return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte(key), WatchId: id},
		}}
	}
	require.NoError(t, disableWatch.Send(disableWatchCreate(disableExistingWatchID, "/auth-allowed/disable-watch")))
	disableWatchFirst, err := disableWatch.Recv()
	require.NoError(t, err)
	require.True(t, disableWatchFirst.Created && !disableWatchFirst.Canceled &&
		disableWatchFirst.WatchId == disableExistingWatchID)

	_, err = root.UserChangePassword(ctx, "alice", "alice-changed")
	require.NoError(t, err)
	_, oldPasswordErr := bootstrap.Authenticate(ctx, "alice", "alice-secret")
	_, newPasswordErr := bootstrap.Authenticate(ctx, "alice", "alice-changed")
	aliceChanged := authClient(t, endpoint, "alice", "alice-changed")
	disableRangeStream, err := etcdserverpb.NewKVClient(aliceChanged.ActiveConnection()).RangeStream(
		ctx,
		&etcdserverpb.RangeRequest{
			Key:      []byte("/auth-transition/rangestream/"),
			RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/auth-transition/rangestream/")),
		},
	)
	require.NoError(t, err)
	disableRangeFirstResponse, err := disableRangeStream.Recv()
	require.NoError(t, err)
	disableRangeFirst := disableRangeFirstResponse.RangeResponse != nil &&
		len(disableRangeFirstResponse.RangeResponse.Kvs) > 0 &&
		disableRangeFirstResponse.RangeResponse.Header == nil
	_, err = root.AuthDisable(ctx)
	require.NoError(t, err)
	disableRangeAfterChunks := 0
	disableRangeCount := len(disableRangeFirstResponse.GetRangeResponse().Kvs)
	disableRangeTerminal := false
	var disableRangeErr error
	for {
		response, receiveErr := disableRangeStream.Recv()
		if receiveErr != nil {
			disableRangeErr = receiveErr
			break
		}
		disableRangeAfterChunks++
		rangeResponse := response.GetRangeResponse()
		require.NotNil(t, rangeResponse)
		disableRangeCount += len(rangeResponse.Kvs)
		if rangeResponse.Header != nil && rangeResponse.Count == authTransitionRangeCount && !rangeResponse.More {
			disableRangeTerminal = true
		}
	}
	disabledStatus, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)

	require.NoError(t, disableKeepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(disableLease.ID)}))
	disableKeepAliveSecond, disableKeepAliveAfterErr := disableKeepAlive.Recv()
	disableKeepAliveAfter := disableKeepAliveAfterErr == nil &&
		disableKeepAliveSecond.ID == int64(disableLease.ID) && disableKeepAliveSecond.TTL > 0
	_ = disableKeepAlive.CloseSend()

	_, err = bootstrap.Put(ctx, "/auth-allowed/disable-watch", "after-disable")
	require.NoError(t, err)
	disableExistingEvent, err := disableWatch.Recv()
	require.NoError(t, err)
	disableExistingWatch := disableExistingEvent.WatchId == disableExistingWatchID &&
		len(disableExistingEvent.Events) == 1 &&
		string(disableExistingEvent.Events[0].Kv.Value) == "after-disable"
	require.NoError(t, disableWatch.Send(disableWatchCreate(disableNewWatchID, "/auth-allowed/disable-new-watch")))
	disableNewCreated, err := disableWatch.Recv()
	require.NoError(t, err)
	disableNewWatchCreated := disableNewCreated.Created && !disableNewCreated.Canceled &&
		disableNewCreated.WatchId == disableNewWatchID
	_, err = bootstrap.Put(ctx, "/auth-allowed/disable-new-watch", "new-after-disable")
	require.NoError(t, err)
	disableNewEvent, err := disableWatch.Recv()
	require.NoError(t, err)
	disableNewWatchEvent := disableNewEvent.WatchId == disableNewWatchID &&
		len(disableNewEvent.Events) == 1 &&
		string(disableNewEvent.Events[0].Kv.Value) == "new-after-disable"
	_ = disableWatch.CloseSend()
	_, err = bootstrap.Revoke(ctx, disableLease.ID)
	require.NoError(t, err)

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
		AnonymousPointStream:      anonymousPointStream,
		DeniedPointStream:         deniedPointStream,
		AllowedPointStream:        allowedPointStream,
		AllowedCountOnlyStream:    allowedCountOnlyStream,
		AllowedEmptyStream:        allowedEmptyStream,
		AllowedReversedStream:     allowedReversedStream,
		AnonymousStatus:           authError(anonymousStatusErr),
		UserStatusOK:              userStatusErr == nil,
		AnonymousMemberList:       authError(anonymousMemberListErr),
		UserMemberListOK:          userMemberListErr == nil,
		AnonymousAlarmList:        authError(anonymousAlarmListErr),
		UserAlarmListOK:           userAlarmListErr == nil,
		UserAlarmDisarm:           authError(userAlarmDisarmErr),
		UserHash:                  authError(userHashErr),
		RootHashOK:                rootHashErr == nil,
		AnonymousDefragment:       authError(anonymousDefragmentErr),
		UserDefragment:            authError(userDefragmentErr),
		AnonymousSnapshot:         authError(anonymousSnapshotErr),
		UserSnapshot:              authError(userSnapshotErr),
		AnonymousRawHash:          authError(anonymousRawHashErr),
		UserRawHash:               authError(userRawHashErr),
		AnonymousMoveLeader:       authError(anonymousMoveLeaderErr),
		UserMoveLeader:            authError(userMoveLeaderErr),
		AnonymousDowngrade:        authError(anonymousDowngradeErr),
		UserDowngrade:             authError(userDowngradeErr),
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
		AnonymousKeepAliveBefore:  anonymousKeepAliveBefore,
		KeepAliveSendAfterEnable:  keepAliveSendAfterEnableErr == nil,
		KeepAliveAfterEnable:      authError(keepAliveAfterEnableErr),
		AnonymousWatchBeforeAuth:  anonymousWatchBeforeAuth,
		ExistingWatchAfterEnable:  existingWatchAfterEnable,
		NewWatchAfterEnableCancel: newWatchAfterEnableCancel,
		NewWatchAfterEnable:       newWatchAfterEnable,
		WatchStreamFirstCreated:   watchStreamFirstCreated,
		WatchCreateAfterRevoke:    watchCreateAfterRevoke.CancelReason,
		ExistingWatchAfterRevoke:  existingWatchAfterRevokeOK,
		WatchCreateAfterRestore:   watchCreateAfterRestoreOK,
		RestoredWatchFanoutOK:     restoredWatchFanoutOK,
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
		DisableKeepAliveBefore:    disableKeepAliveBefore,
		DisableKeepAliveAfter:     disableKeepAliveAfter,
		DisableExistingWatch:      disableExistingWatch,
		DisableNewWatchCreated:    disableNewWatchCreated,
		DisableNewWatchEvent:      disableNewWatchEvent,
		DisableRangeFirst:         disableRangeFirst,
		DisableRangeAfter:         disableRangeAfterChunks > 0,
		DisableRangeCount:         disableRangeCount,
		DisableRangeTerminal:      disableRangeTerminal,
		DisableRangeError:         authError(disableRangeErr),
		AuthDisabledStatus:        !disabledStatus.Enabled,
		PreAuthRangeStreamFirst:   preAuthRangeStreamFirst,
		RangeStreamAfterChunks:    rangeStreamAfterChunks > 0,
		RangeStreamAfterCount:     rangeStreamAfterCount,
		RangeStreamAfterTerminal:  rangeStreamAfterTerminal,
		RangeStreamAfterEOF:       rangeStreamAfterEOF,
		AuthRangeStreamFirst:      authRangeStreamFirst,
		AuthRangeStreamAfter:      authRangeStreamAfterChunks > 0,
		AuthRangeStreamCount:      authRangeStreamCount,
		AuthRangeStreamTerminal:   authRangeStreamTerminal,
		AuthRangeStreamError:      authError(authRangeStreamErr),
		SnapshotStreamFirst:       snapshotStreamFirst,
		SnapshotStreamAfter:       snapshotStreamAfterResponses > 0,
		SnapshotStreamTerminal:    snapshotStreamTerminal,
		SnapshotStreamDigest:      snapshotStreamDigest,
		SnapshotStreamEOF:         snapshotStreamEOF,
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
	for name, outcome := range map[string]authErrorOutcome{
		"Defragment": reference.AnonymousDefragment,
		"Snapshot":   reference.AnonymousSnapshot,
		"Hash":       reference.AnonymousRawHash,
		"MoveLeader": reference.AnonymousMoveLeader,
		"Downgrade":  reference.AnonymousDowngrade,
	} {
		require.True(t, outcome.UserEmpty, "anonymous %s: %+v", name, outcome)
	}
	for name, outcome := range map[string]authErrorOutcome{
		"Defragment": reference.UserDefragment,
		"Snapshot":   reference.UserSnapshot,
		"Hash":       reference.UserRawHash,
		"MoveLeader": reference.UserMoveLeader,
		"Downgrade":  reference.UserDowngrade,
	} {
		require.True(t, outcome.PermissionDenied, "non-root %s: %+v", name, outcome)
	}
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
	require.True(t, reference.AnonymousKeepAliveBefore)
	require.True(t, reference.KeepAliveSendAfterEnable)
	require.True(t, reference.KeepAliveAfterEnable.UserEmpty)
	require.Equal(t, codes.InvalidArgument, reference.KeepAliveAfterEnable.Code)
	require.True(t, reference.AnonymousWatchBeforeAuth)
	require.True(t, reference.ExistingWatchAfterEnable)
	require.True(t, reference.NewWatchAfterEnableCancel)
	require.Equal(t, rpctypes.ErrGRPCUserEmpty.Error(), reference.NewWatchAfterEnable)
	require.True(t, reference.WatchStreamFirstCreated)
	require.Equal(t, rpctypes.ErrGRPCPermissionDenied.Error(), reference.WatchCreateAfterRevoke)
	require.True(t, reference.ExistingWatchAfterRevoke)
	require.True(t, reference.WatchCreateAfterRestore)
	require.True(t, reference.RestoredWatchFanoutOK)
	require.True(t, reference.AnonymousLeaseList.UserEmpty)
	require.True(t, reference.UserLeaseList.PermissionDenied)
	require.True(t, reference.RootLeaseListContains)
	require.True(t, reference.UserLeaseListAfterRevoke)
	require.True(t, reference.WriterTxnPutPrevKV.PermissionDenied)
	require.True(t, reference.WriterTxnValuePreserved)
	require.True(t, reference.DisableKeepAliveBefore)
	require.True(t, reference.DisableKeepAliveAfter)
	require.True(t, reference.DisableExistingWatch)
	require.True(t, reference.DisableNewWatchCreated)
	require.True(t, reference.DisableNewWatchEvent)
	require.True(t, reference.DisableRangeFirst)
	require.True(t, reference.DisableRangeAfter)
	require.Equal(t, authTransitionRangeCount, reference.DisableRangeCount)
	require.True(t, reference.DisableRangeTerminal)
	require.Equal(t, codes.InvalidArgument, reference.DisableRangeError.Code)
	require.Equal(t, status.Convert(rpctypes.ErrGRPCAuthOldRevision).Message(), reference.DisableRangeError.Message)
	require.True(t, reference.AuthDisabledStatus)
	require.True(t, reference.PreAuthRangeStreamFirst)
	require.True(t, reference.RangeStreamAfterChunks)
	require.Equal(t, authTransitionRangeCount, reference.RangeStreamAfterCount)
	require.True(t, reference.RangeStreamAfterTerminal)
	require.True(t, reference.RangeStreamAfterEOF)
	require.True(t, reference.AuthRangeStreamFirst)
	require.True(t, reference.AuthRangeStreamAfter)
	require.Equal(t, authTransitionRangeCount, reference.AuthRangeStreamCount)
	require.True(t, reference.AuthRangeStreamTerminal)
	require.Equal(t, codes.InvalidArgument, reference.AuthRangeStreamError.Code)
	require.Equal(t, status.Convert(rpctypes.ErrGRPCAuthOldRevision).Message(), reference.AuthRangeStreamError.Message)
	require.True(t, reference.SnapshotStreamFirst)
	require.True(t, reference.SnapshotStreamAfter)
	require.True(t, reference.SnapshotStreamTerminal)
	require.True(t, reference.SnapshotStreamDigest)
	require.True(t, reference.SnapshotStreamEOF)
	require.True(t, reference.AnonymousPointStream.Error.UserEmpty)
	require.Equal(t, codes.InvalidArgument, reference.AnonymousPointStream.Error.Code)
	require.True(t, reference.DeniedPointStream.Error.PermissionDenied)
	require.Equal(t, codes.PermissionDenied, reference.DeniedPointStream.Error.Code)
	require.Equal(t, delegatedRangeStreamAuthOutcome{
		KVs: 1, Count: 1, Header: true,
	}, reference.AllowedPointStream)
	require.Equal(t, delegatedRangeStreamAuthOutcome{
		Count: 1, Header: true,
	}, reference.AllowedCountOnlyStream)
	require.Equal(t, delegatedRangeStreamAuthOutcome{Header: true}, reference.AllowedEmptyStream)
	require.Equal(t, delegatedRangeStreamAuthOutcome{Header: true}, reference.AllowedReversedStream)
	actual := collectAuthDifferentialOutcome(t, kubebrainEndpoint)
	require.Equal(t, reference, actual)
}
