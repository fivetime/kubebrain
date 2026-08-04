package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type jwtAuthOutcome struct {
	TokenSegments       int
	InitialPut          bool
	RangeStreamFirst    bool
	RangeStreamAfter    bool
	RangeStreamCount    int
	RangeStreamTerminal bool
	RangeStreamError    authErrorOutcome
	OldToken            authErrorOutcome
	ReauthPut           bool
}

func TestJWTAuthDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_JWT_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_JWT_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_JWT_ETCD_ENDPOINT and KUBEBRAIN_JWT_ETCD_ENDPOINT")
	}
	referenceResult := runJWTAuthScenario(t, reference, "reference")
	require.Equal(t, 3, referenceResult.TokenSegments)
	require.True(t, referenceResult.InitialPut)
	require.True(t, referenceResult.RangeStreamFirst)
	require.True(t, referenceResult.RangeStreamAfter)
	require.Equal(t, authTransitionRangeCount, referenceResult.RangeStreamCount)
	require.True(t, referenceResult.RangeStreamTerminal)
	require.Equal(t, codes.InvalidArgument, referenceResult.RangeStreamError.Code)
	require.Equal(t, status.Convert(rpctypes.ErrGRPCAuthOldRevision).Message(), referenceResult.RangeStreamError.Message)
	// clientv3 converts the raw InvalidArgument status to the sentinel
	// rpctypes.ErrAuthOldRevision, whose status code is Unknown to callers.
	require.Equal(t, codes.Unknown, referenceResult.OldToken.Code)
	require.Equal(t, status.Convert(rpctypes.ErrGRPCAuthOldRevision).Message(), referenceResult.OldToken.Message)
	require.True(t, referenceResult.ReauthPut)
	kubebrainResult := runJWTAuthScenario(t, kubebrain, "kubebrain")
	require.Equal(t, referenceResult, kubebrainResult)
}

func runJWTAuthScenario(t *testing.T, endpoint, name string) jwtAuthOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")
	key := "/jwt-differential/" + name
	rangePrefix := key + "/rangestream/"
	_, err := bootstrap.UserAdd(ctx, "root", "root-secret")
	if err != nil {
		require.Contains(t, err.Error(), "user name already exists")
	}
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		root, clientErr := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
			Username: "root", Password: "root-secret",
		})
		require.NoError(t, clientErr)
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
		_, _ = bootstrap.Delete(cleanupCtx, key)
		_, _ = bootstrap.Delete(cleanupCtx, rangePrefix, clientv3.WithPrefix())
		require.NoError(t, root.Close())
		users, listUsersErr := bootstrap.UserList(cleanupCtx)
		require.NoError(t, listUsersErr)
		require.Empty(t, users.Users)
		roles, listRolesErr := bootstrap.RoleList(cleanupCtx)
		require.NoError(t, listRolesErr)
		require.Empty(t, roles.Roles)
		remaining, getErr := bootstrap.Get(cleanupCtx, key)
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
		remainingRange, getRangeErr := bootstrap.Get(cleanupCtx, rangePrefix, clientv3.WithPrefix())
		require.NoError(t, getRangeErr)
		require.Zero(t, remainingRange.Count)
	})
	rangeValue := strings.Repeat("x", 256*1024)
	for i := 0; i < authTransitionRangeCount; i++ {
		_, err = bootstrap.Put(ctx, fmt.Sprintf("%s%02d", rangePrefix, i), rangeValue)
		require.NoError(t, err)
	}
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	auth, err := bootstrap.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)
	oldClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Token: auth.Token,
	})
	require.NoError(t, err)
	defer oldClient.Close()
	_, initialPutErr := oldClient.Put(ctx, key, "initial")

	root := authClient(t, endpoint, "root", "root-secret")
	rangeStream, err := etcdserverpb.NewKVClient(oldClient.ActiveConnection()).RangeStream(
		ctx,
		&etcdserverpb.RangeRequest{
			Key:      []byte(rangePrefix),
			RangeEnd: []byte(clientv3.GetPrefixRangeEnd(rangePrefix)),
		},
	)
	require.NoError(t, err)
	first, err := rangeStream.Recv()
	require.NoError(t, err)
	rangeStreamFirst := first.RangeResponse != nil &&
		len(first.RangeResponse.Kvs) > 0 && first.RangeResponse.Header == nil
	_, err = root.RoleAdd(ctx, fmt.Sprintf("jwt-revision-invalidator-%s-%d", name, time.Now().UnixNano()))
	require.NoError(t, err)
	rangeStreamAfterChunks := 0
	rangeStreamCount := len(first.GetRangeResponse().Kvs)
	rangeStreamTerminal := false
	var rangeStreamErr error
	for {
		response, receiveErr := rangeStream.Recv()
		if receiveErr != nil {
			rangeStreamErr = receiveErr
			break
		}
		rangeStreamAfterChunks++
		rangeResponse := response.GetRangeResponse()
		require.NotNil(t, rangeResponse)
		rangeStreamCount += len(rangeResponse.Kvs)
		if rangeResponse.Header != nil && rangeResponse.Count == authTransitionRangeCount && !rangeResponse.More {
			rangeStreamTerminal = true
		}
	}
	_, oldTokenErr := oldClient.Get(ctx, key)
	reauthenticated := authClient(t, endpoint, "root", "root-secret")
	_, reauthPutErr := reauthenticated.Put(ctx, key, "reauthenticated")

	return jwtAuthOutcome{
		TokenSegments:       len(strings.Split(auth.Token, ".")),
		InitialPut:          initialPutErr == nil,
		RangeStreamFirst:    rangeStreamFirst,
		RangeStreamAfter:    rangeStreamAfterChunks > 0,
		RangeStreamCount:    rangeStreamCount,
		RangeStreamTerminal: rangeStreamTerminal,
		RangeStreamError:    authError(rangeStreamErr),
		OldToken:            authError(oldTokenErr),
		ReauthPut:           reauthPutErr == nil,
	}
}
