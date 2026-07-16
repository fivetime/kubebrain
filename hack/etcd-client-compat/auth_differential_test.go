package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authErrorOutcome struct {
	Code    codes.Code
	Message string
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
	return authErrorOutcome{Code: status.Code(err), Message: status.Convert(err).Message()}
}

func collectAuthDifferentialOutcome(t *testing.T, endpoint string) authDifferentialOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")

	_, err := bootstrap.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = bootstrap.UserAddWithOptions(ctx, "nopass", "", &clientv3.UserAddOptions{NoPassword: true})
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, implicitRootRoleErr := bootstrap.RoleGet(ctx, "root")
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)
	statusAfterEnable, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)

	_, wrongCredentialsErr := bootstrap.Authenticate(ctx, "missing", "wrong")
	_, noPasswordErr := bootstrap.Authenticate(ctx, "nopass", "password")
	root := authClient(t, endpoint, "root", "root-secret")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = root.AuthDisable(cleanupCtx)
	})

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
	actual := collectAuthDifferentialOutcome(t, kubebrainEndpoint)
	require.Equal(t, reference, actual)
}
