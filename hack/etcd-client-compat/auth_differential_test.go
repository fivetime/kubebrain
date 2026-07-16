package compat

import (
	"context"
	"os"
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
	EnabledRevision        uint64
	DuplicateGrantRevision uint64
	DuplicateRoleRevision  uint64
	ImplicitRootRole       authErrorOutcome
	WrongCredentials       authErrorOutcome
	NoPassword             authErrorOutcome
	DuplicateRole          authErrorOutcome
	MissingRevoke          authErrorOutcome
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

	return authDifferentialOutcome{
		EnabledRevision:        statusAfterEnable.AuthRevision,
		DuplicateGrantRevision: statusAfterDuplicateGrant.AuthRevision,
		DuplicateRoleRevision:  statusAfterDuplicateRole.AuthRevision,
		ImplicitRootRole:       authError(implicitRootRoleErr),
		WrongCredentials:       authError(wrongCredentialsErr),
		NoPassword:             authError(noPasswordErr),
		DuplicateRole:          authError(duplicateRoleErr),
		MissingRevoke:          authError(missingRevokeErr),
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
