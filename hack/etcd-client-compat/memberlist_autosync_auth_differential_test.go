package compat

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type authenticatedAutoSyncOutcome struct {
	EndpointsReplaced bool
	EndpointsMatch    bool
	RangeAfterSyncOK  bool
}

func TestAuthenticatedAutoSyncDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcome := runAuthenticatedAutoSyncScenario(t, reference)
	kubeBrainOutcome := runAuthenticatedAutoSyncScenario(t, compatEndpoint())
	require.Equal(t, referenceOutcome, kubeBrainOutcome)
	require.True(t, kubeBrainOutcome.EndpointsReplaced)
	require.True(t, kubeBrainOutcome.EndpointsMatch)
	require.True(t, kubeBrainOutcome.RangeAfterSyncOK)
}

func runAuthenticatedAutoSyncScenario(t *testing.T, endpoint string) authenticatedAutoSyncOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bootstrap, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, bootstrap.Close()) }()

	_, err = bootstrap.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "autosync-reader")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(
		ctx, "autosync-reader", "/a368/autosync-auth/", clientv3.GetPrefixRangeEnd("/a368/autosync-auth/"),
		clientv3.PermissionType(clientv3.PermRead),
	)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "alice", "autosync-reader")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		root, rootErr := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, Username: "root", Password: "root-secret", DialTimeout: 3 * time.Second,
		})
		if rootErr != nil {
			return
		}
		defer root.Close()
		_, _ = root.AuthDisable(cleanupCtx)
		_, _ = bootstrap.UserDelete(cleanupCtx, "alice")
		_, _ = bootstrap.UserDelete(cleanupCtx, "root")
		_, _ = bootstrap.RoleDelete(cleanupCtx, "autosync-reader")
	}()

	// A scheme-less seed is semantically equivalent but textually distinct from
	// the advertised http(s) ClientURLs, making the background replacement
	// directly observable without calling Sync ourselves.
	seed := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	alice, err := clientv3.New(clientv3.Config{
		Endpoints: []string{seed}, Username: "alice", Password: "alice-secret",
		DialTimeout: 3 * time.Second, AutoSyncInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, alice.Close()) }()

	members, err := alice.MemberList(ctx)
	require.NoError(t, err)
	expected := make([]string, 0, len(members.Members))
	for _, member := range members.Members {
		if member.Name != "" && !member.IsLearner {
			expected = append(expected, member.ClientURLs...)
		}
	}
	require.NotEmpty(t, expected)
	sort.Strings(expected)

	replaced := false
	matched := false
	require.Eventually(t, func() bool {
		actual := alice.Endpoints()
		replaced = replaced || len(actual) != 1 || actual[0] != seed
		sort.Strings(actual)
		matched = strings.Join(actual, "\x00") == strings.Join(expected, "\x00")
		return matched
	}, 5*time.Second, 20*time.Millisecond)

	rangeOK := false
	if matched {
		_, rangeErr := alice.Get(ctx, "/a368/autosync-auth/missing")
		rangeOK = rangeErr == nil
	}
	return authenticatedAutoSyncOutcome{
		EndpointsReplaced: replaced,
		EndpointsMatch:    matched, RangeAfterSyncOK: rangeOK,
	}
}
