package compat

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMemberListSupportsOfficialClientSync(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	members, err := cli.MemberList(ctx)
	require.NoError(t, err)
	expected := make([]string, 0, len(members.Members))
	for _, member := range members.Members {
		if member.Name != "" && !member.IsLearner {
			expected = append(expected, member.ClientURLs...)
		}
	}
	require.NotEmpty(t, expected)

	require.NoError(t, cli.Sync(ctx))
	actual := cli.Endpoints()
	sort.Strings(expected)
	sort.Strings(actual)
	require.Equal(t, expected, actual)

	// The synchronized set may include a configured member that is currently
	// down, as etcd membership does. gRPC must still select a ready replica.
	key := testPrefix(t) + "/sync"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})
	_, err = cli.Put(ctx, key, "ok")
	require.NoError(t, err)
	got, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, "ok", string(got.Kvs[0].Value))
}
