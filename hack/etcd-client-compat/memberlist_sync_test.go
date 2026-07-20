package compat

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type memberListFlagOutcome struct {
	Linearizable       bool
	HeaderRevisionZero bool
	ClusterIDNonZero   bool
	MemberIDNonZero    bool
	RaftTermPositive   bool
	LocalMemberListed  bool
	LeaderListed       bool
	MembersWellFormed  bool
}

func TestMemberListFlagsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes, referenceCount := memberListFlagOutcomes(t, reference)
	kubebrainOutcomes, kubebrainCount := memberListFlagOutcomes(t, compatEndpoint())
	expectedKubeBrainCount, err := expectedKubeBrainMemberCount()
	require.NoError(t, err)
	require.Equal(t, referenceOutcomes, kubebrainOutcomes)
	require.Equal(t, []memberListFlagOutcome{
		{
			HeaderRevisionZero: true, ClusterIDNonZero: true, MemberIDNonZero: true,
			RaftTermPositive: true, LocalMemberListed: true, LeaderListed: true, MembersWellFormed: true,
		},
		{
			Linearizable: true, HeaderRevisionZero: true, ClusterIDNonZero: true, MemberIDNonZero: true,
			RaftTermPositive: true, LocalMemberListed: true, LeaderListed: true, MembersWellFormed: true,
		},
	}, kubebrainOutcomes)
	require.Equal(t, 1, referenceCount)
	require.Equal(t, expectedKubeBrainCount, kubebrainCount)
}

func expectedKubeBrainMemberCount() (int, error) {
	raw, ok := os.LookupEnv("KUBEBRAIN_EXPECTED_MEMBER_COUNT")
	return parseExpectedKubeBrainMemberCount(raw, ok)
}

func parseExpectedKubeBrainMemberCount(raw string, configured bool) (int, error) {
	const defaultMemberCount = 3

	if !configured {
		return defaultMemberCount, nil
	}
	count, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || count <= 0 {
		return 0, fmt.Errorf("KUBEBRAIN_EXPECTED_MEMBER_COUNT must be a positive integer, got %q", raw)
	}
	return count, nil
}

func TestExpectedKubeBrainMemberCount(t *testing.T) {
	t.Run("production default", func(t *testing.T) {
		count, err := parseExpectedKubeBrainMemberCount("", false)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})
	t.Run("disposable topology", func(t *testing.T) {
		count, err := parseExpectedKubeBrainMemberCount("1", true)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
	for _, value := range []string{"", "0", "-1", "three"} {
		t.Run("reject "+value, func(t *testing.T) {
			_, err := parseExpectedKubeBrainMemberCount(value, true)
			require.Error(t, err)
		})
	}
}

func memberListFlagOutcomes(t *testing.T, endpoint string) ([]memberListFlagOutcome, int) {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := etcdserverpb.NewClusterClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	statusResp, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	outcomes := make([]memberListFlagOutcome, 0, 2)
	memberCount := 0
	for _, linearizable := range []bool{false, true} {
		resp, listErr := cluster.MemberList(ctx, &etcdserverpb.MemberListRequest{Linearizable: linearizable})
		require.NoError(t, listErr)
		if memberCount == 0 {
			memberCount = len(resp.Members)
		} else {
			require.Equal(t, memberCount, len(resp.Members))
		}
		localListed, leaderListed, wellFormed := false, false, len(resp.Members) > 0
		for _, member := range resp.Members {
			localListed = localListed || member.ID == resp.Header.MemberId
			leaderListed = leaderListed || member.ID == statusResp.Leader
			wellFormed = wellFormed && member.ID != 0 && member.Name != "" &&
				len(member.PeerURLs) > 0 && len(member.ClientURLs) > 0 && !member.IsLearner
		}
		outcomes = append(outcomes, memberListFlagOutcome{
			Linearizable: linearizable, HeaderRevisionZero: resp.Header.Revision == 0,
			ClusterIDNonZero: resp.Header.ClusterId != 0, MemberIDNonZero: resp.Header.MemberId != 0,
			RaftTermPositive: resp.Header.RaftTerm > 0, LocalMemberListed: localListed,
			LeaderListed: leaderListed, MembersWellFormed: wellFormed,
		})
	}
	return outcomes, memberCount
}

func TestMemberListHeaderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceRevision := memberListHeaderRevision(t, reference)
	kubebrainRevision := memberListHeaderRevision(t, compatEndpoint())
	require.Equal(t, referenceRevision, kubebrainRevision)
	require.Zero(t, kubebrainRevision)
}

func memberListHeaderRevision(t *testing.T, endpoint string) int64 {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := "/dbaas-memberlist/header-revision"
	put, err := cli.Put(ctx, key, "1")
	require.NoError(t, err)
	require.Positive(t, put.Header.Revision)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})
	resp, err := cli.MemberList(ctx)
	require.NoError(t, err)
	return resp.Header.Revision
}

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

	// Production MemberList advertises stable cluster DNS names. A test launched
	// on the host can verify the Sync result but cannot resolve *.svc; restore the
	// ingress endpoint for its data-plane smoke. The explicit opt-in is exercised
	// by the in-cluster probe, which must prove the synchronized endpoints are
	// independently dialable.
	if os.Getenv("KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIALABLE") != "1" {
		cli.SetEndpoints(compatEndpoint())
	}

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
