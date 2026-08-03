package compat

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type memberMutationStateOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestMemberMutationStateDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcomes := memberMutationStateOutcomes(t, reference)
	require.Equal(t, []memberMutationStateOutcome{
		{Name: "remove-missing", Code: "NotFound", Message: "etcdserver: member not found"},
		{Name: "update-missing", Code: "NotFound", Message: "etcdserver: member not found"},
		{Name: "promote-missing", Code: "NotFound", Message: "etcdserver: member not found"},
		{Name: "promote-voter", Code: "FailedPrecondition", Message: "etcdserver: can only promote a learner member"},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, memberMutationStateOutcomes(t, compatEndpoint()))
}

func memberMutationStateOutcomes(t *testing.T, endpoint string) []memberMutationStateOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewClusterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	members, err := client.MemberList(ctx, &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, members.Members)
	var voterID uint64
	for _, member := range members.Members {
		if !member.IsLearner {
			voterID = member.ID
			break
		}
	}
	require.NotZero(t, voterID)

	missingID := uint64(math.MaxUint64)
	tests := []struct {
		name string
		call func() error
	}{
		{name: "remove-missing", call: func() error {
			_, err := client.MemberRemove(ctx, &etcdserverpb.MemberRemoveRequest{ID: missingID})
			return err
		}},
		{name: "update-missing", call: func() error {
			_, err := client.MemberUpdate(ctx, &etcdserverpb.MemberUpdateRequest{
				ID: missingID, PeerURLs: []string{"http://127.0.0.1:32380"},
			})
			return err
		}},
		{name: "promote-missing", call: func() error {
			_, err := client.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{ID: missingID})
			return err
		}},
		{name: "promote-voter", call: func() error {
			_, err := client.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{ID: voterID})
			return err
		}},
	}
	outcomes := make([]memberMutationStateOutcome, 0, len(tests))
	for _, test := range tests {
		callErr := test.call()
		outcomes = append(outcomes, memberMutationStateOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
