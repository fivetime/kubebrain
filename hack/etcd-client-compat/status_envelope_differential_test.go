package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type statusEnvelopeOutcome struct {
	HeaderPresent          bool
	ClusterIDPositive      bool
	MemberIDPositive       bool
	HeaderRevisionPositive bool
	HeaderTermPositive     bool
	VersionPresent         bool
	StorageVersionPresent  bool
	LeaderPositive         bool
	RaftIndexPositive      bool
	RaftAppliedPositive    bool
	AppliedNotAhead        bool
	RaftTermPositive       bool
	HeaderTermMatches      bool
	DBSizePositive         bool
	DBSizeInUsePositive    bool
	DBSizeInUseNotLarger   bool
	DBSizeQuotaPositive    bool
	IsLearner              bool
	DowngradeInfoPresent   bool
	DowngradeEnabled       bool
	DowngradeTarget        string
	Errors                 []string
}

func TestMaintenanceStatusEnvelopeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := statusEnvelopeOutcome{
		HeaderPresent:          true,
		ClusterIDPositive:      true,
		MemberIDPositive:       true,
		HeaderRevisionPositive: true,
		HeaderTermPositive:     true,
		VersionPresent:         true,
		StorageVersionPresent:  true,
		LeaderPositive:         true,
		RaftIndexPositive:      true,
		RaftAppliedPositive:    true,
		AppliedNotAhead:        true,
		RaftTermPositive:       true,
		HeaderTermMatches:      true,
		DBSizePositive:         true,
		DBSizeInUsePositive:    true,
		DBSizeInUseNotLarger:   true,
		DBSizeQuotaPositive:    true,
		DowngradeInfoPresent:   true,
		Errors:                 []string{},
	}
	referenceOutcome := readStatusEnvelope(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, readStatusEnvelope(t, compatEndpoint()))
}

func readStatusEnvelope(t *testing.T, endpoint string) statusEnvelopeOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	header := response.GetHeader()
	downgrade := response.GetDowngradeInfo()
	return statusEnvelopeOutcome{
		HeaderPresent:          header != nil,
		ClusterIDPositive:      header.GetClusterId() > 0,
		MemberIDPositive:       header.GetMemberId() > 0,
		HeaderRevisionPositive: header.GetRevision() > 0,
		HeaderTermPositive:     header.GetRaftTerm() > 0,
		VersionPresent:         response.GetVersion() != "",
		StorageVersionPresent:  response.GetStorageVersion() != "",
		LeaderPositive:         response.GetLeader() > 0,
		RaftIndexPositive:      response.GetRaftIndex() > 0,
		RaftAppliedPositive:    response.GetRaftAppliedIndex() > 0,
		AppliedNotAhead:        response.GetRaftAppliedIndex() <= response.GetRaftIndex(),
		RaftTermPositive:       response.GetRaftTerm() > 0,
		HeaderTermMatches:      header.GetRaftTerm() == response.GetRaftTerm(),
		DBSizePositive:         response.GetDbSize() > 0,
		DBSizeInUsePositive:    response.GetDbSizeInUse() > 0,
		DBSizeInUseNotLarger:   response.GetDbSizeInUse() <= response.GetDbSize(),
		DBSizeQuotaPositive:    response.GetDbSizeQuota() > 0,
		IsLearner:              response.GetIsLearner(),
		DowngradeInfoPresent:   downgrade != nil,
		DowngradeEnabled:       downgrade.GetEnabled(),
		DowngradeTarget:        downgrade.GetTargetVersion(),
		Errors:                 append([]string{}, response.GetErrors()...),
	}
}
