package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestValidateStatusResponseProjectsExactEnvelope(t *testing.T) {
	result, err := validateStatusResponse(&etcdserverpb.StatusResponse{
		Header:           &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8},
		Version:          "3.7.0",
		DbSize:           99,
		Leader:           456,
		RaftIndex:        7,
		RaftTerm:         8,
		RaftAppliedIndex: 7,
		Errors:           []string{"example"},
		DbSizeInUse:      88,
		IsLearner:        true,
		StorageVersion:   "3.7.0",
		DbSizeQuota:      2147483648,
		DowngradeInfo:    &etcdserverpb.DowngradeInfo{Enabled: true, TargetVersion: "3.6.0"},
	})
	require.NoError(t, err)
	require.Equal(t, statusProbeResult{
		Header:           statusProbeHeader{ClusterID: 123, MemberID: 456, Revision: 7, RaftTerm: 8},
		Version:          "3.7.0",
		DBSize:           99,
		Leader:           456,
		RaftIndex:        7,
		RaftTerm:         8,
		RaftAppliedIndex: 7,
		Errors:           []string{"example"},
		DBSizeInUse:      88,
		IsLearner:        true,
		StorageVersion:   "3.7.0",
		DBSizeQuota:      2147483648,
		DowngradeInfo:    statusProbeDowngradeInfo{Enabled: true, TargetVersion: "3.6.0"},
	}, result)
}

func TestValidateStatusResponseProjectsDefaultDowngradeInfo(t *testing.T) {
	result, err := validateStatusResponse(&etcdserverpb.StatusResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8},
		Version: "3.5.0",
	})
	require.NoError(t, err)
	require.Equal(t, statusProbeDowngradeInfo{}, result.DowngradeInfo)
	require.Empty(t, result.Errors)
	require.NotNil(t, result.Errors)
}

func TestValidateStatusResponseRejectsMissingVersionedDowngradeInfo(t *testing.T) {
	for _, version := range []string{"3.6.0", "3.6.0-rc.1", "4.0.0", "9223372036854775808.0.0"} {
		t.Run(version, func(t *testing.T) {
			_, err := validateStatusResponse(&etcdserverpb.StatusResponse{
				Header:  &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8},
				Version: version,
			})
			require.ErrorContains(t, err, "downgrade information is missing")
		})
	}
}

func TestValidateStatusResponseRejectsInvalidDowngradeState(t *testing.T) {
	for _, tc := range []struct {
		name string
		info *etcdserverpb.DowngradeInfo
	}{
		{name: "disabled with target", info: &etcdserverpb.DowngradeInfo{TargetVersion: "3.6.0"}},
		{name: "enabled without target", info: &etcdserverpb.DowngradeInfo{Enabled: true}},
		{name: "enabled with invalid target", info: &etcdserverpb.DowngradeInfo{Enabled: true, TargetVersion: "not-semver"}},
		{name: "enabled with prerelease target", info: &etcdserverpb.DowngradeInfo{Enabled: true, TargetVersion: "3.6.0-rc.1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateStatusResponse(&etcdserverpb.StatusResponse{
				Header:        &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8},
				Version:       "3.7.0",
				DowngradeInfo: tc.info,
			})
			require.ErrorContains(t, err, "downgrade information is inconsistent")
		})
	}
}

func TestValidateStatusResponseRejectsInvalidVersion(t *testing.T) {
	_, err := validateStatusResponse(&etcdserverpb.StatusResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8},
		Version: "not-semver",
	})
	require.ErrorContains(t, err, "version must be a semver string")
}

func TestValidateStatusResponseRejectsMissingEnvelope(t *testing.T) {
	_, err := validateStatusResponse(nil)
	require.ErrorContains(t, err, "nil response")
	_, err = validateStatusResponse(&etcdserverpb.StatusResponse{})
	require.ErrorContains(t, err, "header is missing")
}

func TestValidateStatusResponseRejectsInvalidEnvelope(t *testing.T) {
	valid := &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8}
	for _, tc := range []struct {
		name     string
		response *etcdserverpb.StatusResponse
		want     string
	}{
		{name: "cluster", response: &etcdserverpb.StatusResponse{Header: &etcdserverpb.ResponseHeader{MemberId: valid.MemberId, Revision: valid.Revision, RaftTerm: valid.RaftTerm}}, want: "cluster ID must be positive"},
		{name: "member", response: &etcdserverpb.StatusResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: valid.ClusterId, Revision: valid.Revision, RaftTerm: valid.RaftTerm}}, want: "member ID must be positive"},
		{name: "revision", response: &etcdserverpb.StatusResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: valid.ClusterId, MemberId: valid.MemberId, Revision: -1, RaftTerm: valid.RaftTerm}}, want: "revision must be non-negative"},
		{name: "term", response: &etcdserverpb.StatusResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: valid.ClusterId, MemberId: valid.MemberId, Revision: valid.Revision}}, want: "header raft term must be positive"},
		{name: "db size", response: &etcdserverpb.StatusResponse{Header: valid, DbSize: -1}, want: "db size must be non-negative"},
		{name: "db size in use", response: &etcdserverpb.StatusResponse{Header: valid, DbSizeInUse: -1}, want: "db size in use must be non-negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateStatusResponse(tc.response)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
