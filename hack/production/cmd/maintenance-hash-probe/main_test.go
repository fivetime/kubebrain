package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestValidateHashResponseProjectsExactEnvelope(t *testing.T) {
	result, err := validateHashResponse(&etcdserverpb.HashResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: 123,
			MemberId:  456,
			Revision:  7,
			RaftTerm:  8,
		},
		Hash: 4294967295,
	})
	require.NoError(t, err)
	require.Equal(t, hashProbeResult{
		Header: hashProbeHeader{ClusterID: 123, MemberID: 456, Revision: 7, RaftTerm: 8},
		Hash:   4294967295,
	}, result)
}

func TestValidateHashResponseRejectsMissingEnvelope(t *testing.T) {
	_, err := validateHashResponse(nil)
	require.ErrorContains(t, err, "nil response")
	_, err = validateHashResponse(&etcdserverpb.HashResponse{})
	require.ErrorContains(t, err, "header is missing")
}

func TestValidateHashResponseRejectsInvalidHeader(t *testing.T) {
	valid := &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8}
	for _, tc := range []struct {
		name   string
		header *etcdserverpb.ResponseHeader
		want   string
	}{
		{name: "cluster", header: &etcdserverpb.ResponseHeader{MemberId: valid.MemberId, Revision: valid.Revision, RaftTerm: valid.RaftTerm}, want: "cluster ID must be positive"},
		{name: "member", header: &etcdserverpb.ResponseHeader{ClusterId: valid.ClusterId, Revision: valid.Revision, RaftTerm: valid.RaftTerm}, want: "member ID must be positive"},
		{name: "revision", header: &etcdserverpb.ResponseHeader{ClusterId: valid.ClusterId, MemberId: valid.MemberId, Revision: -1, RaftTerm: valid.RaftTerm}, want: "revision must be non-negative"},
		{name: "term", header: &etcdserverpb.ResponseHeader{ClusterId: valid.ClusterId, MemberId: valid.MemberId, Revision: valid.Revision}, want: "raft term must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateHashResponse(&etcdserverpb.HashResponse{Header: tc.header})
			require.ErrorContains(t, err, tc.want)
		})
	}
}
