package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestValidateHashKVResponseProjectsExactEnvelope(t *testing.T) {
	result, err := validateHashKVResponse(&etcdserverpb.HashKVResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: 123,
			MemberId:  456,
			Revision:  7,
			RaftTerm:  8,
		},
		Hash:            4294967295,
		CompactRevision: 3,
		HashRevision:    7,
	})
	require.NoError(t, err)
	require.Equal(t, hashKVProbeResult{
		Header:          hashKVProbeHeader{ClusterID: 123, MemberID: 456, Revision: 7, RaftTerm: 8},
		Hash:            4294967295,
		CompactRevision: 3,
		HashRevision:    7,
	}, result)
}

func TestValidateHashKVResponseRejectsMissingEnvelope(t *testing.T) {
	_, err := validateHashKVResponse(nil)
	require.ErrorContains(t, err, "nil response")
	_, err = validateHashKVResponse(&etcdserverpb.HashKVResponse{})
	require.ErrorContains(t, err, "header is missing")
}

func TestValidateHashKVResponseRejectsInvalidHeader(t *testing.T) {
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
			_, err := validateHashKVResponse(&etcdserverpb.HashKVResponse{Header: tc.header})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestValidateHashKVResponseRejectsInvalidRevisions(t *testing.T) {
	header := &etcdserverpb.ResponseHeader{ClusterId: 123, MemberId: 456, Revision: 7, RaftTerm: 8}
	_, err := validateHashKVResponse(&etcdserverpb.HashKVResponse{Header: header, HashRevision: -1})
	require.ErrorContains(t, err, "hash revision must be non-negative")
	_, err = validateHashKVResponse(&etcdserverpb.HashKVResponse{Header: header, CompactRevision: -2})
	require.ErrorContains(t, err, "compact revision must be at least -1")
}
