package compat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClientV3WatchResponseUsesPointerHeader pins upstream etcd 36d0f9ac.
// WatchResponse.Header is a pointer so empty/close/progress responses can share
// the same nil-safe header semantics as protobuf watch responses.
func TestClientV3WatchResponseUsesPointerHeader(t *testing.T) {
	var _ *etcdserverpb.ResponseHeader = clientv3.WatchResponse{}.Header

	response := clientv3.WatchResponse{}
	require.False(t, response.IsProgressNotify())

	response.Header = &etcdserverpb.ResponseHeader{Revision: 7}
	require.True(t, response.IsProgressNotify())
}
