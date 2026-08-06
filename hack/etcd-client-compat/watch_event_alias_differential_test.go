package compat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Compile-time contract for upstream etcd 2214d9f13: clientv3.Event is an alias
// of mvccpb.Event, so protobuf watch events can be used directly through the
// clientv3 API surface without pointer conversion wrappers.
var (
	_ *clientv3.Event = (*mvccpb.Event)(nil)
	_ *mvccpb.Event   = (*clientv3.Event)(nil)
)

func TestClientV3EventAliasesMVCCPBEvent(t *testing.T) {
	create := &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{CreateRevision: 11, ModRevision: 11}}
	modify := &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{CreateRevision: 11, ModRevision: 12}}
	deleted := &mvccpb.Event{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{CreateRevision: 11, ModRevision: 13}}

	require.True(t, create.IsCreate())
	require.False(t, create.IsModify())
	require.False(t, modify.IsCreate())
	require.True(t, modify.IsModify())
	require.False(t, deleted.IsCreate())
	require.False(t, deleted.IsModify())
}
