package etcd

import (
	"context"
	"encoding/binary"
	"testing"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type putCurrentValidationBackend struct {
	backend.Backend
	current *proto.KeyValue
	writes  int
	update  *proto.UpdateRequest
}

func (b *putCurrentValidationBackend) Get(context.Context, *proto.GetRequest) (*proto.GetResponse, error) {
	return &proto.GetResponse{Kv: b.current}, nil
}

func (b *putCurrentValidationBackend) Update(_ context.Context, r *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	b.writes++
	b.update = r
	return &proto.UpdateResponse{Header: &proto.ResponseHeader{Revision: 6}, Succeeded: true}, nil
}

func (b *putCurrentValidationBackend) Create(context.Context, *proto.CreateRequest) (*proto.CreateResponse, error) {
	b.writes++
	return &proto.CreateResponse{Header: &proto.ResponseHeader{Revision: 6}, Succeeded: true}, nil
}

func (b *putCurrentValidationBackend) TxnApply(context.Context, []backend.TxnWriteOp, []backend.TxnGuard) ([]backend.TxnWriteResult, uint64, error) {
	b.writes++
	return nil, 6, nil
}

// Removing the shim's pre-read must not allow a plain overwrite (without
// PrevKv) to silently replace a corrupt current object. Any future fast path
// needs equivalent validation before its first durable mutation.
func TestPutValidatesCurrentInlineLifecycleWithoutPrevKV(t *testing.T) {
	inline := func(create, version uint64) []byte {
		value := make([]byte, 20)
		copy(value, []byte{0, 'k', 'b', 3})
		binary.BigEndian.PutUint64(value[4:], create)
		binary.BigEndian.PutUint64(value[12:], version)
		return append(value, []byte("old")...)
	}
	for _, tc := range []struct {
		name  string
		value []byte
		valid bool
	}{
		{"valid", inline(3, 2), true},
		{"truncated envelope", []byte{0, 'k', 'b', 3}, false},
		{"future create revision", inline(6, 1), false},
		{"first version at wrong revision", inline(3, 1), false},
		{"impossible version count", inline(3, 4), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &putCurrentValidationBackend{current: &proto.KeyValue{
				Key: []byte("/put/current"), Value: tc.value, Revision: 5,
			}}
			shim := &backendShim{backend: probe}
			shim.watchTranslator = newWatchTranslator(shim)
			for i := range shim.mutationLocks {
				shim.mutationLocks[i] = make(chan struct{}, 1)
			}
			response, err := shim.Put(t.Context(), &etcdserverpb.PutRequest{
				Key: probe.current.Key, Value: []byte("new"),
			})
			if !tc.valid {
				require.ErrorIs(t, err, backend.ErrInvalidMVCCMetadata)
				require.Nil(t, response)
				require.Zero(t, probe.writes, "corruption must be rejected before mutation")
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, probe.writes)
			require.Equal(t, uint64(5), probe.update.Kv.Revision)
			require.Equal(t, []byte("new"), probe.update.Kv.Value)
			require.Equal(t, int64(6), response.Header.Revision)
			require.Nil(t, response.PrevKv)
		})
	}
}
