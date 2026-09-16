package tikv

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestProtocolFencedLatencyFixtureBound(t *testing.T) {
	ctx := context.Background()
	prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
	ks, _, err := protocolBackendScope(prefix)
	require.NoError(t, err)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: prefix + "backend", Keyspace: ks.Name(),
		Identity: ks.Name(), EnableEtcdCompatibility: true, QuotaBackendBytes: 2 << 30},
		metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	b.SetCurrentRevision(100)
	ctx = installRealProtocolFences(t, ctx, b)
	for i := 0; i < 1+10+20; i++ {
		_, _, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: []byte("/integration/latency/key"), Value: bytes.Repeat([]byte("v"), 256)}}, nil)
		require.NoError(t, err)
	}
	keys, err := protocolBackendKeysWithFences(ctx, kv, prefix, true)
	require.NoError(t, err)
	require.LessOrEqual(t, len(keys), 512+100, "retain ordinary-key cleanup headroom without enlarging the existing shard allowance")
	_, err = protocolBackendKeys(ctx, kv, prefix)
	require.Error(t, err, "unfenced cleanup must not silently accept coordination shards")
}
