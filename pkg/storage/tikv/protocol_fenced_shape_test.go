package tikv

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type fencedShapeClient struct {
	*protocolLatencyClient
	leadership, restoration atomic.Int32
}

func (c *fencedShapeClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if ctx.Value(protocolLatencyMarker{}) == true && req.Type == tikvrpc.CmdPrewrite {
		for _, mutation := range req.Prewrite().Mutations {
			if strings.HasPrefix(string(mutation.Key), "/kubebrain-internal/ks-fenced-shape/election-fence/") {
				c.leadership.Add(1)
			}
			if strings.HasPrefix(string(mutation.Key), "/kubebrain-internal/ks-fenced-shape/restoration-fence-shard/") {
				c.restoration.Add(1)
			}
		}
	}
	return c.protocolLatencyClient.SendRequest(ctx, addr, req, timeout)
}

// This in-process mock models the production separation of coordination,
// internal/event metadata and user objects. It is not a Raft durability or
// latency benchmark. Do not add the 512 fence shards to the bounded real
// protocol fixture without separately designing its ownership/cleanup scope.
func TestProtocolProductionFencedShape(t *testing.T) {
	for _, onePC := range []bool{false, true} {
		t.Run(fmt.Sprintf("enable1pc=%t", onePC), func(t *testing.T) {
			t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) {
				c.Enable1PC = onePC
				c.EnableAsyncCommit = false
			}))
			for _, fenced := range []bool{false, true} {
				t.Run(fmt.Sprintf("production_fences=%t", fenced), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					ks, err := coder.NewKeyspace("fenced-shape")
					require.NoError(t, err)
					key := []byte("/probe/watch")
					client, cluster, pd, err := testutils.NewMockTiKV("", nil)
					require.NoError(t, err)
					_, _, first := testutils.BootstrapWithSingleStore(cluster)
					middle, peer := cluster.AllocID(), cluster.AllocID()
					cluster.Split(first, middle, ks.EventLogRangeStart(0), []uint64{peer}, peer)
					last, peer := cluster.AllocID(), cluster.AllocID()
					cluster.Split(middle, last, ks.NewCoder().EncodeRevisionKey(key), []uint64{peer}, peer)
					rpc := &fencedShapeClient{protocolLatencyClient: &protocolLatencyClient{Client: client}}
					store, err := clienttikv.NewKVStore("fenced-shape", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), rpc)
					require.NoError(t, err)
					kv := NewKvStoreWithStorage([]*clienttikv.KVStore{store})
					b := backend.NewBackend(kv, backend.Config{
						Prefix: "/kubebrain-internal/ks-fenced-shape", Keyspace: ks.Name(), Identity: "leader",
						EnableEtcdCompatibility: true, QuotaBackendBytes: 2 << 30,
					}, metricmock.NewMinimalMetrics(gomock.NewController(t)))
					t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
					b.SetCurrentRevision(100)
					require.NoError(t, b.EnsureQuotaInitialized(ctx))
					if fenced {
						require.NoError(t, b.GetResourceLock().Create(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: "leader", LeaseDurationSeconds: 30}))
						_, _, ok := b.GetResourceLock().(election.StorageFenceTokenProvider).StorageFenceToken(0)
						require.True(t, ok)
						_, _, ok = b.GetResourceLock().(election.RestorationFenceTokenProvider).RestorationFenceToken(0)
						require.True(t, ok)
						b.SetLeadershipFence(func() (uint64, bool) { return 1, true })
						ctx = backend.WithLeadershipEpoch(ctx, 1)
					}
					var observations []storage.BatchCommitObservation
					measured := storage.WithBatchCommitObserver(context.WithValue(ctx, protocolLatencyMarker{}, true), func(o storage.BatchCommitObservation) {
						observations = append(observations, o)
					})
					for i := 0; i < 3; i++ {
						value := []byte(fmt.Sprintf("value-%d", i))
						_, revision, err := b.TxnApply(measured, []backend.TxnWriteOp{{Key: key, Value: value}}, nil)
						require.NoError(t, err)
						require.EqualValues(t, 101+i, revision)
						got, err := b.Get(ctx, &proto.GetRequest{Key: key})
						require.NoError(t, err)
						require.NotNil(t, got.Kv)
						require.Equal(t, revision, got.Kv.Revision)
						require.Equal(t, value, backend.StripInlineValue(got.Kv.Value))
					}
					groups := int32(2)
					if fenced {
						groups = 3
					}
					require.Len(t, observations, 3)
					for _, o := range observations {
						require.NoError(t, o.Err)
						require.True(t, o.HasWriteDetails)
						require.Equal(t, groups, o.PrewriteRegionGroups)
					}
					require.Equal(t, int(3*groups), rpc.requestSnapshot()["prewrite"])
					require.Zero(t, rpc.snapshot().OnePC, "both layouts are multi-Region even when 1PC is enabled")
					fenceMutations := int32(0)
					if fenced {
						fenceMutations = 3
					}
					require.Equal(t, fenceMutations, rpc.leadership.Load(), "every measured batch must carry its leadership CAS mutation")
					require.Equal(t, fenceMutations, rpc.restoration.Load(), "every measured batch must carry its restoration CAS mutation")
				})
			}
		})
	}
}
